import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import pandas as pd


class Database:
    def __init__(self, db_path: str = "house_hunter.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.initialize()

    def initialize(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS listings (
                property_id TEXT PRIMARY KEY,
                address TEXT,
                city TEXT,
                state TEXT,
                zip_code TEXT,
                price REAL,
                beds REAL,
                baths REAL,
                sqft REAL,
                lot_sqft REAL,
                year_built REAL,
                latitude REAL,
                longitude REAL,
                description TEXT,
                property_type TEXT,
                status TEXT,
                list_date TEXT,
                photo_url TEXT,
                mls_id TEXT,
                hoa_fee REAL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                raw_data TEXT
            );

            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id TEXT NOT NULL,
                price REAL NOT NULL,
                recorded_at TEXT NOT NULL,
                UNIQUE(property_id, price),
                FOREIGN KEY (property_id) REFERENCES listings(property_id)
            );

            CREATE TABLE IF NOT EXISTS preferences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS listing_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id TEXT NOT NULL,
                action TEXT NOT NULL CHECK(action IN ('favorite', 'reject', 'note')),
                note TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (property_id) REFERENCES listings(property_id)
            );

            CREATE TABLE IF NOT EXISTS scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id TEXT NOT NULL,
                score REAL NOT NULL,
                reasoning TEXT,
                preferences_hash TEXT NOT NULL,
                scored_at TEXT NOT NULL,
                UNIQUE(property_id, preferences_hash),
                FOREIGN KEY (property_id) REFERENCES listings(property_id)
            );

            CREATE TABLE IF NOT EXISTS saved_searches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                config_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_used_at TEXT
            );

            CREATE TABLE IF NOT EXISTS distance_locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                priority INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS geocode_cache (
                place_name TEXT PRIMARY KEY,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                cached_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS school_districts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                state TEXT,
                rating INTEGER CHECK(rating IS NULL OR (rating >= 1 AND rating <= 10)),
                excluded INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                UNIQUE(name, state)
            );

            CREATE TABLE IF NOT EXISTS zip_district_map (
                zip_code TEXT PRIMARY KEY,
                district_id INTEGER NOT NULL,
                FOREIGN KEY (district_id) REFERENCES school_districts(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS score_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                preferences_hash TEXT NOT NULL,
                preferences_json TEXT NOT NULL,
                locations_json TEXT NOT NULL,
                districts_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
        """)
        self.conn.commit()
        self._migrate()

    def _migrate(self):
        """Add columns that may not exist in older databases."""
        listing_cols = {r[1] for r in self.conn.execute("PRAGMA table_info(listings)").fetchall()}
        if "hoa_fee" not in listing_cols:
            self.conn.execute("ALTER TABLE listings ADD COLUMN hoa_fee REAL")
        if "district_id" not in listing_cols:
            self.conn.execute("ALTER TABLE listings ADD COLUMN district_id INTEGER")

        district_cols = {r[1] for r in self.conn.execute("PRAGMA table_info(school_districts)").fetchall()}
        if "nces_id" not in district_cols:
            self.conn.execute("ALTER TABLE school_districts ADD COLUMN nces_id TEXT")
            self.conn.execute("ALTER TABLE school_districts ADD COLUMN school_count INTEGER")
            self.conn.execute("ALTER TABLE school_districts ADD COLUMN enrollment INTEGER")
            self.conn.execute("ALTER TABLE school_districts ADD COLUMN student_teacher_ratio REAL")

        self.conn.commit()

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def upsert_listings(self, df: pd.DataFrame) -> tuple[list[str], list[str]]:
        """Insert or update listings from a DataFrame. Returns (new_ids, price_changed_ids)."""
        new_ids = []
        price_changed_ids = set()
        now = self._now()

        for _, row in df.iterrows():
            raw = row.to_dict()
            # Convert NaN/NaT to None for JSON serialization
            for k, v in raw.items():
                try:
                    if pd.isna(v):
                        raw[k] = None
                except (ValueError, TypeError):
                    # Array-valued or non-scalar cells — keep as-is
                    pass

            pid = str(raw.get("property_url") or raw.get("mls_id") or raw.get("property_id", ""))
            if not pid:
                continue

            price = raw.get("list_price")
            address_parts = [
                str(raw.get("street", "") or ""),
                str(raw.get("unit", "") or ""),
            ]
            address = " ".join(p for p in address_parts if p).strip()

            existing = self.conn.execute(
                "SELECT property_id, price FROM listings WHERE property_id = ?", (pid,)
            ).fetchone()

            hoa_fee = raw.get("hoa_fee")
            if hoa_fee is not None:
                hoa_fee = float(hoa_fee)

            if existing is None:
                self.conn.execute(
                    """INSERT INTO listings
                       (property_id, address, city, state, zip_code, price, beds, baths,
                        sqft, lot_sqft, year_built, latitude, longitude, description,
                        property_type, status, list_date, photo_url, mls_id, hoa_fee,
                        first_seen_at, last_seen_at, raw_data)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        pid,
                        address,
                        raw.get("city"),
                        raw.get("state"),
                        raw.get("zip_code"),
                        price,
                        raw.get("beds"),
                        raw.get("full_baths", raw.get("baths")),
                        raw.get("sqft"),
                        raw.get("lot_sqft"),
                        raw.get("year_built"),
                        raw.get("latitude"),
                        raw.get("longitude"),
                        raw.get("description"),
                        raw.get("style") or raw.get("property_type"),
                        raw.get("status"),
                        raw.get("list_date"),
                        raw.get("primary_photo"),
                        raw.get("mls_id"),
                        hoa_fee,
                        now,
                        now,
                        json.dumps(raw, default=str),
                    ),
                )
                new_ids.append(pid)
            else:
                self.conn.execute(
                    "UPDATE listings SET last_seen_at = ?, raw_data = ? WHERE property_id = ?",
                    (now, json.dumps(raw, default=str), pid),
                )
                if price is not None and existing["price"] is not None and abs(price - existing["price"]) > 0.01:
                    self.conn.execute(
                        "UPDATE listings SET price = ? WHERE property_id = ?",
                        (price, pid),
                    )
                    price_changed_ids.add(pid)

            # Record price history
            if price is not None:
                try:
                    self.conn.execute(
                        "INSERT INTO price_history (property_id, price, recorded_at) VALUES (?, ?, ?)",
                        (pid, price, now),
                    )
                except sqlite3.IntegrityError:
                    pass  # Same price already recorded

        self.conn.commit()
        return new_ids, list(price_changed_ids)

    # --- Preferences ---

    def add_preference(self, text: str) -> int:
        cur = self.conn.execute(
            "INSERT INTO preferences (text, active, created_at) VALUES (?, 1, ?)",
            (text, self._now()),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_active_preferences(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT id, text, created_at FROM preferences WHERE active = 1 ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]

    def deactivate_preference(self, pref_id: int):
        self.conn.execute("UPDATE preferences SET active = 0 WHERE id = ?", (pref_id,))
        self.conn.commit()

    def get_preferences_hash(self) -> str:
        prefs = self.get_active_preferences()
        parts = [p["text"] for p in prefs]
        # Include distance locations in hash
        locs = self.get_locations()
        for loc in locs:
            parts.append(f"loc:{loc['name']}:{loc['latitude']}:{loc['longitude']}:{loc['priority']}")
        # Include district state in hash
        districts = self.get_all_districts()
        for d in districts:
            parts.append(f"dist:{d['name']}:{d['rating']}:{d['excluded']}")
        text = "|".join(parts)
        return hashlib.md5(text.encode()).hexdigest()

    # --- Actions ---

    def add_action(self, property_id: str, action: str, note: Optional[str] = None):
        self.conn.execute(
            "INSERT INTO listing_actions (property_id, action, note, created_at) VALUES (?, ?, ?, ?)",
            (property_id, action, note, self._now()),
        )
        self.conn.commit()

    def get_favorites(self) -> list[dict]:
        rows = self.conn.execute("""
            SELECT DISTINCT l.* FROM listings l
            JOIN listing_actions a ON l.property_id = a.property_id
            WHERE a.action = 'favorite'
            AND l.property_id NOT IN (
                SELECT property_id FROM listing_actions WHERE action = 'reject'
                AND created_at > (SELECT MAX(created_at) FROM listing_actions
                                  WHERE action = 'favorite' AND property_id = l.property_id)
            )
            ORDER BY a.created_at DESC
        """).fetchall()
        return [dict(r) for r in rows]

    def get_rejected_ids(self) -> set[str]:
        rows = self.conn.execute("""
            SELECT DISTINCT property_id FROM listing_actions WHERE action = 'reject'
        """).fetchall()
        return {r["property_id"] for r in rows}

    def get_notes(self, property_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT note, created_at FROM listing_actions WHERE property_id = ? AND action = 'note' ORDER BY created_at",
            (property_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Scores ---

    def save_scores(self, scores: list[dict], preferences_hash: str):
        now = self._now()
        for s in scores:
            self.conn.execute(
                """INSERT OR REPLACE INTO scores (property_id, score, reasoning, preferences_hash, scored_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (s["property_id"], s["score"], s.get("reasoning", ""), preferences_hash, now),
            )
        self.conn.commit()

    def get_cached_scores(self, property_ids: list[str], preferences_hash: str) -> dict[str, dict]:
        if not property_ids:
            return {}
        placeholders = ",".join("?" * len(property_ids))
        rows = self.conn.execute(
            f"SELECT property_id, score, reasoning FROM scores WHERE property_id IN ({placeholders}) AND preferences_hash = ?",
            (*property_ids, preferences_hash),
        ).fetchall()
        return {r["property_id"]: dict(r) for r in rows}

    # --- Queries ---

    def get_listing(self, property_id: str) -> Optional[dict]:
        row = self.conn.execute("SELECT * FROM listings WHERE property_id = ?", (property_id,)).fetchone()
        return dict(row) if row else None

    def get_all_listings(self) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM listings ORDER BY last_seen_at DESC").fetchall()
        return [dict(r) for r in rows]

    def get_price_history(self, property_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT price, recorded_at FROM price_history WHERE property_id = ? ORDER BY recorded_at",
            (property_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Saved Searches ---

    def save_search(self, name: str, config_dict: dict) -> int:
        cur = self.conn.execute(
            "INSERT OR REPLACE INTO saved_searches (name, config_json, created_at) VALUES (?, ?, ?)",
            (name, json.dumps(config_dict), self._now()),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_saved_searches(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT id, name, config_json, created_at, last_used_at FROM saved_searches ORDER BY last_used_at DESC NULLS LAST, created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def load_search(self, id_or_name) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT id, name, config_json, created_at, last_used_at FROM saved_searches WHERE id = ? OR name = ?",
            (id_or_name, str(id_or_name)),
        ).fetchone()
        if row:
            self.conn.execute(
                "UPDATE saved_searches SET last_used_at = ? WHERE id = ?",
                (self._now(), row["id"]),
            )
            self.conn.commit()
            return dict(row)
        return None

    def delete_search(self, id_or_name) -> bool:
        cur = self.conn.execute(
            "DELETE FROM saved_searches WHERE id = ? OR name = ?",
            (id_or_name, str(id_or_name)),
        )
        self.conn.commit()
        return cur.rowcount > 0

    # --- Distance Locations ---

    def add_location(self, name: str, lat: float, lon: float, priority: int = 1) -> int:
        cur = self.conn.execute(
            "INSERT INTO distance_locations (name, latitude, longitude, priority, created_at) VALUES (?, ?, ?, ?, ?)",
            (name, lat, lon, priority, self._now()),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_locations(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT id, name, latitude, longitude, priority, created_at FROM distance_locations ORDER BY priority DESC, id"
        ).fetchall()
        return [dict(r) for r in rows]

    def remove_location(self, loc_id: int) -> bool:
        cur = self.conn.execute("DELETE FROM distance_locations WHERE id = ?", (loc_id,))
        self.conn.commit()
        return cur.rowcount > 0

    def get_cached_geocode(self, name: str) -> Optional[tuple[float, float]]:
        row = self.conn.execute(
            "SELECT latitude, longitude FROM geocode_cache WHERE place_name = ?", (name,)
        ).fetchone()
        if row:
            return (row["latitude"], row["longitude"])
        return None

    def cache_geocode(self, name: str, lat: float, lon: float):
        self.conn.execute(
            "INSERT OR REPLACE INTO geocode_cache (place_name, latitude, longitude, cached_at) VALUES (?, ?, ?, ?)",
            (name, lat, lon, self._now()),
        )
        self.conn.commit()

    # --- School Districts ---

    def add_district(self, name: str, state: Optional[str] = None) -> int:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO school_districts (name, state, created_at) VALUES (?, ?, ?)",
            (name, state, self._now()),
        )
        self.conn.commit()
        if cur.lastrowid:
            return cur.lastrowid
        # Already exists, return existing ID
        row = self.conn.execute(
            "SELECT id FROM school_districts WHERE name = ? AND (state = ? OR (state IS NULL AND ? IS NULL))",
            (name, state, state),
        ).fetchone()
        return row["id"] if row else 0

    def set_district_for_zip(self, zip_code: str, district_id: int):
        self.conn.execute(
            "INSERT OR REPLACE INTO zip_district_map (zip_code, district_id) VALUES (?, ?)",
            (zip_code, district_id),
        )
        self.conn.commit()

    def assign_district_to_zip(self, name: str, zip_code: str, state: Optional[str] = None) -> int:
        district_id = self.add_district(name, state)
        self.set_district_for_zip(zip_code, district_id)
        return district_id

    def get_district_for_zip(self, zip_code: str) -> Optional[dict]:
        row = self.conn.execute(
            """SELECT d.id, d.name, d.state, d.rating, d.excluded
               FROM school_districts d
               JOIN zip_district_map z ON d.id = z.district_id
               WHERE z.zip_code = ?""",
            (zip_code,),
        ).fetchone()
        return dict(row) if row else None

    def get_district_for_listing(self, property_id: str) -> Optional[dict]:
        """Get district for a listing — prefers direct FK, falls back to zip lookup."""
        listing = self.get_listing(property_id)
        if not listing:
            return None
        if listing.get("district_id"):
            row = self.conn.execute(
                "SELECT id, name, state, rating, excluded, nces_id, school_count, enrollment, student_teacher_ratio FROM school_districts WHERE id = ?",
                (listing["district_id"],),
            ).fetchone()
            if row:
                return dict(row)
        if listing.get("zip_code"):
            return self.get_district_for_zip(listing["zip_code"])
        return None

    def get_all_districts(self) -> list[dict]:
        rows = self.conn.execute(
            """SELECT d.id, d.name, d.state, d.rating, d.excluded, d.created_at,
                      d.nces_id, d.school_count, d.enrollment, d.student_teacher_ratio,
                      GROUP_CONCAT(DISTINCT z.zip_code) as zip_codes,
                      COUNT(DISTINCT l.property_id) as listing_count
               FROM school_districts d
               LEFT JOIN zip_district_map z ON d.id = z.district_id
               LEFT JOIN listings l ON l.district_id = d.id
               GROUP BY d.id
               ORDER BY d.name"""
        ).fetchall()
        return [dict(r) for r in rows]

    def exclude_district(self, district_id: int, excluded: bool = True):
        self.conn.execute(
            "UPDATE school_districts SET excluded = ? WHERE id = ?",
            (1 if excluded else 0, district_id),
        )
        self.conn.commit()

    def set_district_rating(self, district_id: int, rating: int):
        self.conn.execute(
            "UPDATE school_districts SET rating = ? WHERE id = ?",
            (rating, district_id),
        )
        self.conn.commit()

    def get_excluded_zips(self) -> set[str]:
        rows = self.conn.execute(
            """SELECT z.zip_code FROM zip_district_map z
               JOIN school_districts d ON z.district_id = d.id
               WHERE d.excluded = 1"""
        ).fetchall()
        return {r["zip_code"] for r in rows}

    def _find_district(self, id_or_name) -> Optional[dict]:
        """Look up a district by ID or case-insensitive name."""
        # Try ID first
        try:
            row = self.conn.execute(
                "SELECT id, name, state, rating, excluded FROM school_districts WHERE id = ?",
                (int(id_or_name),),
            ).fetchone()
            if row:
                return dict(row)
        except (ValueError, TypeError):
            pass
        # Try name
        row = self.conn.execute(
            "SELECT id, name, state, rating, excluded FROM school_districts WHERE LOWER(name) = LOWER(?)",
            (str(id_or_name),),
        ).fetchone()
        return dict(row) if row else None

    # --- Score Snapshots ---

    def save_snapshot(self, name: str) -> int:
        prefs = self.get_active_preferences()
        locations = self.get_locations()
        districts = self.get_all_districts()
        pref_hash = self.get_preferences_hash()
        cur = self.conn.execute(
            """INSERT OR REPLACE INTO score_snapshots
               (name, preferences_hash, preferences_json, locations_json, districts_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (name, pref_hash, json.dumps(prefs), json.dumps(locations), json.dumps(districts), self._now()),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_snapshots(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT id, name, preferences_hash, created_at FROM score_snapshots ORDER BY created_at DESC"
        ).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            # Compute stats from scores table
            stats = self.conn.execute(
                "SELECT COUNT(*) as cnt, AVG(score) as avg FROM scores WHERE preferences_hash = ?",
                (d["preferences_hash"],),
            ).fetchone()
            d["listing_count"] = stats["cnt"] if stats else 0
            d["avg_score"] = round(stats["avg"], 1) if stats and stats["avg"] else None
            results.append(d)
        return results

    def get_snapshot(self, id_or_name) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM score_snapshots WHERE id = ? OR name = ?",
            (id_or_name, str(id_or_name)),
        ).fetchone()
        return dict(row) if row else None

    def delete_snapshot(self, id_or_name) -> bool:
        cur = self.conn.execute(
            "DELETE FROM score_snapshots WHERE id = ? OR name = ?",
            (id_or_name, str(id_or_name)),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def restore_snapshot(self, id_or_name) -> Optional[str]:
        """Restore prefs/locations/districts from a snapshot. Returns snapshot name or None."""
        snap = self.get_snapshot(id_or_name)
        if not snap:
            return None

        prefs = json.loads(snap["preferences_json"])
        locations = json.loads(snap["locations_json"])
        districts = json.loads(snap["districts_json"])

        # Deactivate all current preferences
        self.conn.execute("UPDATE preferences SET active = 0")

        # Insert snapshot preferences as new active rows
        for p in prefs:
            self.conn.execute(
                "INSERT INTO preferences (text, active, created_at) VALUES (?, 1, ?)",
                (p["text"], self._now()),
            )

        # Replace locations
        self.conn.execute("DELETE FROM distance_locations")
        for loc in locations:
            self.conn.execute(
                "INSERT INTO distance_locations (name, latitude, longitude, priority, created_at) VALUES (?, ?, ?, ?, ?)",
                (loc["name"], loc["latitude"], loc["longitude"], loc["priority"], self._now()),
            )

        # Update districts: match by name+state, update rating/excluded
        for d in districts:
            self.conn.execute(
                """UPDATE school_districts SET rating = ?, excluded = ?
                   WHERE LOWER(name) = LOWER(?) AND (state = ? OR (state IS NULL AND ? IS NULL))""",
                (d.get("rating"), d.get("excluded", 0), d["name"], d.get("state"), d.get("state")),
            )

        self.conn.commit()
        return snap["name"]

    def get_snapshot_scores(self, preferences_hash: str, property_ids: list[str]) -> dict[str, dict]:
        if not property_ids:
            return {}
        placeholders = ",".join("?" * len(property_ids))
        rows = self.conn.execute(
            f"SELECT property_id, score, reasoning FROM scores WHERE property_id IN ({placeholders}) AND preferences_hash = ?",
            (*property_ids, preferences_hash),
        ).fetchall()
        return {r["property_id"]: dict(r) for r in rows}

    # --- NCES District Methods ---

    def upsert_district_from_nces(self, nces_data: dict) -> int:
        """Find or create a district from NCES data. Returns district ID."""
        nces_id = nces_data.get("nces_id")
        name = nces_data.get("name")
        state = nces_data.get("state")

        # Try to find by nces_id first
        if nces_id:
            row = self.conn.execute(
                "SELECT id FROM school_districts WHERE nces_id = ?", (nces_id,)
            ).fetchone()
            if row:
                # Update stats
                self.conn.execute(
                    """UPDATE school_districts SET school_count = ?, enrollment = ?,
                       student_teacher_ratio = ? WHERE id = ?""",
                    (nces_data.get("school_count"), nces_data.get("enrollment"),
                     nces_data.get("student_teacher_ratio"), row["id"]),
                )
                self.conn.commit()
                return row["id"]

        # Try to find by name+state
        row = self.conn.execute(
            "SELECT id FROM school_districts WHERE LOWER(name) = LOWER(?) AND (state = ? OR (state IS NULL AND ? IS NULL))",
            (name, state, state),
        ).fetchone()
        if row:
            # Update with NCES data
            self.conn.execute(
                """UPDATE school_districts SET nces_id = ?, school_count = ?, enrollment = ?,
                   student_teacher_ratio = ? WHERE id = ?""",
                (nces_id, nces_data.get("school_count"), nces_data.get("enrollment"),
                 nces_data.get("student_teacher_ratio"), row["id"]),
            )
            self.conn.commit()
            return row["id"]

        # Insert new
        cur = self.conn.execute(
            """INSERT INTO school_districts (name, state, nces_id, school_count, enrollment,
               student_teacher_ratio, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (name, state, nces_id, nces_data.get("school_count"), nces_data.get("enrollment"),
             nces_data.get("student_teacher_ratio"), self._now()),
        )
        self.conn.commit()
        return cur.lastrowid

    def set_listing_district(self, property_id: str, district_id: int):
        self.conn.execute(
            "UPDATE listings SET district_id = ? WHERE property_id = ?",
            (district_id, property_id),
        )
        self.conn.commit()

    def get_listings_without_district(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM listings WHERE district_id IS NULL AND latitude IS NOT NULL AND longitude IS NOT NULL"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_excluded_district_ids(self) -> set[int]:
        rows = self.conn.execute(
            "SELECT id FROM school_districts WHERE excluded = 1"
        ).fetchall()
        return {r["id"] for r in rows}

    def close(self):
        self.conn.close()
