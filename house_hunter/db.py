import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import pandas as pd


class Database:
    def __init__(self, db_path: str = "house_hunter.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
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
        """)
        self.conn.commit()
        self._migrate()

    def _migrate(self):
        """Add columns that may not exist in older databases."""
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(listings)").fetchall()}
        if "hoa_fee" not in cols:
            self.conn.execute("ALTER TABLE listings ADD COLUMN hoa_fee REAL")
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
        text = "|".join(p["text"] for p in prefs)
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

    def close(self):
        self.conn.close()
