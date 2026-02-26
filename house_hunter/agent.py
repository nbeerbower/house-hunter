import re
from dataclasses import asdict
from typing import Optional

from house_hunter.config import AppConfig, SearchConfig
from house_hunter.db import Database
from house_hunter.distance import compute_distances, geocode
from house_hunter.llm import LLM
from house_hunter.prompts import (
    build_chat_system_prompt,
    format_listing_for_prompt,
)
from house_hunter.scraper import search_properties


class Agent:
    def __init__(self, config: AppConfig):
        self.config = config
        self.db = Database(config.db_path)
        self.llm = LLM(config.llm)
        self.current_results: list[dict] = []  # Scored and ranked listings
        self.current_listings: dict[str, dict] = {}  # property_id -> listing dict
        self.index_map: dict[int, str] = {}  # display index -> property_id
        self.current_distances: dict[str, list[dict]] = {}  # property_id -> distances
        self.current_districts: dict[str, dict] = {}  # property_id -> district

    def run_search(self, search_config: Optional[SearchConfig] = None) -> tuple[list[str], list[str]]:
        """Scrape listings, upsert to DB, score, and rank. Returns (new_ids, price_changed_ids)."""
        config = search_config or self.config.search
        print(f"Searching for properties in {config.location}...")

        df = search_properties(config)
        if df.empty:
            print("No results found.")
            return [], []

        print(f"Found {len(df)} listings from scraper.")

        # Upsert to database
        new_ids, price_changed_ids = self.db.upsert_listings(df)
        if new_ids:
            print(f"  {len(new_ids)} new listings")
        if price_changed_ids:
            print(f"  {len(price_changed_ids)} price changes detected")

        # Build listing dicts from DB (gets normalized data)
        all_listings = self.db.get_all_listings()
        # Filter to only listings from current search (those that appear in df)
        search_pids = set()
        for _, row in df.iterrows():
            pid = str(row.get("property_url") or row.get("mls_id") or row.get("property_id", ""))
            if pid:
                search_pids.add(pid)

        listings = [l for l in all_listings if l["property_id"] in search_pids]

        # Exclude HOA listings if requested
        if self.config.exclude_hoa:
            before = len(listings)
            listings = [l for l in listings if not l.get("hoa_fee") or l["hoa_fee"] == 0]
            excluded = before - len(listings)
            if excluded:
                print(f"  Excluded {excluded} listings with HOA fees")

        # Exclude rejected
        rejected = self.db.get_rejected_ids()
        listings = [l for l in listings if l["property_id"] not in rejected]
        if rejected:
            print(f"  Excluded {len(rejected)} rejected listings")

        # Auto-populate districts for new listings with lat/lon
        if new_ids:
            from house_hunter.schools import populate_districts
            new_listings = [l for l in listings if l["property_id"] in set(new_ids) and l.get("latitude") and l.get("longitude")]
            if new_listings:
                populate_districts(self.db, new_listings, quiet=True)
                # Refresh listing data to get district_id
                all_listings = self.db.get_all_listings()
                listings = [l for l in all_listings if l["property_id"] in search_pids]
                # Re-apply HOA/rejection filters
                if self.config.exclude_hoa:
                    listings = [l for l in listings if not l.get("hoa_fee") or l["hoa_fee"] == 0]
                listings = [l for l in listings if l["property_id"] not in rejected]

        # Exclude listings in excluded school districts (by district_id and zip)
        excluded_district_ids = self.db.get_excluded_district_ids()
        excluded_zips = self.db.get_excluded_zips()
        if excluded_district_ids or excluded_zips:
            before = len(listings)
            listings = [
                l for l in listings
                if l.get("district_id") not in excluded_district_ids
                and l.get("zip_code") not in excluded_zips
            ]
            excluded_count = before - len(listings)
            if excluded_count:
                print(f"  Excluded {excluded_count} listings in excluded school districts")

        if not listings:
            print("No listings to score after filtering.")
            return new_ids, price_changed_ids

        # Store listings for reference
        self.current_listings = {l["property_id"]: l for l in listings}

        # Score listings
        self._score_listings(listings)

        return new_ids, price_changed_ids

    def _score_listings(self, listings: list[dict]):
        """Score listings against preferences using LLM."""
        prefs = self.db.get_active_preferences()
        pref_texts = [p["text"] for p in prefs]
        pref_hash = self.db.get_preferences_hash()
        favorites = self.db.get_favorites()
        rejected_ids = self.db.get_rejected_ids()

        # Build rejection summary
        rej_summary = ""
        if rejected_ids:
            rej_count = len(rejected_ids)
            rej_summary = f"{rej_count} properties rejected by buyer"

        # Compute distances for listings with lat/lon
        locations = self.db.get_locations()
        distances_map = {}
        if locations:
            for l in listings:
                lat, lon = l.get("latitude"), l.get("longitude")
                if lat and lon:
                    distances_map[l["property_id"]] = compute_distances(lat, lon, locations)
        self.current_distances = distances_map

        # Build districts map (prefer direct FK, fall back to zip)
        districts_map = {}
        for l in listings:
            district = self.db.get_district_for_listing(l["property_id"])
            if district:
                districts_map[l["property_id"]] = district
        self.current_districts = districts_map

        # Load cached scores
        pids = [l["property_id"] for l in listings]
        cached = self.db.get_cached_scores(pids, pref_hash)

        uncached_count = len(pids) - len(cached)
        if uncached_count > 0:
            print(f"\nScoring {uncached_count} listings with {self.config.llm.display_name}...")
            if cached:
                print(f"  ({len(cached)} cached scores reused)")

        def on_progress(batch_num, total_batches):
            print(f"  Batch {batch_num}/{total_batches}...")

        results = self.llm.score_all_listings(
            listings, pref_texts, favorites, rej_summary,
            cached, on_progress if uncached_count > 0 else None,
            distances_map=distances_map if distances_map else None,
            districts_map=districts_map if districts_map else None,
            distance_locations=locations if locations else None,
        )

        # Save new scores
        new_scores = [r for r in results if r["property_id"] not in cached]
        if new_scores:
            self.db.save_scores(new_scores, pref_hash)

        self.current_results = results
        self._rebuild_index_map()

    def _rebuild_index_map(self):
        """Rebuild the display index -> property_id mapping."""
        self.index_map = {}
        for i, r in enumerate(self.current_results, 1):
            self.index_map[i] = r["property_id"]

    def get_shortlist(self, top_n: int = 10) -> list[dict]:
        """Get top N scored results with listing data merged in."""
        results = []
        for i, r in enumerate(self.current_results[:top_n], 1):
            listing = self.current_listings.get(r["property_id"], {})
            results.append({
                "index": i,
                "property_id": r["property_id"],
                "score": r["score"],
                "reasoning": r["reasoning"],
                **listing,
            })
        return results

    def handle_feedback(self, user_input: str) -> str:
        """Process user input — regex for commands, LLM fallback for natural language."""
        text = user_input.strip()
        if not text:
            return ""

        # --- Regex command matching ---

        # favorite #N
        m = re.match(r'^(?:fav(?:orite)?)\s+#?(\d+)$', text, re.IGNORECASE)
        if m:
            return self._cmd_favorite(int(m.group(1)))

        # reject #N
        m = re.match(r'^(?:reject|skip|pass)\s+#?(\d+)$', text, re.IGNORECASE)
        if m:
            return self._cmd_reject(int(m.group(1)))

        # detail #N
        m = re.match(r'^(?:detail|details|info|show)\s+#?(\d+)$', text, re.IGNORECASE)
        if m:
            return self.detail_listing_by_index(int(m.group(1)))

        # compare #N #M ...
        m = re.match(r'^compare\s+((?:#?\d+\s*)+)$', text, re.IGNORECASE)
        if m:
            indices = [int(x) for x in re.findall(r'\d+', m.group(1))]
            return self._cmd_compare(indices)

        # note #N <text>
        m = re.match(r'^note\s+#?(\d+)\s+(.+)$', text, re.IGNORECASE)
        if m:
            return self._cmd_note(int(m.group(1)), m.group(2))

        # save search <name>
        m = re.match(r'^save\s+search\s+["\']?(.+?)["\']?$', text, re.IGNORECASE)
        if m:
            return self._cmd_save_search(m.group(1))

        # load search <name|id>
        m = re.match(r'^load\s+search\s+["\']?(.+?)["\']?$', text, re.IGNORECASE)
        if m:
            return self._cmd_load_search(m.group(1))

        # delete search <name|id>
        m = re.match(r'^delete\s+search\s+["\']?(.+?)["\']?$', text, re.IGNORECASE)
        if m:
            return self._cmd_delete_search(m.group(1))

        # searches
        if re.match(r'^searches$', text, re.IGNORECASE):
            return self._cmd_list_searches()

        # add location <name> [priority N]
        m = re.match(r'^add\s+location\s+["\']?(.+?)["\']?(?:\s+priority\s+(\d+))?$', text, re.IGNORECASE)
        if m:
            return self._cmd_add_location(m.group(1), int(m.group(2)) if m.group(2) else 1)

        # locations
        if re.match(r'^locations$', text, re.IGNORECASE):
            return self._cmd_list_locations()

        # remove location <id>
        m = re.match(r'^remove\s+location\s+(\d+)$', text, re.IGNORECASE)
        if m:
            return self._cmd_remove_location(int(m.group(1)))

        # assign district <name> to <zip>
        m = re.match(r'^assign\s+district\s+["\']?(.+?)["\']?\s+to\s+(\d{5})$', text, re.IGNORECASE)
        if m:
            return self._cmd_assign_district(m.group(1), m.group(2))

        # districts
        if re.match(r'^districts$', text, re.IGNORECASE):
            return self._cmd_list_districts()

        # exclude district <name|id>
        m = re.match(r'^exclude\s+district\s+["\']?(.+?)["\']?$', text, re.IGNORECASE)
        if m:
            return self._cmd_exclude_district(m.group(1), True)

        # include district <name|id>
        m = re.match(r'^include\s+district\s+["\']?(.+?)["\']?$', text, re.IGNORECASE)
        if m:
            return self._cmd_exclude_district(m.group(1), False)

        # rate district <name|id> <1-10>
        m = re.match(r'^rate\s+district\s+["\']?(.+?)["\']?\s+(\d+)$', text, re.IGNORECASE)
        if m:
            return self._cmd_rate_district(m.group(1), int(m.group(2)))

        # populate districts
        if re.match(r'^populate\s+districts?$', text, re.IGNORECASE):
            return self._cmd_populate_districts()

        # fetch ratings
        if re.match(r'^fetch\s+ratings?$', text, re.IGNORECASE):
            return self._cmd_fetch_ratings()

        # snapshot <name> (save)
        m = re.match(r'^snapshot\s+["\']?(.+?)["\']?$', text, re.IGNORECASE)
        if m:
            return self._cmd_save_snapshot(m.group(1))

        # snapshots (list)
        if re.match(r'^snapshots$', text, re.IGNORECASE):
            return self._cmd_list_snapshots()

        # compare snapshot <name>
        m = re.match(r'^compare\s+snapshot\s+["\']?(.+?)["\']?$', text, re.IGNORECASE)
        if m:
            return self._cmd_compare_snapshot(m.group(1))

        # restore snapshot <name>
        m = re.match(r'^restore\s+snapshot\s+["\']?(.+?)["\']?$', text, re.IGNORECASE)
        if m:
            return self._cmd_restore_snapshot(m.group(1))

        # delete snapshot <name>
        m = re.match(r'^delete\s+snapshot\s+["\']?(.+?)["\']?$', text, re.IGNORECASE)
        if m:
            return self._cmd_delete_snapshot(m.group(1))

        # search <location>
        m = re.match(r'^search\s+(.+)$', text, re.IGNORECASE)
        if m:
            return self._cmd_search(m.group(1))

        # show favorites
        if re.match(r'^(?:show\s+)?fav(?:orite)?s$', text, re.IGNORECASE):
            return self._cmd_show_favorites()

        # show preferences
        if re.match(r'^(?:show\s+)?pref(?:erence)?s$', text, re.IGNORECASE):
            return self._cmd_show_preferences()

        # remove preference N
        m = re.match(r'^(?:remove|delete)\s+pref(?:erence)?\s+(\d+)$', text, re.IGNORECASE)
        if m:
            return self._cmd_remove_preference(int(m.group(1)))

        # price history #N
        m = re.match(r'^(?:price\s+)?history\s+#?(\d+)$', text, re.IGNORECASE)
        if m:
            return self._cmd_price_history(int(m.group(1)))

        # refresh
        if re.match(r'^refresh$', text, re.IGNORECASE):
            return self._cmd_refresh()

        # top/list N
        m = re.match(r'^(?:top|list)\s*(\d+)?$', text, re.IGNORECASE)
        if m:
            n = int(m.group(1)) if m.group(1) else 10
            return self._cmd_show_top(n)

        # help
        if re.match(r'^help$', text, re.IGNORECASE):
            return self._cmd_help()

        # --- LLM fallback for natural language ---
        return self._handle_natural_language(text)

    def _resolve_index(self, index: int) -> Optional[str]:
        return self.index_map.get(index)

    def _cmd_favorite(self, index: int) -> str:
        pid = self._resolve_index(index)
        if not pid:
            return f"No listing #{index} found."
        listing = self.current_listings.get(pid, {})
        self.db.add_action(pid, "favorite")
        addr = listing.get("address", pid)
        return f"Favorited #{index}: {addr}"

    def _cmd_reject(self, index: int) -> str:
        pid = self._resolve_index(index)
        if not pid:
            return f"No listing #{index} found."
        listing = self.current_listings.get(pid, {})
        self.db.add_action(pid, "reject")
        addr = listing.get("address", pid)
        # Remove from current results
        self.current_results = [r for r in self.current_results if r["property_id"] != pid]
        self._rebuild_index_map()
        return f"Rejected #{index}: {addr} (removed from results)"

    def _cmd_note(self, index: int, note_text: str) -> str:
        pid = self._resolve_index(index)
        if not pid:
            return f"No listing #{index} found."
        self.db.add_action(pid, "note", note_text)
        return f"Note added to #{index}."

    def _cmd_compare(self, indices: list[int]) -> str:
        listings = []
        for idx in indices:
            pid = self._resolve_index(idx)
            if not pid:
                return f"No listing #{idx} found."
            listing = self.current_listings.get(pid)
            if listing:
                listings.append(listing)
        if len(listings) < 2:
            return "Need at least 2 listings to compare."
        return self.llm.compare(listings)

    def _cmd_search(self, location: str) -> str:
        new_config = SearchConfig(
            location=location,
            listing_type=self.config.search.listing_type,
            price_min=self.config.search.price_min,
            price_max=self.config.search.price_max,
            beds_min=self.config.search.beds_min,
            beds_max=self.config.search.beds_max,
            baths_min=self.config.search.baths_min,
            baths_max=self.config.search.baths_max,
            sqft_min=self.config.search.sqft_min,
            sqft_max=self.config.search.sqft_max,
            limit=self.config.search.limit,
            extra_property_data=self.config.search.extra_property_data,
        )
        self.run_search(new_config)
        if self.current_results:
            return self._format_shortlist(self.get_shortlist())
        return "No results found for that location."

    # --- Saved Searches ---

    def _cmd_save_search(self, name: str) -> str:
        config_dict = asdict(self.config.search)
        self.db.save_search(name, config_dict)
        return f"Saved search '{name}' ({self.config.search.location})."

    def _cmd_load_search(self, id_or_name: str) -> str:
        # Try as int ID first
        try:
            lookup = int(id_or_name)
        except ValueError:
            lookup = id_or_name
        row = self.db.load_search(lookup)
        if not row:
            return f"No saved search '{id_or_name}' found."
        import json
        config_dict = json.loads(row["config_json"])
        search_config = SearchConfig(**{k: v for k, v in config_dict.items() if k in SearchConfig.__dataclass_fields__})
        self.config.search = search_config
        self.run_search(search_config)
        result = f"Loaded search '{row['name']}' ({search_config.location})."
        if self.current_results:
            result += "\n\n" + self._format_shortlist(self.get_shortlist())
        return result

    def _cmd_delete_search(self, id_or_name: str) -> str:
        try:
            lookup = int(id_or_name)
        except ValueError:
            lookup = id_or_name
        if self.db.delete_search(lookup):
            return f"Deleted saved search '{id_or_name}'."
        return f"No saved search '{id_or_name}' found."

    def _cmd_list_searches(self) -> str:
        searches = self.db.get_saved_searches()
        if not searches:
            return "No saved searches. Use 'save search <name>' to save the current search."
        import json
        lines = ["Saved searches:"]
        for s in searches:
            config = json.loads(s["config_json"])
            location = config.get("location", "?")
            last_used = s["last_used_at"][:10] if s.get("last_used_at") else "never"
            lines.append(f"  [{s['id']}] {s['name']} — {location} (last used: {last_used})")
        lines.append("\nUse 'load search <name>' to load one.")
        return "\n".join(lines)

    # --- Distance Locations ---

    def _cmd_add_location(self, name: str, priority: int = 1) -> str:
        # Check limit
        existing = self.db.get_locations()
        if len(existing) >= 3:
            return "Maximum 3 locations allowed. Remove one first with 'remove location <id>'."

        # Check geocode cache first
        cached = self.db.get_cached_geocode(name)
        if cached:
            lat, lon = cached
        else:
            result = geocode(name)
            if not result:
                return f"Could not geocode '{name}'. Try a more specific place name."
            lat, lon = result
            self.db.cache_geocode(name, lat, lon)

        loc_id = self.db.add_location(name, lat, lon, priority)
        return f"Added location [{loc_id}]: {name} ({lat:.4f}, {lon:.4f}) priority {priority}"

    def _cmd_list_locations(self) -> str:
        locations = self.db.get_locations()
        if not locations:
            return "No distance locations set. Use 'add location <place>' to add one."
        lines = ["Distance locations:"]
        for loc in locations:
            lines.append(f"  [{loc['id']}] {loc['name']} — priority {loc['priority']} ({loc['latitude']:.4f}, {loc['longitude']:.4f})")
        lines.append("\nUse 'remove location <id>' to remove one.")
        return "\n".join(lines)

    def _cmd_remove_location(self, loc_id: int) -> str:
        if self.db.remove_location(loc_id):
            return f"Removed location {loc_id}. Use 'refresh' to re-score."
        return f"No location with ID {loc_id} found."

    # --- School Districts ---

    def _cmd_assign_district(self, name: str, zip_code: str) -> str:
        district_id = self.db.assign_district_to_zip(name, zip_code)
        return f"Assigned zip {zip_code} to district '{name}' [{district_id}]."

    def _cmd_list_districts(self) -> str:
        districts = self.db.get_all_districts()
        if not districts:
            return "No school districts. Use 'assign district <name> to <zip>' or 'populate districts' to add them."
        lines = ["School districts:"]
        for d in districts:
            zips = d.get("zip_codes") or "no zips"
            rating = f"rated {d['rating']}/10" if d.get("rating") else "unrated"
            status = "EXCLUDED" if d["excluded"] else "active"
            extras = []
            if d.get("enrollment"):
                extras.append(f"{d['enrollment']:,} students")
            if d.get("school_count"):
                extras.append(f"{d['school_count']} schools")
            if d.get("student_teacher_ratio"):
                extras.append(f"{d['student_teacher_ratio']:.0f}:1 ratio")
            if d.get("listing_count"):
                extras.append(f"{d['listing_count']} listings")
            extra_str = f" ({', '.join(extras)})" if extras else ""
            lines.append(f"  [{d['id']}] {d['name']} — {rating}, {status}, zips: {zips}{extra_str}")
        lines.append("\nCommands: rate district <name> <1-10>, exclude/include district <name>, populate districts, fetch ratings")
        return "\n".join(lines)

    def _cmd_exclude_district(self, id_or_name: str, exclude: bool) -> str:
        district = self.db._find_district(id_or_name)
        if not district:
            return f"No district '{id_or_name}' found."
        self.db.exclude_district(district["id"], exclude)
        action = "Excluded" if exclude else "Included"
        return f"{action} district '{district['name']}'. Use 'refresh' to re-score."

    def _cmd_rate_district(self, id_or_name: str, rating: int) -> str:
        if rating < 1 or rating > 10:
            return "Rating must be between 1 and 10."
        district = self.db._find_district(id_or_name)
        if not district:
            return f"No district '{id_or_name}' found."
        self.db.set_district_rating(district["id"], rating)
        return f"Rated district '{district['name']}' {rating}/10. Use 'refresh' to re-score."

    # --- Score Snapshots ---

    def _cmd_save_snapshot(self, name: str) -> str:
        self.db.save_snapshot(name)
        return f"Saved snapshot '{name}' with current preferences, locations, and districts."

    def _cmd_list_snapshots(self) -> str:
        snapshots = self.db.get_snapshots()
        if not snapshots:
            return "No snapshots. Use 'snapshot <name>' to save the current scoring state."
        current_hash = self.db.get_preferences_hash()
        lines = ["Score snapshots:"]
        for s in snapshots:
            current_tag = " (current)" if s["preferences_hash"] == current_hash else ""
            avg = f"avg {s['avg_score']}" if s.get("avg_score") else "no scores"
            lines.append(f"  [{s['id']}] {s['name']} — {s['listing_count']} listings, {avg}, {s['created_at'][:10]}{current_tag}")
        lines.append("\nCommands: compare snapshot <name>, restore snapshot <name>, delete snapshot <name>")
        return "\n".join(lines)

    def _cmd_compare_snapshot(self, name: str) -> str:
        snap = self.db.get_snapshot(name)
        if not snap:
            return f"No snapshot '{name}' found."
        if not self.current_listings:
            return "No listings loaded. Run a search first."

        pids = list(self.current_listings.keys())
        old_scores = self.db.get_snapshot_scores(snap["preferences_hash"], pids)
        current_hash = self.db.get_preferences_hash()
        new_scores = self.db.get_cached_scores(pids, current_hash)

        if not old_scores and not new_scores:
            return f"No scores found for snapshot '{snap['name']}' or current state."

        rows = []
        for pid in pids:
            listing = self.current_listings[pid]
            old = old_scores.get(pid, {}).get("score")
            new = new_scores.get(pid, {}).get("score")
            if old is None and new is None:
                continue
            delta = (new or 0) - (old or 0) if old is not None and new is not None else None
            rows.append((listing.get("address", pid[:30]), old, new, delta))

        if not rows:
            return "No comparable scores found."

        # Sort by absolute delta
        rows.sort(key=lambda r: abs(r[3]) if r[3] is not None else 0, reverse=True)

        lines = [f"Comparison with snapshot '{snap['name']}':", ""]
        lines.append(f"  {'Address':<35} {'Old':>5} {'New':>5} {'Delta':>6}")
        lines.append(f"  {'-'*35} {'-'*5} {'-'*5} {'-'*6}")

        improved = declined = stable = 0
        total_delta = 0.0
        count_delta = 0

        for addr, old, new, delta in rows:
            old_str = f"{old:.1f}" if old is not None else "  —"
            new_str = f"{new:.1f}" if new is not None else "  —"
            if delta is not None:
                sign = "+" if delta > 0 else ""
                delta_str = f"{sign}{delta:.1f}"
                total_delta += delta
                count_delta += 1
                if delta > 0.1:
                    improved += 1
                elif delta < -0.1:
                    declined += 1
                else:
                    stable += 1
            else:
                delta_str = "  —"
            lines.append(f"  {addr[:35]:<35} {old_str:>5} {new_str:>5} {delta_str:>6}")

        avg_delta = total_delta / count_delta if count_delta else 0
        lines.append("")
        lines.append(f"  Summary: avg delta {avg_delta:+.2f} | {improved} improved, {declined} declined, {stable} stable")
        return "\n".join(lines)

    def _cmd_restore_snapshot(self, name: str) -> str:
        restored = self.db.restore_snapshot(name)
        if not restored:
            return f"No snapshot '{name}' found."
        return f"Restored snapshot '{restored}'. Preferences, locations, and districts reverted. Use 'refresh' to re-score."

    def _cmd_delete_snapshot(self, name: str) -> str:
        if self.db.delete_snapshot(name):
            return f"Deleted snapshot '{name}'."
        return f"No snapshot '{name}' found."

    # --- Auto District Mapping ---

    def _cmd_populate_districts(self) -> str:
        from house_hunter.schools import populate_districts
        listings = list(self.current_listings.values()) if self.current_listings else self.db.get_listings_without_district()
        if not listings:
            return "No listings to process."
        result = populate_districts(self.db, listings)
        return result

    def _cmd_fetch_ratings(self) -> str:
        from house_hunter.schools import fetch_all_ratings
        result = fetch_all_ratings(self.db)
        return result

    # --- Existing commands ---

    def _cmd_show_favorites(self) -> str:
        favs = self.db.get_favorites()
        if not favs:
            return "No favorites yet."
        lines = ["Favorites:"]
        for i, f in enumerate(favs, 1):
            price = f.get("price")
            price_str = f"${price:,.0f}" if price else "N/A"
            lines.append(f"  {i}. {price_str} | {f.get('beds', '?')}bd/{f.get('baths', '?')}ba | {f.get('address', '')}, {f.get('city', '')}")
        return "\n".join(lines)

    def _cmd_show_preferences(self) -> str:
        prefs = self.db.get_active_preferences()
        if not prefs:
            return "No preferences set. Tell me what you're looking for!"
        lines = ["Active preferences:"]
        for p in prefs:
            lines.append(f"  [{p['id']}] {p['text']}")
        lines.append("\nUse 'remove preference <id>' to remove one.")
        return "\n".join(lines)

    def _cmd_remove_preference(self, pref_id: int) -> str:
        self.db.deactivate_preference(pref_id)
        return f"Preference {pref_id} removed. Use 'refresh' to re-score with updated preferences."

    def _cmd_price_history(self, index: int) -> str:
        pid = self._resolve_index(index)
        if not pid:
            return f"No listing #{index} found."
        history = self.db.get_price_history(pid)
        listing = self.current_listings.get(pid, {})
        addr = listing.get("address", pid)
        if not history:
            return f"No price history for #{index}: {addr}"
        lines = [f"Price history for #{index}: {addr}"]
        for h in history:
            lines.append(f"  ${h['price']:,.0f} — {h['recorded_at'][:10]}")
        return "\n".join(lines)

    def _cmd_refresh(self) -> str:
        if not self.current_listings:
            return "No listings loaded. Run a search first."
        listings = list(self.current_listings.values())
        self._score_listings(listings)
        return "Scores refreshed.\n\n" + self._format_shortlist(self.get_shortlist())

    def _cmd_show_top(self, n: int) -> str:
        if not self.current_results:
            return "No results. Run a search first."
        return self._format_shortlist(self.get_shortlist(n))

    def _cmd_help(self) -> str:
        return """Commands:
  detail #N          — Full details for listing #N
  compare #N #M      — Compare listings side by side
  fav #N             — Favorite a listing
  reject #N          — Reject a listing (removes from results)
  note #N <text>     — Add a note to a listing
  search <location>  — Search a new location
  top [N]            — Show top N results (default 10)
  favorites          — Show your favorited listings
  prefs              — Show your preferences
  remove pref <id>   — Remove a preference
  history #N         — Show price history
  refresh            — Re-score all listings
  save search <name> — Save current search config
  load search <name> — Load a saved search
  delete search <name> — Delete a saved search
  searches           — List saved searches
  add location <place> [priority N] — Add a distance location (max 3)
  locations          — List distance locations
  remove location <id> — Remove a distance location
  assign district <name> to <zip> — Assign a zip to a school district
  districts          — List school districts
  rate district <name> <1-10> — Rate a district
  exclude district <name> — Exclude a district from results
  include district <name> — Re-include a district
  populate districts — Auto-populate districts from NCES for all listings
  fetch ratings      — Fetch GreatSchools ratings for unrated districts
  snapshot <name>    — Save current scoring state (prefs/locations/districts)
  snapshots          — List saved snapshots
  compare snapshot <name> — Compare current scores with a snapshot
  restore snapshot <name> — Restore prefs/locations/districts from a snapshot
  delete snapshot <name>  — Delete a snapshot
  help               — Show this help
  quit               — Exit

Or just type naturally — e.g., "I want a big yard" adds a preference."""

    def detail_listing_by_index(self, index: int) -> str:
        pid = self._resolve_index(index)
        if not pid:
            return f"No listing #{index} found."
        return self.detail_listing(pid, index)

    def detail_listing(self, property_id: str, display_index: Optional[int] = None) -> str:
        """Full formatted view of a listing."""
        listing = self.current_listings.get(property_id) or self.db.get_listing(property_id)
        if not listing:
            return "Listing not found."

        idx = display_index or "?"
        price = listing.get("price")
        price_str = f"${price:,.0f}" if price else "N/A"

        lines = [
            f"{'=' * 60}",
            f"Listing #{idx}: {listing.get('address', 'Unknown')}, {listing.get('city', '')}, {listing.get('state', '')} {listing.get('zip_code', '')}",
            f"{'=' * 60}",
            f"Price: {price_str}",
            f"Beds: {listing.get('beds', 'N/A')}  |  Baths: {listing.get('baths', 'N/A')}  |  Sqft: {listing.get('sqft', 'N/A')}",
            f"Lot: {listing.get('lot_sqft', 'N/A')} sqft  |  Year Built: {listing.get('year_built', 'N/A')}",
            f"Type: {listing.get('property_type', 'N/A')}  |  Status: {listing.get('status', 'N/A')}",
        ]

        hoa = listing.get("hoa_fee")
        if hoa and hoa > 0:
            lines.append(f"HOA: ${hoa:,.0f}/mo")
        else:
            lines.append("HOA: None")

        if listing.get("mls_id"):
            lines.append(f"MLS: {listing['mls_id']}")

        if listing.get("property_id", "").startswith("http"):
            lines.append(f"URL: {listing['property_id']}")

        # Distances
        dists = self.current_distances.get(property_id)
        if not dists and listing.get("latitude") and listing.get("longitude"):
            locations = self.db.get_locations()
            if locations:
                dists = compute_distances(listing["latitude"], listing["longitude"], locations)
        if dists:
            lines.append("\nDistances:")
            for d in sorted(dists, key=lambda x: x["priority"], reverse=True):
                lines.append(f"  {d['name']}: {d['distance_miles']} mi (priority {d['priority']})")

        # School district
        district = self.current_districts.get(property_id)
        if not district:
            district = self.db.get_district_for_listing(property_id)
        if district:
            rating_str = f", rated {district['rating']}/10" if district.get("rating") else ""
            status_str = " [EXCLUDED]" if district.get("excluded") else ""
            nces_parts = []
            if district.get("enrollment"):
                nces_parts.append(f"{district['enrollment']:,} students")
            if district.get("school_count"):
                nces_parts.append(f"{district['school_count']} schools")
            if district.get("student_teacher_ratio"):
                nces_parts.append(f"{district['student_teacher_ratio']:.0f}:1 student-teacher ratio")
            nces_str = f" ({', '.join(nces_parts)})" if nces_parts else ""
            lines.append(f"\nSchool District: {district['name']}{rating_str}{status_str}{nces_str}")

        # Score info
        score_info = next((r for r in self.current_results if r["property_id"] == property_id), None)
        if score_info:
            lines.append(f"\nScore: {score_info['score']}/10")
            if score_info.get("reasoning"):
                lines.append(f"Analysis: {score_info['reasoning']}")

        # Description
        desc = listing.get("description")
        if desc:
            lines.append(f"\nDescription:\n{desc}")

        # Notes
        notes = self.db.get_notes(property_id)
        if notes:
            lines.append("\nYour Notes:")
            for n in notes:
                lines.append(f"  - {n['note']} ({n['created_at'][:10]})")

        return "\n".join(lines)

    def _handle_natural_language(self, text: str) -> str:
        """Use LLM to classify intent and handle accordingly."""
        intent = self.llm.classify_intent(text)
        intent_type = intent.get("intent", "unknown")

        if intent_type == "preference":
            pref_text = intent.get("text", text)
            pref_id = self.db.add_preference(pref_text)
            result = f"Added preference [{pref_id}]: {pref_text}"
            if self.current_listings:
                result += "\nUse 'refresh' to re-score listings with this new preference."
            return result

        elif intent_type == "question":
            prefs = [p["text"] for p in self.db.get_active_preferences()]
            context = self.get_shortlist(10)
            system = build_chat_system_prompt(prefs, context)
            return self.llm.chat(system, text)

        elif intent_type == "search":
            location = intent.get("location", text)
            return self._cmd_search(location)

        else:
            # Try as a question anyway
            prefs = [p["text"] for p in self.db.get_active_preferences()]
            context = self.get_shortlist(10)
            system = build_chat_system_prompt(prefs, context)
            return self.llm.chat(system, text)

    def _format_shortlist(self, shortlist: list[dict]) -> str:
        """Format the shortlist for display."""
        if not shortlist:
            return "No results to show."

        # Get top-priority location for distance display
        locations = self.db.get_locations()
        top_loc = None
        if locations:
            top_loc = max(locations, key=lambda x: x["priority"])

        lines = [f"\nTop {len(shortlist)} listings:\n"]
        for item in shortlist:
            score = item.get("score", 0)
            price = item.get("price")
            price_str = f"${price:,.0f}" if price else "N/A"
            beds = item.get("beds")
            beds_str = f"{beds:.0f}" if beds else "?"
            baths = item.get("baths")
            baths_str = f"{baths:.0f}" if baths else "?"
            sqft = item.get("sqft")
            sqft_str = f"{sqft:,.0f}" if sqft else "?"
            addr = item.get("address", "")
            city = item.get("city", "")
            reasoning = item.get("reasoning", "")
            hoa = item.get("hoa_fee")
            hoa_tag = f"  HOA${hoa:,.0f}" if hoa and hoa > 0 else ""

            # Distance to top-priority location
            dist_tag = ""
            if top_loc:
                dists = self.current_distances.get(item["property_id"])
                if dists:
                    for d in dists:
                        if d["name"] == top_loc["name"]:
                            dist_tag = f"  {d['distance_miles']}mi"
                            break

            # Score bar
            filled = int(score)
            bar = "\u2588" * filled + "\u2591" * (10 - filled)

            lines.append(f"  #{item['index']:>2}  [{bar}] {score:4.1f}  {price_str:>12}  {beds_str}bd/{baths_str}ba  {sqft_str:>6} sqft  {addr}, {city}{hoa_tag}{dist_tag}")
            if reasoning:
                lines.append(f"       {reasoning[:100]}")
            lines.append("")

        lines.append("Type 'detail #N' for more info, 'fav #N' to favorite, or 'help' for all commands.")
        return "\n".join(lines)
