import re
from typing import Optional

from house_hunter.config import AppConfig, SearchConfig
from house_hunter.db import Database
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

        # Exclude rejected
        rejected = self.db.get_rejected_ids()
        listings = [l for l in listings if l["property_id"] not in rejected]
        if rejected:
            print(f"  Excluded {len(rejected)} rejected listings")

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

        if listing.get("mls_id"):
            lines.append(f"MLS: {listing['mls_id']}")

        if listing.get("property_id", "").startswith("http"):
            lines.append(f"URL: {listing['property_id']}")

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

            # Score bar
            filled = int(score)
            bar = "█" * filled + "░" * (10 - filled)

            lines.append(f"  #{item['index']:>2}  [{bar}] {score:4.1f}  {price_str:>12}  {beds_str}bd/{baths_str}ba  {sqft_str:>6} sqft  {addr}, {city}")
            if reasoning:
                lines.append(f"       {reasoning[:100]}")
            lines.append("")

        lines.append("Type 'detail #N' for more info, 'fav #N' to favorite, or 'help' for all commands.")
        return "\n".join(lines)
