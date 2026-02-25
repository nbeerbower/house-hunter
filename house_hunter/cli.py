import readline
import sys

from house_hunter.agent import Agent


class CLI:
    def __init__(self, agent: Agent):
        self.agent = agent

    def run(self):
        """Main interactive loop."""
        # Initial search
        new_ids, changed_ids = self.agent.run_search()
        self._print_price_changes(new_ids, changed_ids)

        # Show initial shortlist
        shortlist = self.agent.get_shortlist()
        if shortlist:
            print(self.agent._format_shortlist(shortlist))

        # Check if preferences exist
        prefs = self.agent.db.get_active_preferences()
        if not prefs:
            print("\nNo preferences set yet. Tell me what you're looking for!")
            print("Examples: 'I want at least 3 bedrooms', 'prefer a large yard', 'budget under 400k'\n")

        # Input loop
        while True:
            try:
                user_input = input("\nhouse-hunter> ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\nGoodbye!")
                break

            if not user_input:
                continue

            if user_input.lower() in ("quit", "exit", "q"):
                print("Goodbye!")
                break

            response = self.agent.handle_feedback(user_input)
            if response:
                print(f"\n{response}")

    def _print_price_changes(self, new_ids: list[str], changed_ids: list[str]):
        """Print summary of new listings and price changes."""
        if not new_ids and not changed_ids:
            return

        if new_ids:
            print(f"\n  New listings: {len(new_ids)}")

        if changed_ids:
            print(f"\n  Price changes detected ({len(changed_ids)}):")
            for pid in changed_ids[:5]:
                history = self.agent.db.get_price_history(pid)
                listing = self.agent.db.get_listing(pid)
                addr = listing.get("address", pid) if listing else pid
                if len(history) >= 2:
                    old_price = history[-2]["price"]
                    new_price = history[-1]["price"]
                    diff = new_price - old_price
                    direction = "↓" if diff < 0 else "↑"
                    print(f"    {addr}: ${old_price:,.0f} → ${new_price:,.0f} ({direction}${abs(diff):,.0f})")
            if len(changed_ids) > 5:
                print(f"    ... and {len(changed_ids) - 5} more")
