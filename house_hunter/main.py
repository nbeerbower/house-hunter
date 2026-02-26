import argparse

from house_hunter.agent import Agent
from house_hunter.cli import CLI
from house_hunter.config import AppConfig, LLMConfig, SearchConfig


def main():
    parser = argparse.ArgumentParser(description="House Hunter — LLM-powered real estate agent")
    parser.add_argument("location", nargs="?", default="San Francisco, CA", help="Search location")
    parser.add_argument("--price-min", type=int, help="Minimum price")
    parser.add_argument("--price-max", type=int, help="Maximum price")
    parser.add_argument("--beds-min", type=int, help="Minimum bedrooms")
    parser.add_argument("--beds-max", type=int, help="Maximum bedrooms")
    parser.add_argument("--baths-min", type=float, help="Minimum bathrooms")
    parser.add_argument("--baths-max", type=float, help="Maximum bathrooms")
    parser.add_argument("--sqft-min", type=int, help="Minimum square footage")
    parser.add_argument("--sqft-max", type=int, help="Maximum square footage")
    parser.add_argument("--lot-min", type=float, help="Minimum lot size in acres")
    parser.add_argument("--lot-max", type=float, help="Maximum lot size in acres")
    parser.add_argument("--year-min", type=int, help="Minimum year built")
    parser.add_argument("--year-max", type=int, help="Maximum year built")
    parser.add_argument("--past-days", type=int, help="Only listings from last N days")
    parser.add_argument("--limit", type=int, default=500, help="Max listings to fetch (default: 500)")
    parser.add_argument("--type", dest="property_type", nargs="+",
                        help="Property types (e.g. single_family multi_family condo)")
    parser.add_argument("--no-hoa", action="store_true", help="Exclude listings with HOA fees")
    args = parser.parse_args()

    # Convert lot acres to sqft for the API
    lot_sqft_min = int(args.lot_min * 43560) if args.lot_min else None
    lot_sqft_max = int(args.lot_max * 43560) if args.lot_max else None

    search_config = SearchConfig(
        location=args.location,
        listing_type="for_sale",
        property_type=args.property_type,
        price_min=args.price_min,
        price_max=args.price_max,
        beds_min=args.beds_min,
        beds_max=args.beds_max,
        baths_min=args.baths_min,
        baths_max=args.baths_max,
        sqft_min=args.sqft_min,
        sqft_max=args.sqft_max,
        lot_sqft_min=lot_sqft_min,
        lot_sqft_max=lot_sqft_max,
        year_built_min=args.year_min,
        year_built_max=args.year_max,
        past_days=args.past_days,
        limit=args.limit,
        extra_property_data=True,
    )

    llm_config = LLMConfig.from_env()
    app_config = AppConfig(search=search_config, llm=llm_config, exclude_hoa=args.no_hoa)
    agent = Agent(app_config)
    cli = CLI(agent)
    cli.run()


if __name__ == "__main__":
    main()
