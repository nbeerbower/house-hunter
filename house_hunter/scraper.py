import pandas as pd
from homeharvest import scrape_property

from house_hunter.config import SearchConfig


def search_properties(config: SearchConfig) -> pd.DataFrame:
    """Run a property search using HomeHarvest and return results as a DataFrame."""
    try:
        df = scrape_property(
            location=config.location,
            listing_type=config.listing_type,
            property_type=config.property_type,
            price_min=config.price_min,
            price_max=config.price_max,
            beds_min=config.beds_min,
            beds_max=config.beds_max,
            baths_min=config.baths_min,
            baths_max=config.baths_max,
            sqft_min=config.sqft_min,
            sqft_max=config.sqft_max,
            lot_sqft_min=config.lot_sqft_min,
            lot_sqft_max=config.lot_sqft_max,
            year_built_min=config.year_built_min,
            year_built_max=config.year_built_max,
            past_days=config.past_days,
            foreclosure=config.foreclosure,
            exclude_pending=config.exclude_pending,
            mls_only=config.mls_only,
            extra_property_data=config.extra_property_data,
            limit=config.limit,
        )
    except Exception as e:
        print(f"Error scraping properties: {e}")
        return pd.DataFrame()

    return df
