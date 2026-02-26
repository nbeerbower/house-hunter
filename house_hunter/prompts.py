def format_listing_for_prompt(listing: dict, index: int) -> str:
    """Format a single listing as a compact one-liner for LLM prompts."""
    price = listing.get("price")
    price_str = f"${price:,.0f}" if price else "Price N/A"

    beds = listing.get("beds")
    beds_str = f"{beds:.0f}bd" if beds else "?bd"

    baths = listing.get("baths")
    baths_str = f"{baths:.0f}ba" if baths else "?ba"

    sqft = listing.get("sqft")
    sqft_str = f"{sqft:,.0f} sqft" if sqft else "? sqft"

    lot = listing.get("lot_sqft")
    if lot and lot >= 43560:
        lot_str = f"{lot / 43560:.2f} acres"
    elif lot:
        lot_str = f"{lot:,.0f} sqft lot"
    else:
        lot_str = "? lot"

    year = listing.get("year_built")
    year_str = f"Built {year:.0f}" if year else "Year N/A"

    hoa = listing.get("hoa_fee")
    if hoa and hoa > 0:
        hoa_str = f"HOA ${hoa:,.0f}/mo"
    else:
        hoa_str = "No HOA"

    address = listing.get("address", "")
    city = listing.get("city", "")
    state = listing.get("state", "")
    location = ", ".join(p for p in [address, city, state] if p)

    desc = listing.get("description") or ""
    if len(desc) > 200:
        desc = desc[:200] + "..."

    lines = [
        f"[#{index}] {price_str} | {beds_str}/{baths_str} | {sqft_str} | {lot_str} | {year_str} | {hoa_str}",
        f"  {location}",
    ]
    if desc:
        lines.append(f"  {desc}")
    return "\n".join(lines)


def format_listing_batch(listings: list[dict], start_index: int = 1) -> str:
    """Format a batch of listings for an LLM prompt."""
    parts = []
    for i, listing in enumerate(listings):
        parts.append(format_listing_for_prompt(listing, start_index + i))
    return "\n\n".join(parts)


def build_scoring_system_prompt(
    preferences: list[str],
    favorites: list[dict],
    rejections_summary: str,
) -> str:
    """Build the system prompt for scoring listings."""
    pref_bullets = "\n".join(f"- {p}" for p in preferences) if preferences else "- No specific preferences set yet"

    fav_section = ""
    if favorites:
        fav_lines = []
        for f in favorites[:5]:
            price = f.get("price")
            price_str = f"${price:,.0f}" if price else "N/A"
            fav_lines.append(f"  - {price_str} | {f.get('beds', '?')}bd/{f.get('baths', '?')}ba | {f.get('address', 'Unknown')}")
        fav_section = "\n\nProperties the buyer has FAVORITED (use as positive signal):\n" + "\n".join(fav_lines)

    rej_section = ""
    if rejections_summary:
        rej_section = f"\n\nProperties the buyer has REJECTED (use as negative signal):\n  {rejections_summary}"

    return f"""You are a real estate analyst helping a home buyer evaluate listings.

Buyer's preferences:
{pref_bullets}
{fav_section}{rej_section}

Score each listing from 0 to 10 based on how well it matches the buyer's preferences.
- 9-10: Excellent match, should definitely visit
- 7-8: Strong match, worth serious consideration
- 5-6: Moderate match, some pros and cons
- 3-4: Weak match, significant drawbacks
- 0-2: Poor match, does not fit preferences

Respond with ONLY a JSON array. Each element must have:
- "index": the listing number (e.g. 3 for [#3])
- "score": number from 0 to 10 (one decimal allowed)
- "pros": array of short strings (key positives)
- "cons": array of short strings (key negatives)
- "summary": one-sentence summary of the match

Example:
[{{"index": 1, "score": 7.5, "pros": ["large yard", "updated kitchen"], "cons": ["older roof"], "summary": "Solid match with good outdoor space but may need roof work."}}]"""


def build_scoring_user_prompt(listings_text: str) -> str:
    """Build the user prompt for a scoring batch."""
    return f"Score the following listings:\n\n{listings_text}"


def build_chat_system_prompt(preferences: list[str], context_listings: list[dict]) -> str:
    """Build system prompt for conversational Q&A about properties."""
    pref_text = "\n".join(f"- {p}" for p in preferences) if preferences else "No preferences set."

    listing_text = ""
    if context_listings:
        parts = []
        for i, l in enumerate(context_listings[:10], 1):
            price = l.get("price")
            price_str = f"${price:,.0f}" if price else "N/A"
            parts.append(f"#{i}: {price_str} | {l.get('beds', '?')}bd/{l.get('baths', '?')}ba | {l.get('sqft', '?')} sqft | {l.get('address', '')}, {l.get('city', '')}")
        listing_text = "\n\nCurrent shortlist:\n" + "\n".join(parts)

    return f"""You are a helpful real estate assistant. Answer questions about properties and help the buyer make decisions.

Buyer's preferences:
{pref_text}
{listing_text}

Be concise and practical. If asked about a specific property, reference it by number."""


def build_comparison_prompt(listings: list[dict]) -> str:
    """Build a prompt to compare specific listings."""
    parts = []
    for i, l in enumerate(listings, 1):
        parts.append(format_listing_for_prompt(l, i))
    listings_text = "\n\n".join(parts)

    return f"""Compare these properties side by side. For each, note:
- Key strengths
- Key weaknesses
- Best suited for what type of buyer

Then give your recommendation for which is the better value.

{listings_text}"""
