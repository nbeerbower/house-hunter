import json
import re
import time
from typing import Optional

import requests


NCES_API_URL = (
    "https://nces.ed.gov/opengis/rest/services/School_District_Boundaries/"
    "EDGE_ADMINDATA_SCHOOLDISTRICTS_SY2223/MapServer/1/query"
)

GREATSCHOOLS_SEARCH_URL = "https://www.greatschools.org/search/search.page"


def lookup_district_nces(lat: float, lon: float) -> Optional[dict]:
    """Look up school district from NCES ArcGIS API by lat/lon."""
    params = {
        "geometry": f"{lon},{lat}",
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "LEAID,LEA_NAME,LSTATE,LZIP,SCH,MEMBER,STUTERATIO",
        "returnGeometry": "false",
        "f": "json",
    }
    try:
        resp = requests.get(NCES_API_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError):
        return None

    features = data.get("features", [])
    if not features:
        return None

    attrs = features[0].get("attributes", {})
    if not attrs.get("LEA_NAME"):
        return None

    return {
        "nces_id": attrs.get("LEAID"),
        "name": attrs.get("LEA_NAME"),
        "state": attrs.get("LSTATE"),
        "school_count": attrs.get("SCH"),
        "enrollment": attrs.get("MEMBER"),
        "student_teacher_ratio": attrs.get("STUTERATIO"),
    }


def _gs_extract_schools(html: str) -> list[dict]:
    """Extract school data from gon.search JSON embedded in GreatSchools HTML."""
    m = re.search(r'gon\.search=({.*?});gon\.', html)
    if not m:
        return []
    try:
        data = json.loads(m.group(1))
        return data.get("schools", [])
    except (json.JSONDecodeError, KeyError):
        return []


def _gs_fetch_page(params: dict) -> list[dict]:
    """Fetch a single GreatSchools search page and extract schools."""
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        resp = requests.get(GREATSCHOOLS_SEARCH_URL, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        return _gs_extract_schools(resp.text)
    except requests.RequestException:
        return []


def fetch_greatschools_rating(district_name: str, state: str) -> Optional[float]:
    """Fetch GreatSchools rating for a district by finding its districtId and browsing all schools."""
    state_lower = state.lower()

    # Step 1: Search by name to find a school with a districtId
    schools = _gs_fetch_page({"q": district_name, "state": state_lower})
    if not schools:
        return None

    district_id = None
    for s in schools:
        if s.get("districtId") and s.get("districtName") and district_name.lower() in s["districtName"].lower():
            district_id = s["districtId"]
            break

    # Step 2: If we found a districtId, browse all schools in that district (paginated)
    if district_id:
        all_schools = []
        for page in range(1, 6):  # max 5 pages
            page_schools = _gs_fetch_page({"districtId": district_id, "state": state_lower, "page": page})
            if not page_schools:
                break
            all_schools.extend(page_schools)
            if len(page_schools) < 15:  # less than a full page means last page
                break
            time.sleep(1)
    else:
        # Fallback: use the keyword search results, filter to matching district
        all_schools = [s for s in schools if s.get("districtName") and district_name.lower() in s["districtName"].lower()]
        if not all_schools:
            all_schools = schools  # last resort: use all search results

    # Step 3: Extract ratings
    ratings = [s["rating"] for s in all_schools if s.get("rating") and isinstance(s["rating"], (int, float)) and 1 <= s["rating"] <= 10]
    if not ratings:
        return None

    return round(sum(ratings) / len(ratings), 1)


def populate_districts(db, listings: list[dict], quiet: bool = False) -> str:
    """Batch populate districts from NCES for listings with lat/lon but no district_id."""
    unmapped = [l for l in listings if l.get("latitude") and l.get("longitude") and not l.get("district_id")]
    if not unmapped:
        return "All listings already have districts assigned."

    if not quiet:
        print(f"Looking up districts for {len(unmapped)} listings via NCES API...")

    mapped = 0
    failed = 0
    for i, listing in enumerate(unmapped):
        nces_data = lookup_district_nces(listing["latitude"], listing["longitude"])
        if nces_data:
            district_id = db.upsert_district_from_nces(nces_data)
            db.set_listing_district(listing["property_id"], district_id)
            mapped += 1
            if not quiet:
                print(f"  [{i+1}/{len(unmapped)}] {listing.get('address', '?')} -> {nces_data['name']}")
        else:
            failed += 1
            if not quiet:
                print(f"  [{i+1}/{len(unmapped)}] {listing.get('address', '?')} -> no district found")

        # Politeness delay between API calls
        if i < len(unmapped) - 1:
            time.sleep(1)

    return f"District population complete: {mapped} mapped, {failed} failed out of {len(unmapped)} listings."


def fetch_all_ratings(db) -> str:
    """Fetch GreatSchools ratings for all districts that don't have one."""
    districts = db.get_all_districts()
    unrated = [d for d in districts if not d.get("rating")]
    if not unrated:
        return "All districts already have ratings."

    print(f"Fetching GreatSchools ratings for {len(unrated)} districts...")

    fetched = 0
    failed = 0
    for i, d in enumerate(unrated):
        state = d.get("state", "")
        if not state:
            failed += 1
            continue

        rating = fetch_greatschools_rating(d["name"], state)
        if rating is not None:
            # Round to nearest integer for the 1-10 scale
            int_rating = max(1, min(10, round(rating)))
            db.set_district_rating(d["id"], int_rating)
            fetched += 1
            print(f"  [{i+1}/{len(unrated)}] {d['name']} -> {int_rating}/10 (avg {rating})")
        else:
            failed += 1
            print(f"  [{i+1}/{len(unrated)}] {d['name']} -> no rating found")

        # Politeness delay
        if i < len(unrated) - 1:
            time.sleep(2)

    return f"Rating fetch complete: {fetched} rated, {failed} failed out of {len(unrated)} districts."
