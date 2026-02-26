# House Hunter

LLM-powered real estate agent that scrapes listings, scores them against your preferences, and provides an interactive CLI and web UI for exploring results.

## Setup

```bash
uv sync
```

### LLM Configuration

**Local (llama-server / llama.cpp):**
```bash
export HOUSE_HUNTER_API_BASE=http://localhost:8081/v1
```

**Cloud (OpenAI, Anthropic, etc.):**
```bash
export HOUSE_HUNTER_MODEL=claude-sonnet-4-20250514
export ANTHROPIC_API_KEY=sk-...
```

Uses [litellm](https://github.com/BerriAI/litellm) under the hood, so any supported provider works. See `HOUSE_HUNTER_*` env vars in `config.py` for all options.

## Usage

### CLI

```bash
uv run python -m house_hunter.main "Bucks County, PA" --price-max 500000 --beds-min 3
```

Options:
- `--price-min` / `--price-max` — price range
- `--beds-min` / `--beds-max` — bedroom count
- `--baths-min` / `--baths-max` — bathroom count
- `--sqft-min` / `--sqft-max` — square footage
- `--lot-min` / `--lot-max` — lot size in acres
- `--year-min` / `--year-max` — year built
- `--past-days N` — only listings from last N days
- `--type single_family condo` — property types
- `--no-hoa` — exclude listings with HOA fees
- `--limit N` — max listings to fetch (default 500)

### Web UI

```bash
uv run python -m house_hunter.main --web
```

Launches at `http://localhost:8181` with:
- **Listings** — browse, search, sort by score/price/date, rescore
- **Favorites** — your saved favorites
- **Preferences** — manage scoring preferences
- **Locations** — distance priority locations (max 3)
- **Districts** — school district management with auto-population and ratings
- **Snapshots** — save/compare/restore scoring configurations
- **Agent** — chat interface with full CLI command support

### Interactive Commands

Once running (CLI or Agent chat), you can:

```
detail #N              Show full details for listing #N
compare #N #M          Compare listings side by side (LLM-powered)
fav #N                 Favorite a listing
reject #N              Reject and remove from results
note #N <text>         Add a note to a listing
search <location>      Search a new location
top [N]                Show top N results (default 10)
favorites              Show favorited listings
prefs                  Show active preferences
remove pref <id>       Remove a preference
history #N             Price history for a listing
refresh                Re-score all listings
```

**Saved Searches:**
```
save search <name>     Save current search config
load search <name>     Load and run a saved search
delete search <name>   Delete a saved search
searches               List all saved searches
```

**Distance Locations:**
```
add location <place> [priority N]   Add a distance location (max 3)
locations                           List distance locations
remove location <id>                Remove a location
```

**School Districts:**
```
assign district <name> to <zip>   Manual zip-to-district mapping
districts                         List all districts with stats
rate district <name> <1-10>       Rate a district
exclude district <name>           Exclude from results
include district <name>           Re-include
populate districts                Auto-detect districts via NCES API
fetch ratings                     Fetch GreatSchools ratings for unrated districts
```

**Score Snapshots:**
```
snapshot <name>              Save current scoring state
snapshots                    List all snapshots
compare snapshot <name>      Compare current scores vs snapshot
restore snapshot <name>      Restore prefs/locations/districts from snapshot
delete snapshot <name>       Delete a snapshot
```

Or just type naturally — e.g., "I want a big yard" adds a preference, "what's the best school district?" asks the LLM.

## How Scoring Works

1. Listings are scraped via [homeharvest](https://github.com/Bunsly/HomeHarvest)
2. Each listing is scored 0-10 by the LLM against your active preferences
3. Scores factor in: preferences, distance to priority locations, school district ratings, favorite/rejection patterns
4. Scores are cached by a hash of preferences + locations + districts — changing any of these invalidates the cache
5. Snapshots let you save a scoring config, tweak it, and compare results

## School District Auto-Detection

Districts are automatically populated from the [NCES ArcGIS API](https://nces.ed.gov/opengis/) using listing lat/lon coordinates. This provides:
- District name and NCES ID
- Enrollment, school count, student-teacher ratio

Ratings can be fetched from GreatSchools via the `fetch ratings` command or web UI button.

## Architecture

```
house_hunter/
  main.py          — CLI entry point + arg parsing
  config.py        — SearchConfig, LLMConfig, AppConfig dataclasses
  scraper.py       — homeharvest wrapper
  db.py            — SQLite persistence (listings, scores, prefs, districts, snapshots)
  llm.py           — litellm wrapper for scoring, comparison, intent classification
  prompts.py       — LLM prompt templates
  agent.py         — command parsing + orchestration
  cli.py           — interactive REPL
  distance.py      — haversine distance + geocoding
  schools.py       — NCES API client + GreatSchools scraper
  web/
    __init__.py    — Flask app factory
    routes_browse.py — listings, favorites, prefs, locations, districts, snapshots
    routes_agent.py  — chat, search, refresh
    filters.py     — Jinja template filters
    templates/     — HTML templates (Pico CSS)
    static/        — CSS
```
