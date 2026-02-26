# House Hunter — Claude Code Guidelines

## Project Overview

LLM-powered real estate agent with CLI + Flask web UI. Scrapes listings via homeharvest, scores them against user preferences using a local or cloud LLM, and provides interactive exploration tools.

## Running

```bash
# CLI
HOUSE_HUNTER_API_BASE=http://localhost:8081/v1 uv run python -m house_hunter.main "Location"

# Web UI
HOUSE_HUNTER_API_BASE=http://localhost:8081/v1 uv run python -m house_hunter.main --web
```

Always use `uv run` — dependencies are managed by uv, not system Python.

## Architecture

- **Entry**: `main.py` parses args, builds `AppConfig`, launches CLI or Flask
- **Agent** (`agent.py`): Central orchestrator. Regex-first command parsing with LLM fallback for natural language. Holds `current_listings`, `current_results`, `index_map` in memory
- **DB** (`db.py`): SQLite with WAL mode, `check_same_thread=False` for Flask compatibility. All tables created in `initialize()`, migrations in `_migrate()`
- **LLM** (`llm.py`): Wraps litellm. Handles batched scoring, comparison, intent classification. Local servers get `openai/` prefix and dummy API key automatically
- **Web** (`web/`): Flask app factory in `__init__.py`. Browse routes (`routes_browse.py`) and agent chat routes (`routes_agent.py`). Uses Pico CSS

## Key Patterns

- **Score caching**: MD5 hash of `(preferences + locations + districts)`. Changing any invalidates all cached scores. Hash computed in `db.get_preferences_hash()`
- **Score snapshots**: Save current prefs/locations/districts state, restore later. Old scores persist in DB keyed by their preferences_hash
- **District mapping**: Two paths — direct `listings.district_id` FK (set by NCES API auto-population) or `zip_district_map` table (manual fallback). `get_district_for_listing()` tries FK first
- **Listing IDs**: `property_id` is the property URL from homeharvest (a full URL string, not a numeric ID)
- **Index map**: Display indices (`#1`, `#2`, ...) map to property_ids via `agent.index_map`. Always use `_resolve_index()` to convert

## Adding Agent Commands

1. Add regex pattern in `handle_feedback()` (before the LLM fallback section)
2. Implement `_cmd_<name>()` method returning a string
3. Add to `_cmd_help()` output
4. For web: add route in `routes_browse.py`, template if needed

## Database Changes

- Add new tables in `initialize()`
- Add new columns in `_migrate()` using `ALTER TABLE ... ADD COLUMN` with existence checks
- Always `self.conn.commit()` after writes

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `HOUSE_HUNTER_API_BASE` | Local LLM server URL (e.g. `http://localhost:8081/v1`) |
| `HOUSE_HUNTER_MODEL` | Model name (default: `local` if api_base set, else `gpt-4o-mini`) |
| `HOUSE_HUNTER_API_KEY` | API key override |
| `HOUSE_HUNTER_TEMPERATURE` | LLM temperature (default: 0.3) |
| `HOUSE_HUNTER_MAX_TOKENS` | Max tokens (default: 4096) |
| `HOUSE_HUNTER_BATCH_SIZE` | Listings per scoring batch (default: 20) |
| `HOUSE_HUNTER_DEBUG` | Set to `1` to log all LLM prompts/responses to stderr |

## External APIs (no keys needed)

- **NCES ArcGIS** (`schools.py`): Free school district lookup by lat/lon. 1s delay between requests
- **GreatSchools** (`schools.py`): Scrapes search page for school ratings. Parses `gon.search` JSON from HTML. 2s delay between requests
- **Nominatim** (`distance.py`): Free geocoding via geopy. Results cached in `geocode_cache` table
