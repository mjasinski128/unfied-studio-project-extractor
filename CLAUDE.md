# CLAUDE.md – unified-studio-extractor

## Purpose

Extracts all resources from an Amazon DataZone / Unified Studio domain into a single JSON file. Primary goals:

1. **Navigation** – produce one JSON file the user can open in an IDE and explore all objects across all projects without knowing which project owns what.
2. **Debugging** – surface resource IDs and relationships to diagnose "ID not found" errors in the console.
3. **Backup** – capture all user-generated content: metadata forms, glossaries, asset types, assets, data products, subscriptions.

## Tech stack

- Python, managed with **Poetry** (`pyproject.toml` / `poetry.lock`)
- `boto3` DataZone client (`client = boto3.Session(...).client("datazone")`)
- Run everything via `poetry run python main.py ...`

## Code conventions

### Function naming

Every resource type has three explicit, clearly named functions — no generic dispatch:

| Pattern | Purpose |
|---|---|
| `list_*` | Paginated list call, returns raw summaries from boto3 |
| `get_*` | Detail call for a single resource by ID |
| `get_*s` | Orchestrates list + detail, returns enriched list for a whole project |

This is intentional. Keep it explicit so the user can read, modify, or call any layer independently.

### Error handling

All boto3 calls go through `safe_call()`. On any error it logs a warning and returns `{"_error": "..."}` inline so the extract keeps running. Never raise or abort on a single resource failure.

### Pagination

Use `all_pages(fn, result_key, **kwargs)` for straightforward paginated calls. For calls that also need error handling (e.g. `search_types`), inline the pagination loop with a `safe_call` guard.

## Output hierarchy

Subscriptions exist at **two levels** — this is intentional and must be preserved:

### Per-object (asset / data product)
If an asset has been published as a listing, it carries its own subscription context directly:
```
asset.listing_id
asset.subscriptions
asset.subscription_requests
asset.subscription_grants
```
Data products are always listings, so they always carry this context.

### Per-project (two views)
```
project.subscriptions_as_provider    – project owns the listed item
project.subscriptions_as_subscriber  – project is consuming someone else's listing
```

The helper `get_subscription_context(client, domain_id, listing_id)` returns all three subscription objects for a given listing. Use it when attaching subscriptions to any new object type.

## Adding new resource types

1. Add `list_<resource>(client, domain_id, ...)` using `all_pages()`
2. Add `get_<resource>(client, domain_id, resource_id)` using `safe_call()`
3. Add `get_<resource>s(client, domain_id, project_id)` to orchestrate
4. Call it from `extract_project()`

If the new resource is a listing (i.e. subscribable), call `get_subscription_context()` on it and merge the result in.
