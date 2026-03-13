# Unified Studio Extractor

Extracts all resources from an Amazon DataZone / Unified Studio domain into a single JSON file for offline analysis, navigation in an IDE, and backup purposes.

## What it extracts

Per project:

- **Data sources** – with run history
- **Asset types** – with revision details
- **Assets** – with filters and revision history
- **Data products** – published listings
- **Form types** – metadata form definitions
- **Glossaries** – with all terms
- **Subscriptions** – with request details
- **Subscription grants**
- **Environments**

## Setup

```bash
poetry install
```

Requires AWS credentials configured (profile or environment variables).

## Usage

```bash
# Find your domain ID
poetry run python main.py --list-domains --region eu-west-1

# Full domain extract → extract.json
poetry run python main.py --domain-id dzd_xxxx --region eu-west-1

# Single project only
poetry run python main.py --domain-id dzd_xxxx --project-id prj_xxxx

# Custom output file or stdout
poetry run python main.py --domain-id dzd_xxxx --output my_backup.json
poetry run python main.py --domain-id dzd_xxxx --output -

# Use a named AWS profile
poetry run python main.py --domain-id dzd_xxxx --profile my-profile
```

## Output shape

```json
{
  "domain": { ... },
  "projects": [
    {
      "id": "prj_xxxx",
      "name": "...",
      "data_sources": [ { "id": "...", "runs": [ ... ] } ],
      "asset_types": [ ... ],
      "assets": [ { "id": "...", "filters": [ ... ], "revisions": [ ... ] } ],
      "data_products": [ ... ],
      "form_types": [ ... ],
      "glossaries": [ { "id": "...", "terms": [ ... ] } ],
      "subscriptions": [ ... ],
      "subscription_requests": [ ... ],
      "subscription_grants": [ ... ],
      "environments": [ ... ]
    }
  ]
}
```

Errors on individual resources do not abort the extract — they appear inline as `{"_error": "..."}` so you can see exactly which IDs or permissions are missing.

## Code structure

Each resource type has three explicit functions in `main.py`:

| Function | Purpose |
|---|---|
| `list_*` | Paginated list call, returns raw summaries |
| `get_*` | Detail call for a single resource by ID |
| `get_*s` | Orchestrates list + detail for a whole project |

Call any of these directly from a REPL or script when you only need one resource type.
