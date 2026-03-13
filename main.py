#!/usr/bin/env python3
"""
Amazon DataZone / Unified Studio – full-domain extractor.

Produces a single JSON file containing every resource in the domain so you can
navigate it in an IDE and find any object regardless of which project owns it.

Usage:
    poetry run python main.py --domain-id dzd_xxxx --region eu-west-1
    poetry run python main.py --domain-id dzd_xxxx --project-id prj_xxxx   # single project
    poetry run python main.py --list-domains                                # find your domain ID
"""

import argparse
import json
import logging
import sys
from datetime import date, datetime

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.WARNING, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def json_default(obj):
    """Make datetime objects JSON-serialisable."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Not serialisable: {type(obj)}")


def safe_call(fn, *args, **kwargs):
    """
    Call a boto3 method and return its result.
    On error, return {"_error": "<message>"} so the rest of the extract continues.
    """
    try:
        result = fn(*args, **kwargs)
        result.pop("ResponseMetadata", None)
        log.debug("  %s → keys: %s", fn.__name__, list(result.keys()))
        return result
    except ClientError as e:
        msg = e.response["Error"]["Message"]
        log.warning("  SKIP  %s – %s", fn.__name__, msg)
        return {"_error": msg}
    except Exception as e:
        log.warning("  SKIP  %s – %s", fn.__name__, e)
        return {"_error": str(e)}


def all_pages(fn, result_key, **kwargs):
    """
    Collect every page of a paginated boto3 call into a flat list.
    Handles NextToken / nextToken transparently.
    """
    items = []
    while True:
        response = fn(**kwargs)
        response.pop("ResponseMetadata", None)
        if not items:  # log structure of first page only
            log.debug("  %s → top-level keys: %s  |  first item keys: %s",
                      fn.__name__,
                      list(response.keys()),
                      list(response.get(result_key, [{}])[0].keys()) if response.get(result_key) else "[]")
        items.extend(response.get(result_key, []))
        token = response.get("nextToken") or response.get("NextToken")
        if not token:
            break
        # DataZone consistently uses nextToken
        kwargs["nextToken"] = token
    return items


# ---------------------------------------------------------------------------
# Domain
# ---------------------------------------------------------------------------

def list_domains(client):
    log.info("Listing domains")
    return all_pages(client.list_domains, "items")


def get_domain(client, domain_id):
    log.info("Describing domain %s", domain_id)
    return safe_call(client.get_domain, identifier=domain_id)


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------

def list_projects(client, domain_id):
    log.info("Listing projects in domain %s", domain_id)
    return all_pages(client.list_projects, "items", domainIdentifier=domain_id)


def get_project(client, domain_id, project_id):
    return safe_call(client.get_project,
                     domainIdentifier=domain_id,
                     identifier=project_id)


# ---------------------------------------------------------------------------
# Data Sources
# ---------------------------------------------------------------------------

def list_data_sources(client, domain_id, project_id):
    return all_pages(client.list_data_sources, "items",
                     domainIdentifier=domain_id,
                     projectIdentifier=project_id)


def get_data_source(client, domain_id, data_source_id):
    return safe_call(client.get_data_source,
                     domainIdentifier=domain_id,
                     identifier=data_source_id)


def list_data_source_runs(client, domain_id, data_source_id):
    return all_pages(client.list_data_source_runs, "items",
                     domainIdentifier=domain_id,
                     dataSourceIdentifier=data_source_id)


def get_data_sources(client, domain_id, project_id):
    log.info("  [%s] data sources", project_id)
    sources = list_data_sources(client, domain_id, project_id)
    result = []
    for s in sources:
        ds_id = s.get("dataSourceId") or s.get("id")
        detail = get_data_source(client, domain_id, ds_id)
        runs = list_data_source_runs(client, domain_id, ds_id)
        detail["runs_number"] = len(runs)
        detail["runs_last"] = runs[0] if runs else None
        result.append(detail)
    return result


# ---------------------------------------------------------------------------
# Asset Types  (domain-wide; optionally filtered by owning project)
# ---------------------------------------------------------------------------

def search_asset_types(client, domain_id, project_id=None):
    """
    search_types with searchScope=ASSET_TYPE.
    Note: owningProjectIdentifier is not a valid parameter for search_types;
    results are domain-wide regardless of project_id.
    """
    kwargs = dict(domainIdentifier=domain_id, searchScope="ASSET_TYPE", managed=False)
    items = []
    while True:
        response = safe_call(client.search_types, **kwargs)
        if "_error" in response:
            break
        items.extend(response.get("items", []))
        token = response.get("nextToken")
        if not token:
            break
        kwargs["nextToken"] = token
    return items


def get_asset_type(client, domain_id, asset_type_id, revision):
    return safe_call(client.get_asset_type,
                     domainIdentifier=domain_id,
                     identifier=asset_type_id,
                     revision=revision)


def get_asset_types(client, domain_id, project_id=None):
    log.info("  [%s] asset types", project_id or "domain")
    hits = search_asset_types(client, domain_id, project_id)
    result = []
    for h in hits:
        item = h.get("assetTypeItem", h)
        # search_types is domain-wide; filter by owning project when scoping to a project
        if project_id and item.get("owningProjectId") != project_id:
            continue
        detail = get_asset_type(client, domain_id,
                                item["name"], item.get("revision", "1"))
        result.append(detail)
    return result


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------

def search_assets(client, domain_id, project_id):
    """Use the search API with searchScope=ASSET to find all assets in a project."""
    items = []
    kwargs = dict(domainIdentifier=domain_id,
                  searchScope="ASSET",
                  owningProjectIdentifier=project_id)
    while True:
        response = safe_call(client.search, **kwargs)
        if "_error" in response:
            break
        items.extend(response.get("items", []))
        token = response.get("nextToken")
        if not token:
            break
        kwargs["nextToken"] = token
    return items


def get_asset(client, domain_id, asset_id):
    return safe_call(client.get_asset,
                     domainIdentifier=domain_id,
                     identifier=asset_id)


def list_asset_filters(client, domain_id, asset_id):
    return all_pages(client.list_asset_filters, "items",
                     domainIdentifier=domain_id,
                     assetIdentifier=asset_id)


def list_asset_revisions(client, domain_id, asset_id):
    return all_pages(client.list_asset_revisions, "items",
                     domainIdentifier=domain_id,
                     identifier=asset_id)


def extract_listing_id(asset_detail):
    """
    Pull the listing ID out of a get_asset response, if the asset has been
    published to the catalog.  DataZone nests it in different shapes depending
    on version; we try the known locations in order.
    """
    for path in [
        lambda d: d.get("listing", {}).get("listingId"),
        lambda d: d.get("latestVersionDetails", {}).get("listing", {}).get("listingId"),
        lambda d: d.get("additionalAttributes", {}).get("latestTimeSeriesDataPointFormsOutput", {}).get("listingId"),
    ]:
        try:
            value = path(asset_detail)
            if value:
                return value
        except (AttributeError, TypeError):
            pass
    return None


def get_assets(client, domain_id, project_id):
    log.info("  [%s] assets", project_id)
    hits = search_assets(client, domain_id, project_id)
    result = []
    for h in hits:
        item = h.get("assetItem", {})
        asset_id = item.get("itemId") or item.get("identifier")
        if not asset_id:
            result.append(h)
            continue
        detail = get_asset(client, domain_id, asset_id)
        detail["filters"] = list_asset_filters(client, domain_id, asset_id)
        detail["revisions"] = list_asset_revisions(client, domain_id, asset_id)

        # If this asset has been published as a listing, attach its full
        # subscription context directly on the asset.
        listing_id = extract_listing_id(detail)
        if listing_id:
            log.info("    asset %s has listing %s – fetching subscriptions",
                     asset_id, listing_id)
            detail["listing_id"] = listing_id
            detail.update(get_subscription_context(client, domain_id, listing_id))

        result.append(detail)
    return result


# ---------------------------------------------------------------------------
# Data Products  (published as Listings)
# ---------------------------------------------------------------------------

def search_listings(client, domain_id):
    """
    search_listings is domain-wide; there is no project or type filter in the API.
    Returns all listing items (both assetListingItem and dataProductListingItem).
    """
    kwargs = dict(domainIdentifier=domain_id)
    items = []
    while True:
        response = safe_call(client.search_listings, **kwargs)
        if "_error" in response:
            break
        items.extend(response.get("items", []))
        token = response.get("nextToken")
        if not token:
            break
        kwargs["nextToken"] = token
    return items


def get_listing(client, domain_id, listing_id):
    return safe_call(client.get_listing,
                     domainIdentifier=domain_id,
                     identifier=listing_id)


def get_data_product(client, domain_id, data_product_id):
    return safe_call(client.get_data_product,
                     domainIdentifier=domain_id,
                     identifier=data_product_id)


def get_data_products(client, domain_id, project_id):
    log.info("  [%s] data products", project_id)
    all_listings = search_listings(client, domain_id)
    result = []
    for listing in all_listings:
        item = listing.get("listingItem", listing)

        # search_listings returns all listing types; keep only data products.
        dp = item.get("dataProductListingItem")
        if not dp:
            continue

        # Filter to this project only.
        owning_project = dp.get("owningProjectId")
        if owning_project and owning_project != project_id:
            continue

        listing_id = dp.get("listingId") or item.get("listingId") or item.get("id")
        if not listing_id:
            result.append(listing)
            continue
        detail = get_listing(client, domain_id, listing_id)

        # A data product is a listing – attach all subscription activity for it.
        log.info("    listing %s – fetching subscriptions", listing_id)
        detail.update(get_subscription_context(client, domain_id, listing_id))

        result.append(detail)
    return result


# ---------------------------------------------------------------------------
# Metadata Forms  (Form Types)
# ---------------------------------------------------------------------------

def search_form_types(client, domain_id, project_id=None):
    # Note: owningProjectIdentifier is not a valid parameter for search_types
    kwargs = dict(domainIdentifier=domain_id, searchScope="FORM_TYPE", managed=False)
    items = []
    while True:
        response = safe_call(client.search_types, **kwargs)
        if "_error" in response:
            break
        items.extend(response.get("items", []))
        token = response.get("nextToken")
        if not token:
            break
        kwargs["nextToken"] = token
    return items


def get_form_type(client, domain_id, form_type_id, revision=None):
    kwargs = dict(domainIdentifier=domain_id, formTypeIdentifier=form_type_id)
    if revision:
        kwargs["revision"] = revision
    return safe_call(client.get_form_type, **kwargs)


def get_form_types(client, domain_id, project_id=None):
    log.info("  [%s] form types (metadata forms)", project_id or "domain")
    hits = search_form_types(client, domain_id, project_id)
    result = []
    for h in hits:
        item = h.get("formTypeItem", h)
        # search_types is domain-wide; filter by owning project when scoping to a project
        if project_id and item.get("owningProjectId") != project_id:
            continue
        detail = get_form_type(client, domain_id,
                               item.get("name") or item.get("formTypeIdentifier"),
                               item.get("revision"))
        result.append(detail)
    return result


# ---------------------------------------------------------------------------
# Glossaries
# ---------------------------------------------------------------------------

def list_glossaries(client, domain_id, project_id=None):
    # ListGlossaries does not exist in this version of the DataZone API.
    # Only Get/Create/Delete/Update are available; glossaries cannot be enumerated.
    log.info("  list_glossaries – not supported by API, skipping")
    return []


def get_glossary(client, domain_id, glossary_id):
    return safe_call(client.get_glossary,
                     domainIdentifier=domain_id,
                     identifier=glossary_id)


def list_glossary_terms(client, domain_id, glossary_id):
    # ListGlossaryTerms does not exist in this version of the DataZone API.
    return []


def get_glossary_term(client, domain_id, term_id):
    return safe_call(client.get_glossary_term,
                     domainIdentifier=domain_id,
                     identifier=term_id)


def get_glossaries(client, domain_id, project_id=None):
    log.info("  [%s] glossaries", project_id or "domain")
    glossaries = list_glossaries(client, domain_id, project_id)
    result = []
    for g in glossaries:
        g_id = g.get("id") or g.get("glossaryId")
        detail = get_glossary(client, domain_id, g_id)
        raw_terms = list_glossary_terms(client, domain_id, g_id)
        detail["terms"] = [get_glossary_term(client, domain_id, t.get("id") or t.get("glossaryTermId"))
                           for t in raw_terms]
        result.append(detail)
    return result


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------

def list_subscriptions(client, domain_id, owning_project_id=None,
                       listing_id=None, subscriber_project_id=None):
    """
    List subscriptions with optional filters.

    owning_project_id      – project that owns (provides) the listed asset/product
    listing_id             – narrow to a specific listing (asset or data product)
    subscriber_project_id  – post-filter: keep only subscriptions where the
                             subscribedPrincipal is this project (API has no
                             native subscriber-project filter)
    """
    kwargs = dict(domainIdentifier=domain_id)
    if owning_project_id:
        kwargs["owningProjectId"] = owning_project_id
    if listing_id:
        kwargs["subscribedListingId"] = listing_id
    items = all_pages(client.list_subscriptions, "items", **kwargs)
    if subscriber_project_id:
        items = [s for s in items
                 if s.get("subscribedPrincipal", {}).get("project", {}).get("id")
                 == subscriber_project_id]
    return items


def get_subscription(client, domain_id, subscription_id):
    return safe_call(client.get_subscription,
                     domainIdentifier=domain_id,
                     identifier=subscription_id)


def list_subscription_requests(client, domain_id, owning_project_id=None,
                                listing_id=None, subscriber_project_id=None):
    # Note: no subscriber-project filter in this API; subscriberProjectId is invalid.
    kwargs = dict(domainIdentifier=domain_id)
    if owning_project_id:
        kwargs["owningProjectId"] = owning_project_id
    if listing_id:
        kwargs["subscribedListingId"] = listing_id
    items = all_pages(client.list_subscription_requests, "items", **kwargs)
    if subscriber_project_id:
        items = [r for r in items
                 if r.get("subscribedPrincipal", {}).get("project", {}).get("id")
                 == subscriber_project_id]
    return items


def get_subscription_request_details(client, domain_id, request_id):
    return safe_call(client.get_subscription_request_details,
                     domainIdentifier=domain_id,
                     identifier=request_id)


def get_subscriptions(client, domain_id, owning_project_id=None,
                      listing_id=None, subscriber_project_id=None):
    subs = list_subscriptions(client, domain_id,
                              owning_project_id=owning_project_id,
                              listing_id=listing_id,
                              subscriber_project_id=subscriber_project_id)
    result = []
    for s in subs:
        detail = get_subscription(client, domain_id, s["id"])
        result.append(detail)
    return result


def get_subscription_requests(client, domain_id, owning_project_id=None,
                               listing_id=None, subscriber_project_id=None):
    requests = list_subscription_requests(client, domain_id,
                                          owning_project_id=owning_project_id,
                                          listing_id=listing_id,
                                          subscriber_project_id=subscriber_project_id)
    result = []
    for r in requests:
        detail = get_subscription_request_details(client, domain_id, r["id"])
        result.append(detail)
    return result


# ---------------------------------------------------------------------------
# Subscription Grants
# ---------------------------------------------------------------------------

def list_subscription_grants(client, domain_id, owning_project_id=None,
                              listing_id=None):
    # API requires at least one of: subscribedListingId, environment, subscriptionId.
    # Without a listing_id we cannot do a project-wide scan; return empty.
    if not listing_id:
        return []
    kwargs = dict(domainIdentifier=domain_id, subscribedListingId=listing_id)
    if owning_project_id:
        kwargs["owningProjectId"] = owning_project_id
    return all_pages(client.list_subscription_grants, "items", **kwargs)


def get_subscription_grant(client, domain_id, grant_id):
    return safe_call(client.get_subscription_grant,
                     domainIdentifier=domain_id,
                     identifier=grant_id)


def get_subscription_grants(client, domain_id, owning_project_id=None,
                             listing_id=None):
    grants = list_subscription_grants(client, domain_id,
                                      owning_project_id=owning_project_id,
                                      listing_id=listing_id)
    result = []
    for g in grants:
        detail = get_subscription_grant(client, domain_id, g["id"])
        result.append(detail)
    return result


def get_subscription_context(client, domain_id, listing_id):
    """
    Return subscriptions, requests, and grants scoped to a single listing.
    Attached directly to the asset or data product that owns the listing.
    """
    return {
        "subscriptions": get_subscriptions(client, domain_id,
                                           listing_id=listing_id),
        "subscription_requests": get_subscription_requests(client, domain_id,
                                                           listing_id=listing_id),
        "subscription_grants": get_subscription_grants(client, domain_id,
                                                       listing_id=listing_id),
    }


# ---------------------------------------------------------------------------
# Environments  (included for context – help resolve environment IDs)
# ---------------------------------------------------------------------------

def list_environments(client, domain_id, project_id):
    return all_pages(client.list_environments, "environmentSummaries",
                     domainIdentifier=domain_id,
                     projectIdentifier=project_id)


def get_environment(client, domain_id, environment_id):
    return safe_call(client.get_environment,
                     domainIdentifier=domain_id,
                     identifier=environment_id)


def get_environments(client, domain_id, project_id):
    log.info("  [%s] environments", project_id)
    envs = list_environments(client, domain_id, project_id)
    return [get_environment(client, domain_id, e.get("id") or e.get("environmentId")) for e in envs]


# ---------------------------------------------------------------------------
# Single-project extract
# ---------------------------------------------------------------------------

def extract_project(client, domain_id, project_id):
    """
    Return a dict with all resources belonging to a single project.

    Subscriptions are captured at two levels:

    1. Per-object  – assets and data products carry their own subscriptions,
                     requests, and grants keyed by listing ID (see those sections).

    2. Per-project – two views so you can answer "what does this project expose?"
                     and "what has this project subscribed to?" independently:

       subscriptions_as_provider   – this project owns the listed item
       subscriptions_as_subscriber – this project is the consuming subscriber
    """
    log.info("Extracting project %s", project_id)
    project = get_project(client, domain_id, project_id)
    project["data_sources"] = get_data_sources(client, domain_id, project_id)
    project["asset_types"] = get_asset_types(client, domain_id, project_id)
    project["assets"] = get_assets(client, domain_id, project_id)
    project["data_products"] = get_data_products(client, domain_id, project_id)
    project["form_types"] = get_form_types(client, domain_id, project_id)
    project["glossaries"] = get_glossaries(client, domain_id, project_id)
    project["environments"] = get_environments(client, domain_id, project_id)

    # Project as provider: subscriptions to listings this project owns
    log.info("  [%s] subscriptions (as provider)", project_id)
    project["subscriptions_as_provider"] = {
        "subscriptions": get_subscriptions(client, domain_id,
                                           owning_project_id=project_id),
        "subscription_requests": get_subscription_requests(client, domain_id,
                                                           owning_project_id=project_id),
        "subscription_grants": get_subscription_grants(client, domain_id,
                                                       owning_project_id=project_id),
    }

    # Project as subscriber: filtered in Python by subscribedPrincipal.project.id
    # (API has no native subscriber-project filter)
    log.info("  [%s] subscriptions (as subscriber)", project_id)
    project["subscriptions_as_subscriber"] = {
        "subscriptions": get_subscriptions(client, domain_id,
                                           subscriber_project_id=project_id),
        "subscription_requests": get_subscription_requests(client, domain_id,
                                                           subscriber_project_id=project_id),
    }

    return project


# ---------------------------------------------------------------------------
# Full-domain extract
# ---------------------------------------------------------------------------

def extract_domain(client, domain_id):
    """Return a dict with the domain and every project fully described."""
    output = {}
    output["domain"] = get_domain(client, domain_id)

    projects = list_projects(client, domain_id)
    log.info("Found %d project(s)", len(projects))
    output["projects"] = [extract_project(client, domain_id, p["id"])
                          for p in projects]
    return output


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_client(region, profile):
    session = boto3.Session(profile_name=profile, region_name=region)
    return session.client("datazone")


def main():
    parser = argparse.ArgumentParser(
        description="Extract all DataZone / Unified Studio resources to JSON.")
    parser.add_argument("--domain-id", help="DataZone domain ID (dzd_…)")
    parser.add_argument("--project-id", help="Extract a single project only")
    parser.add_argument("--region", default="eu-west-1",
                        help="AWS region (default: eu-west-1)")
    parser.add_argument("--profile", default=None,
                        help="AWS credentials profile name")
    parser.add_argument("--output", default="extract.json",
                        help="Output file (default: extract.json)")
    parser.add_argument("--list-domains", action="store_true",
                        help="Print all visible domains and exit (useful to find your domain ID)")
    args = parser.parse_args()

    client = build_client(args.region, args.profile)

    if args.list_domains:
        domains = list_domains(client)
        print(json.dumps(domains, indent=2, default=json_default))
        return

    if not args.domain_id:
        parser.error("--domain-id is required (use --list-domains to find it)")

    if args.project_id:
        data = extract_project(client, args.domain_id, args.project_id)
    else:
        data = extract_domain(client, args.domain_id)

    output = json.dumps(data, indent=2, default=json_default)

    if args.output == "-":
        print(output)
    else:
        with open(args.output, "w") as f:
            f.write(output)
        log.info("Written to %s", args.output)


if __name__ == "__main__":
    main()
