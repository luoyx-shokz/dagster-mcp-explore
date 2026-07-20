"""Dagster MCP assets tools."""

from dagster_mcp.graphql import gql

# ── Assets ────────────────────────────────────────────────────────────────────


def get_recent_materializations(
    asset_key: str,
    limit: int = 5,
    env: str | None = None,
) -> list[dict]:
    """Get the most recent materializations for an asset, with metadata.

    Returns a list of materializations, each with: runId, timestamp,
    assetKey, and metadataEntries (labels, numeric values, text).

    - asset_key: the asset name as a string (e.g. 'my_daily_report')
    - limit: max materializations to return (default 5)

    When to use: to check when an asset was last materialized, track
    materialization frequency, or inspect metadata from recent runs.
    For a broader health view (including staleness and freshness),
    use get_asset_health instead.
    """
    query = """
    query AssetRuns($assetKey: AssetKeyInput!, $limit: Int!) {
      assetOrError(assetKey: $assetKey) {
        ... on Asset {
          assetMaterializations(limit: $limit) {
            runId
            timestamp
            assetKey { path }
            metadataEntries {
              label
              ... on IntMetadataEntry { intValue }
              ... on FloatMetadataEntry { floatValue }
              ... on TextMetadataEntry { text }
            }
          }
        }
      }
    }
    """
    data = gql(query, {"assetKey": {"path": [asset_key]}, "limit": limit}, env=env)
    asset = data.get("assetOrError", {})
    return asset.get("assetMaterializations", [])


def get_asset_details(asset_keys: list[str], env: str | None = None) -> list[dict]:
    """Get detailed metadata for one or more assets: description, lineage, and partitions.

    - asset_keys: list of asset name strings (e.g. ['my_extract', 'my_load'])

    Returns per asset: assetKey, description, groupName, op name,
    isObservable, isPartitioned, partitionDefinition, dependencyKeys
    (upstream assets), dependedByKeys (downstream assets), and the
    latest materialization (runId + timestamp).

    When to use: to understand an asset's lineage (what it depends on
    and what depends on it), check if it's partitioned, or get its
    description. Use search_assets first if you don't know the exact key.
    """
    query = """
    query AssetDetails($assetKeys: [AssetKeyInput!]!) {
      assetNodes(assetKeys: $assetKeys) {
        assetKey { path }
        description
        groupName
        op { name }
        isObservable
        isPartitioned
        partitionDefinition { description }
        dependencyKeys { path }
        dependedByKeys { path }
        assetMaterializations(limit: 1) {
          runId
          timestamp
        }
      }
    }
    """
    keys = [{"path": [k]} for k in asset_keys]
    data = gql(query, {"assetKeys": keys}, env=env)
    return data.get("assetNodes", [])


def search_assets(
    prefix: str | None = None,
    group: str | None = None,
    env: str | None = None,
) -> list[dict]:
    """Search and list assets by name prefix or group. Use this to discover assets.

    Returns per asset: assetKey, groupName, description, isPartitioned, op name.

    - prefix: case-insensitive substring match on any part of the asset key
      (e.g. 'raw_' finds 'raw_orders', 'raw_users')
    - group: exact match on groupName (case-insensitive, e.g. 'analytics')
    - Both filters can be combined.
    - If neither is passed, returns ALL assets.

    When to use: to discover available assets before calling get_asset_details
    or get_asset_health. Use prefix for fuzzy search, group for scoped listing.
    """
    query = """
    query AllAssets {
      assetNodes {
        assetKey { path }
        groupName
        description
        isPartitioned
        op { name }
      }
    }
    """
    data = gql(query, env=env)
    nodes = data.get("assetNodes", [])
    if prefix:
        prefix_lower = prefix.lower()
        nodes = [n for n in nodes if any(prefix_lower in p.lower() for p in n["assetKey"]["path"])]
    if group:
        group_lower = group.lower()
        nodes = [n for n in nodes if (n.get("groupName") or "").lower() == group_lower]
    return nodes


def get_asset_health(asset_key_or_group: str, env: str | None = None) -> list[dict]:
    """Get a consolidated health view for a single asset or all assets in a group.

    This is the BEST tool to assess whether assets are healthy and up-to-date.

    - asset_key_or_group: pass either a single asset key (e.g. 'my_report')
      or a group name (e.g. 'analytics'). If it matches a group, returns
      health for ALL assets in that group.

    Returns per asset:
    - asset_key, group, description
    - last_materialization: {run_id, timestamp, status} of the latest run
    - freshness_policy: {maximum_lag_minutes, cron_schedule} if defined
    - staleness: {is_stale, reasons[]} explaining why the asset is stale

    When to use: to check if critical assets are fresh, find stale assets
    in a group, or verify that recent materializations succeeded.
    Prefer this over get_recent_materializations when you need a health
    assessment rather than raw materialization history.
    """
    # First try as a group — fetch all assets and filter
    all_query = """
    query AllAssets {
      assetNodes {
        assetKey { path }
        groupName
      }
    }
    """
    all_data = gql(all_query, env=env)
    all_nodes = all_data.get("assetNodes", [])

    # Check if it's a group name
    group_keys = [
        n["assetKey"]["path"]
        for n in all_nodes
        if (n.get("groupName") or "").lower() == asset_key_or_group.lower()
    ]

    if group_keys:
        asset_keys_input = [{"path": k} for k in group_keys]
    else:
        asset_keys_input = [{"path": [asset_key_or_group]}]

    # Fetch health details
    health_query = """
    query AssetHealth($assetKeys: [AssetKeyInput!]!) {
      assetNodes(assetKeys: $assetKeys) {
        assetKey { path }
        groupName
        freshnessPolicy { maximumLagMinutes cronSchedule }
        staleCauses { key { path } reason dependency { path } }
        assetMaterializations(limit: 1) {
          runId
          timestamp
        }
      }
    }
    """
    health_data = gql(health_query, {"assetKeys": asset_keys_input}, env=env)
    nodes = health_data.get("assetNodes", [])

    if not nodes:
        return [{"asset_key": asset_key_or_group, "message": "Asset not found."}]

    # For each asset, get the latest run status if there's a materialization
    run_ids = set()
    for n in nodes:
        mats = n.get("assetMaterializations", [])
        if mats:
            run_ids.add(mats[0]["runId"])

    run_statuses: dict[str, str] = {}
    if run_ids:
        runs_query = """
        query RunStatuses($filter: RunsFilter) {
          runsOrError(filter: $filter, limit: 100) {
            ... on Runs {
              results { runId status }
            }
          }
        }
        """
        runs_data = gql(runs_query, {"filter": {"runIds": list(run_ids)}}, env=env)
        for r in runs_data.get("runsOrError", {}).get("results", []):
            run_statuses[r["runId"]] = r["status"]

    results = []
    for n in nodes:
        mats = n.get("assetMaterializations", [])
        last_mat = None
        latest_run_status = None
        if mats:
            last_mat = {"run_id": mats[0]["runId"], "timestamp": mats[0]["timestamp"]}
            latest_run_status = run_statuses.get(mats[0]["runId"])

        fp = n.get("freshnessPolicy")
        freshness_policy = None
        if fp:
            freshness_policy = {
                "max_lag_minutes": fp.get("maximumLagMinutes"),
                "cron": fp.get("cronSchedule"),
            }

        stale_causes = n.get("staleCauses", [])
        results.append(
            {
                "asset_key": n["assetKey"]["path"],
                "group": n.get("groupName"),
                "last_materialization": last_mat,
                "latest_run_status": latest_run_status,
                "freshness_policy": freshness_policy,
                "stale": len(stale_causes) > 0,
                "stale_causes": [c.get("reason", "") for c in stale_causes],
            }
        )

    return results
