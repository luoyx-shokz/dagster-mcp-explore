"""Dagster MCP backfills tools."""

from dagster_mcp.graphql import gql

# ── Backfills ─────────────────────────────────────────────────────────────────


def list_backfills(limit: int = 10, env: str | None = None) -> list[dict]:
    """List recent asset backfills with their status and partition progress.

    Returns per backfill: backfillId, status, numPartitions, timestamp,
    partitionNames, and partitionSetName.

    - limit: max backfills to return (default 10)

    When to use: to monitor in-progress backfills or review recent ones.
    """
    query = """
    query Backfills($limit: Int!, $cursor: String) {
      partitionBackfillsOrError(cursor: $cursor, limit: $limit) {
        ... on PartitionBackfills {
          results {
            backfillId: id
            status
            numPartitions
            timestamp
            partitionNames
            partitionSetName
          }
        }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"limit": limit}, env=env)
    return data.get("partitionBackfillsOrError", {}).get("results", [])
