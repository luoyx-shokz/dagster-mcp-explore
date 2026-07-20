"""GraphQL transport for Dagster MCP tools."""

import httpx

from dagster_mcp.config import build_headers, resolve_connection

_RUNS_FILTER_QUERY = '{ __type(name: "RunsFilter") { inputFields { name } } }'
_runs_filter_job_field: dict[str, str] = {}


def get_runs_filter_job_field(env: str | None = None) -> str:
    graphql_url, api_token, extra_headers_json = resolve_connection(env)

    if graphql_url in _runs_filter_job_field:
        return _runs_filter_job_field[graphql_url]

    try:
        headers = build_headers(api_token, extra_headers_json)
        response = httpx.post(
            graphql_url,
            json={"query": _RUNS_FILTER_QUERY},
            headers=headers,
            timeout=30,
        )
        data = response.json()
        fields = {f["name"] for f in data.get("data", {}).get("__type", {}).get("inputFields", [])}
        if "jobName" in fields:
            field = "jobName"
        elif "pipelineName" in fields:
            field = "pipelineName"
        else:
            field = "jobName"
    except Exception:
        field = "jobName"

    _runs_filter_job_field[graphql_url] = field
    return field


def gql(query: str, variables: dict | None = None, env: str | None = None) -> dict:
    graphql_url, api_token, extra_headers_json = resolve_connection(env)
    headers = build_headers(api_token, extra_headers_json)
    try:
        response = httpx.post(
            graphql_url,
            json={"query": query, "variables": variables or {}},
            headers=headers,
            timeout=30,
        )
    except httpx.ConnectError:
        base_url = graphql_url.removesuffix("/graphql")
        raise RuntimeError(
            f"Cannot connect to Dagster at {base_url}. "
            "Check that DAGSTER_URL is correct and the instance is running."
        )
    except httpx.TimeoutException:
        base_url = graphql_url.removesuffix("/graphql")
        raise RuntimeError(f"Request to Dagster at {base_url} timed out after 30s.")
    if response.status_code >= 400:
        raise RuntimeError(f"Dagster returned HTTP {response.status_code}: {response.text[:500]}")
    data = response.json()
    if "errors" in data:
        messages = [e.get("message", str(e)) for e in data["errors"]]
        raise RuntimeError("Dagster GraphQL error: " + "; ".join(messages))
    return data["data"]
