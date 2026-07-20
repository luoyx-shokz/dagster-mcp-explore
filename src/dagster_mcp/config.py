"""Runtime configuration for Dagster MCP."""

import json
import os

DAGSTER_URL = os.environ.get("DAGSTER_URL", "http://localhost:3000")
DAGSTER_API_TOKEN = os.environ.get("DAGSTER_API_TOKEN", "")
DAGSTER_EXTRA_HEADERS = os.environ.get("DAGSTER_EXTRA_HEADERS", "")
READ_ONLY = os.environ.get("DAGSTER_READ_ONLY", "true").lower() in ("true", "1", "yes")

_DAGSTER_ENVS_RAW = os.environ.get("DAGSTER_ENVS", "")
_DAGSTER_DEFAULT_ENV = os.environ.get("DAGSTER_DEFAULT_ENV", "")


def _parse_dagster_envs(raw: str) -> dict[str, dict]:
    if not raw:
        return {}
    try:
        envs = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "DAGSTER_ENVS must be a valid JSON object "
            "(example: '{\"prod\": {\"url\": \"https://prod.dagster.io\", \"token\": \"...\"}, "
            "\"dev\": {\"url\": \"http://localhost:3000\"}}')."
        ) from exc
    if not isinstance(envs, dict):
        raise RuntimeError("DAGSTER_ENVS must be a JSON object mapping env names to configs.")
    return envs


DAGSTER_ENVS: dict[str, dict] = _parse_dagster_envs(_DAGSTER_ENVS_RAW)


def env_instructions() -> str:
    if not DAGSTER_ENVS:
        return ""
    return f"Available environments: {', '.join(DAGSTER_ENVS)}. Pass env=<name> to each tool. "


def resolve_connection(env: str | None) -> tuple[str, str, str]:
    if not DAGSTER_ENVS:
        return (
            f"{DAGSTER_URL.rstrip('/')}/graphql",
            DAGSTER_API_TOKEN,
            DAGSTER_EXTRA_HEADERS,
        )

    name = env or _DAGSTER_DEFAULT_ENV
    if not name:
        if len(DAGSTER_ENVS) == 1:
            name = next(iter(DAGSTER_ENVS))
        else:
            raise RuntimeError(
                f"Multiple Dagster envs configured but no env specified. "
                f"Available: {', '.join(DAGSTER_ENVS)}. "
                "Pass env=<name> to the tool or set DAGSTER_DEFAULT_ENV."
            )

    if name not in DAGSTER_ENVS:
        raise RuntimeError(f"Unknown Dagster env '{name}'. Available: {', '.join(DAGSTER_ENVS)}.")

    cfg = DAGSTER_ENVS[name]
    url = cfg.get("url", "http://localhost:3000")
    token = cfg.get("token", "")
    extra = cfg.get("extra_headers", "")
    return f"{url.rstrip('/')}/graphql", token, extra


def build_headers(
    api_token: str | None = None,
    extra_headers_json: str | None = None,
) -> dict[str, str]:
    if api_token is None:
        api_token = DAGSTER_API_TOKEN
    if extra_headers_json is None:
        extra_headers_json = DAGSTER_EXTRA_HEADERS

    headers: dict[str, str] = {}
    if api_token:
        headers["Dagster-Cloud-Api-Token"] = api_token
    if extra_headers_json:
        try:
            extra_headers = json.loads(extra_headers_json)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "DAGSTER_EXTRA_HEADERS must be a valid JSON object "
                "(example: '{\"Authorization\":\"Bearer token\"}')."
            ) from exc

        if not isinstance(extra_headers, dict):
            raise RuntimeError("DAGSTER_EXTRA_HEADERS must be a JSON object.")

        invalid_pairs = [
            (key, value)
            for key, value in extra_headers.items()
            if not isinstance(key, str) or not isinstance(value, str)
        ]
        if invalid_pairs:
            raise RuntimeError("DAGSTER_EXTRA_HEADERS keys and values must be strings.")

        headers.update(extra_headers)
    return headers
