"""Dagster MCP server composition."""

from fastmcp import FastMCP

from dagster_mcp.config import READ_ONLY, env_instructions
from dagster_mcp.tools import (
    READ_TOOLS,
    WRITE_TOOLS,
    get_asset_details,
    get_asset_health,
    get_recent_materializations,
    get_run_failure_summary,
    get_run_logs,
    get_run_stats,
    get_run_status,
    get_runs,
    get_tick_history,
    get_instance_status,
    launch_job,
    launch_job_with_partitions,
    list_backfills,
    list_code_locations,
    list_jobs,
    list_schedules,
    list_sensors,
    reload_code_location,
    search_assets,
    terminate_run,
)


def create_mcp() -> FastMCP:
    mode = "read-only" if READ_ONLY else "read-write"
    server = FastMCP(
        "dagster",
        instructions=(
            f"Use these tools to monitor and operate a running Dagster instance ({mode} mode). "
            f"{env_instructions()}"
            "Start with list_jobs or get_runs to explore what is available, then "
            "drill into specific runs, assets, schedules, or sensors as needed."
        ),
    )
    register_tools(server)
    return server


def register_tools(server: FastMCP) -> None:
    for tool in READ_TOOLS:
        server.tool()(tool)
    if READ_ONLY:
        return
    for tool in WRITE_TOOLS:
        server.tool()(tool)


mcp = create_mcp()


def main() -> None:
    mcp.run()


__all__ = [
    "create_mcp",
    "get_asset_details",
    "get_asset_health",
    "get_recent_materializations",
    "get_run_failure_summary",
    "get_run_logs",
    "get_run_stats",
    "get_run_status",
    "get_runs",
    "get_tick_history",
    "get_instance_status",
    "launch_job",
    "launch_job_with_partitions",
    "list_backfills",
    "list_code_locations",
    "list_jobs",
    "list_schedules",
    "list_sensors",
    "main",
    "mcp",
    "register_tools",
    "reload_code_location",
    "search_assets",
    "terminate_run",
]


if __name__ == "__main__":
    main()
