"""Tool registration groups for the Dagster MCP server."""

from dagster_mcp.tools.actions import launch_job, launch_job_with_partitions, terminate_run
from dagster_mcp.tools.assets import (
    get_asset_details,
    get_asset_health,
    get_recent_materializations,
    search_assets,
)
from dagster_mcp.tools.backfills import list_backfills
from dagster_mcp.tools.definitions import (
    get_instance_status,
    get_tick_history,
    list_code_locations,
    list_jobs,
    list_schedules,
    list_sensors,
    reload_code_location,
)
from dagster_mcp.tools.runs import (
    get_run_failure_summary,
    get_run_logs,
    get_run_stats,
    get_run_status,
    get_runs,
)

READ_TOOLS = (
    get_runs,
    get_run_status,
    get_run_logs,
    get_run_stats,
    get_run_failure_summary,
    get_recent_materializations,
    get_asset_details,
    search_assets,
    get_asset_health,
    list_jobs,
    list_schedules,
    list_sensors,
    get_tick_history,
    list_code_locations,
    get_instance_status,
    list_backfills,
)

WRITE_TOOLS = (
    reload_code_location,
    terminate_run,
    launch_job,
    launch_job_with_partitions,
)

__all__ = [
    "READ_TOOLS",
    "WRITE_TOOLS",
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
    "reload_code_location",
    "search_assets",
    "terminate_run",
]
