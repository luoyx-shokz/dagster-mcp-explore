"""Dagster MCP actions tools."""

from dagster_mcp.graphql import gql

# ── Actions ───────────────────────────────────────────────────────────────────


def _execution_tags(tags: dict[str, str] | None) -> list[dict[str, str]]:
    if not tags:
        return []
    return [{"key": key, "value": value} for key, value in tags.items()]


def _execution_metadata(tags: dict[str, str] | None) -> dict | None:
    dagster_tags = _execution_tags(tags)
    if not dagster_tags:
        return None
    return {"tags": dagster_tags}


def _raise_backfill_launch_error(result: dict) -> None:
    typename = result.get("__typename", "UnknownResult")
    if result.get("message"):
        raise RuntimeError(f"Dagster backfill launch failed ({typename}): {result['message']}")
    if result.get("errors"):
        messages = [error.get("message", str(error)) for error in result["errors"]]
        raise RuntimeError(
            f"Dagster backfill launch failed ({typename}): " + "; ".join(messages)
        )
    raise RuntimeError(f"Dagster backfill launch failed ({typename}): {result}")


def terminate_run(run_id: str, env: str | None = None) -> dict:
    """Terminate a running or queued Dagster run.

    - run_id: the runId to terminate (get it from get_runs)

    Returns the run's final status on success, or an error message if the
    run was not found or could not be terminated.

    When to use: to stop a stuck, hung, or runaway run. Only works on runs
    with status STARTED or QUEUED. Already-finished runs cannot be terminated.
    """
    query = """
    mutation TerminateRun($runId: String!) {
      terminateRun(runId: $runId) {
        ... on TerminateRunSuccess { run { runId status } }
        ... on TerminateRunFailure { message }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"runId": run_id}, env=env)
    return data.get("terminateRun", {})


def launch_job(
    job_name: str,
    repository_location: str,
    repository_name: str = "__repository__",
    asset_keys: list[str] | None = None,
    tags: dict[str, str] | None = None,
    run_config: dict | None = None,
    env: str | None = None,
) -> dict:
    """Launch a job or materialize specific assets. Use list_jobs first to find valid names.

    Required parameters:
    - job_name: name of the job (from list_jobs, e.g. 'my_etl_job')
    - repository_location: code location name (from list_jobs, e.g. 'my_project')
    - repository_name: defaults to '__repository__', override if you have
      multiple repositories in a single code location

    Optional parameters:
    - asset_keys: list of asset key strings to materialize. Use this with the
      job that targets them (often '__ASSET_JOB' or a custom asset job name).
      Example: ['raw_orders', 'clean_orders']
    - tags: dict of key-value tags to attach to the run.
      Example: {'triggered_by': 'dataops_agent', 'priority': 'high'}
    - run_config: dict of run configuration to pass to the job. This is the
      same YAML/dict you would enter in the Dagster UI Launchpad.
      Example: {'ops': {'my_op': {'config': {'start_date': '2026-03-01'}}}}

    Returns the launched run's runId and status on success, or an error message.

    When to use: to re-run a failed job, trigger an ad-hoc materialization,
    or launch a job with custom config or tags. After launching, use
    get_run_status or get_runs to monitor progress.
    """
    query = """
    mutation LaunchJob(
      $locationName: String!,
      $repoName: String!,
      $jobName: String!,
      $solidSelection: [String!],
      $executionMetadata: ExecutionMetadata,
      $runConfigData: RunConfigData
    ) {
      launchRun(executionParams: {
        selector: {
          repositoryLocationName: $locationName,
          repositoryName: $repoName,
          jobName: $jobName,
          solidSelection: $solidSelection
        },
        runConfigData: $runConfigData,
        executionMetadata: $executionMetadata
      }) {
        ... on LaunchRunSuccess { run { runId status } }
        ... on InvalidSubsetError { message }
        ... on PythonError { message }
        ... on PresetNotFoundError { message }
        ... on ConflictingExecutionParamsError { message }
        ... on RunConfigValidationInvalid { errors { message } }
      }
    }
    """
    variables = {
        "locationName": repository_location,
        "repoName": repository_name,
        "jobName": job_name,
        "solidSelection": asset_keys or None,
        "runConfigData": run_config or {},
        "executionMetadata": _execution_metadata(tags),
    }
    data = gql(query, variables, env=env)
    return data.get("launchRun", {})


def launch_job_with_partitions(
    job_name: str,
    repository_location: str,
    partition_keys: list[str],
    repository_name: str = "__repository__",
    partition_set_name: str | None = None,
    tags: dict[str, str] | None = None,
    from_failure: bool = False,
    env: str | None = None,
) -> dict:
    """Launch a partitioned job for one or more partition keys.

    Use list_jobs to find job names. Use get_asset_details to check if an asset
    is partitioned (isPartitioned field) and what partition definition it uses.

    Required parameters:
    - job_name: name of the partitioned job (from list_jobs)
    - repository_location: code location name (from list_jobs)
    - partition_keys: one or more partition key strings to run.
      Examples: ['2024-01-01'], ['2024-01-01', '2024-01-02', '2024-01-03']

    Optional parameters:
    - repository_name: defaults to '__repository__', override if you have
      multiple repositories in a single code location
    - partition_set_name: partition set name; defaults to '{job_name}_partition_set'.
      Override this if the job uses a non-standard partition set name.
    - tags: additional key-value tags to attach to the launched runs.
      Example: {'triggered_by': 'dataops_agent'}
    - from_failure: if True, only re-run the failed steps within the given
      partitions (useful for retrying partially-failed partitioned runs)

    Returns backfillId on success — even for a single partition, Dagster creates
    a backfill record. Use list_backfills to monitor progress.

    When to use: to run a job for a specific date/partition, backfill historical
    partitions, or retry failed partitions. For non-partitioned jobs, use launch_job.
    """
    resolved_partition_set = partition_set_name or f"{job_name}_partition_set"

    query = """
    mutation LaunchPartitionBackfill($backfillParams: LaunchBackfillParams!) {
      launchPartitionBackfill(backfillParams: $backfillParams) {
        __typename
        ... on LaunchBackfillSuccess { backfillId launchedRunIds }
        ... on PartitionSetNotFoundError { message }
        ... on PartitionKeysNotFoundError { message partitionKeys }
        ... on PipelineNotFoundError { message }
        ... on PythonError { message }
        ... on UnauthorizedError { message }
        ... on InvalidSubsetError { message }
        ... on RunConflict { message }
        ... on ConflictingExecutionParamsError { message }
        ... on RunConfigValidationInvalid {
          pipelineName
          errors { message }
        }
      }
    }
    """
    variables = {
        "backfillParams": {
            "selector": {
                "repositorySelector": {
                    "repositoryLocationName": repository_location,
                    "repositoryName": repository_name,
                },
                "partitionSetName": resolved_partition_set,
            },
            "partitionNames": partition_keys,
            "tags": _execution_tags(tags),
            "fromFailure": from_failure,
        }
    }
    data = gql(query, variables, env=env)
    result = data.get("launchPartitionBackfill", {})
    if result.get("__typename") == "LaunchBackfillSuccess":
        return result

    _raise_backfill_launch_error(result)
