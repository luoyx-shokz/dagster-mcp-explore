"""Dagster MCP actions tools."""

from collections.abc import Mapping

from dagster_mcp.graphql import gql

# ── Actions ───────────────────────────────────────────────────────────────────


def _execution_tags(tags: dict[str, str] | None) -> list[dict[str, str]]:
    return [{"key": key, "value": value} for key, value in _normalize_tags(tags).items()]


def _execution_metadata(tags: dict[str, str] | None) -> dict | None:
    dagster_tags = _execution_tags(tags)
    if not dagster_tags:
        return None
    return {"tags": dagster_tags}


def _normalize_tags(tags: object) -> dict[str, str]:
    if not tags:
        return {}
    if not isinstance(tags, Mapping):
        raise ValueError("tags must be a dict of string keys and string values.")

    normalized: dict[str, str] = {}
    for key, value in tags.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError("tags must be a dict of string keys and string values.")
        normalized[key] = value
    return normalized


def _run_config_without_execution_tags(
    run_config: dict | None,
    tags: dict[str, str] | None,
) -> tuple[dict | None, dict[str, str]]:
    normalized_tags = _normalize_tags(tags)
    if run_config is None:
        return None, normalized_tags
    if not isinstance(run_config, dict):
        raise ValueError("run_config must be a dict.")
    if "tags" not in run_config:
        return run_config, normalized_tags

    run_config_tags = _normalize_tags(run_config["tags"])
    conflicts = {
        key
        for key, value in run_config_tags.items()
        if key in normalized_tags and normalized_tags[key] != value
    }
    if conflicts:
        conflicting_keys = ", ".join(sorted(conflicts))
        raise ValueError(f"tags conflict between run_config and tags: {conflicting_keys}")

    clean_run_config = {key: value for key, value in run_config.items() if key != "tags"}
    return clean_run_config, {**run_config_tags, **normalized_tags}


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


def _resolve_partition_set_name(
    job_name: str,
    repository_location: str,
    repository_name: str,
    env: str | None,
) -> str:
    query = """
    query PartitionSetsForJob(
      $repositorySelector: RepositorySelector!,
      $jobName: String!
    ) {
      partitionSetsOrError(
        repositorySelector: $repositorySelector,
        pipelineName: $jobName
      ) {
        __typename
        ... on PartitionSets { results { name } }
        ... on PipelineNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    variables = {
        "repositorySelector": {
            "repositoryLocationName": repository_location,
            "repositoryName": repository_name,
        },
        "jobName": job_name,
    }
    data = gql(query, variables, env=env)
    result = data.get("partitionSetsOrError", {})
    typename = result.get("__typename", "UnknownResult")
    if typename != "PartitionSets":
        message = result.get("message", result)
        raise RuntimeError(
            f"Dagster partition set lookup failed ({typename}): {message}"
        )

    names = [item.get("name") for item in result.get("results", []) if item.get("name")]
    if len(names) == 1:
        return names[0]
    if not names:
        raise RuntimeError(f"Dagster job {job_name!r} has no partition set.")
    raise RuntimeError(
        f"Dagster job {job_name!r} has multiple partition sets: {names}. "
        "Pass partition_set_name explicitly."
    )


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
    clean_run_config, merged_tags = _run_config_without_execution_tags(run_config, tags)

    variables = {
        "locationName": repository_location,
        "repoName": repository_name,
        "jobName": job_name,
        "solidSelection": asset_keys or None,
        "runConfigData": clean_run_config or {},
        "executionMetadata": _execution_metadata(merged_tags),
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
    run_config: dict | None = None,
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
    - partition_set_name: partition set name. When omitted, the exact name is
      resolved from Dagster metadata for the selected job and repository.
    - tags: additional key-value tags to attach to the launched runs.
      Example: {'triggered_by': 'dataops_agent'}
    - run_config: dict of run configuration to pass to each partition run.
      Example: {'ops': {'clean': {'config': {'dry_run': False, 'approved': True}}}}
    - from_failure: if True, only re-run the failed steps within the given
      partitions (useful for retrying partially-failed partitioned runs)

    Returns backfillId on success — even for a single partition, Dagster creates
    a backfill record. Use list_backfills to monitor progress.

    When to use: to run a job for a specific date/partition, backfill historical
    partitions, or retry failed partitions. For non-partitioned jobs, use launch_job.
    """
    if not partition_keys:
        raise ValueError("partition_keys must contain at least one partition key.")

    clean_run_config, merged_tags = _run_config_without_execution_tags(run_config, tags)
    resolved_partition_set = partition_set_name or _resolve_partition_set_name(
        job_name,
        repository_location,
        repository_name,
        env,
    )

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
            "tags": _execution_tags(merged_tags),
            "fromFailure": from_failure,
        }
    }
    if clean_run_config is not None:
        variables["backfillParams"]["runConfigData"] = clean_run_config

    data = gql(query, variables, env=env)
    result = data.get("launchPartitionBackfill", {})
    if (
        result.get("__typename") == "LaunchBackfillSuccess"
        and result.get("backfillId")
    ):
        return result

    _raise_backfill_launch_error(result)
