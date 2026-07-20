"""Dagster MCP runs tools."""

from dagster_mcp.graphql import get_runs_filter_job_field, gql

# ── Runs ──────────────────────────────────────────────────────────────────────


def get_runs(
    job_name: str | None = None,
    statuses: list[str] | None = None,
    limit: int = 10,
    env: str | None = None,
) -> list[dict]:
    """List recent pipeline runs. Start here to discover what has been running.

    Returns runId, status, jobName, startTime, endTime, and tags for each run.
    Use the returned runId to drill into details with get_run_status,
    get_run_logs, get_run_stats, or get_run_failure_summary.

    Filtering:
    - job_name: filter by job (e.g. 'my_etl_job')
    - statuses: filter by one or more statuses.
      Valid values: 'SUCCESS', 'FAILURE', 'CANCELED', 'STARTED', 'QUEUED',
      'STARTING', 'CANCELING', 'NOT_STARTED'.
      Examples: ['FAILURE'], ['FAILURE', 'CANCELED'], ['STARTED', 'QUEUED']
    - limit: max runs to return (default 10)

    Typical workflows:
    - Find recent failures: get_runs(statuses=['FAILURE'])
    - Check if a job ran today: get_runs(job_name='my_job', limit=5)
    - Monitor active runs: get_runs(statuses=['STARTED', 'QUEUED'])
    """
    query = """
    query Runs($limit: Int!, $filter: RunsFilter) {
      runsOrError(limit: $limit, filter: $filter) {
        ... on Runs {
          results {
            runId
            status
            jobName
            startTime
            endTime
            tags { key value }
          }
        }
        ... on PythonError { message }
      }
    }
    """
    filter_var: dict = {}
    if statuses:
        filter_var["statuses"] = statuses
    if job_name:
        field = get_runs_filter_job_field(env)
        filter_var[field] = job_name
    data = gql(query, {"limit": limit, "filter": filter_var or None}, env=env)
    runs = data.get("runsOrError", {})
    return runs.get("results", [])


def get_run_status(run_id: str, env: str | None = None) -> dict:
    """Get full details for a single run: status, config, tags, and run lineage.

    Returns: runId, status, startTime, endTime, jobName, tags, runConfigYaml,
    rootRunId, parentRunId, resolvedOpSelection.

    Use rootRunId and parentRunId to understand re-execution chains — if
    parentRunId is set, this run was re-executed from another run.
    resolvedOpSelection shows which steps were selected for re-execution.

    When to use: after get_runs to inspect a specific run, or to check
    whether a run is a re-execution of a previous one.
    """
    query = """
    query RunStatus($runId: ID!) {
      runOrError(runId: $runId) {
        ... on Run {
          runId
          status
          startTime
          endTime
          jobName
          tags { key value }
          runConfigYaml
          rootRunId
          parentRunId
          resolvedOpSelection
        }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"runId": run_id}, env=env)
    return data.get("runOrError", {})


def get_run_logs(
    run_id: str,
    cursor: str | None = None,
    limit: int = 100,
    level_filter: str | None = None,
    env: str | None = None,
) -> dict:
    """Get structured log events for a run, with optional severity filtering and pagination.

    Returns events with __typename, timestamp, message, level, and (where applicable)
    stepKey and error details. Events include step starts/completions, failures,
    retries, materializations, and run-level events.

    Parameters:
    - run_id: the run to fetch logs for
    - level_filter: only return events at this level or above.
      Values: 'DEBUG', 'INFO', 'WARNING', 'ERROR'. When set to 'ERROR',
      also includes ExecutionStepFailureEvent and RunFailureEvent regardless
      of their level field. Default: None (return all events).
    - cursor: pagination cursor returned in previous response. Pass the
      cursor from the last call to get the next page.
    - limit: max events per page (default 100)

    When to use: to investigate what happened during a run. For a quick
    failure diagnosis, prefer get_run_failure_summary instead — it returns
    a consolidated view in a single call. Use get_run_logs when you need
    the full event stream or want to filter by level.
    """
    query = """
    query RunLogs($runId: ID!, $afterCursor: String, $limit: Int!) {
      logsForRun(runId: $runId, afterCursor: $afterCursor, limit: $limit) {
        ... on EventConnection {
          cursor
          hasMore
          events {
            __typename
            ... on MessageEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on LogsCapturedEvent {
              timestamp
              message
              level
              stepKey
              logKey
              fileKey
            }
            ... on ExecutionStepStartEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on ExecutionStepSuccessEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on ExecutionStepOutputEvent {
              timestamp
              message
              level
              stepKey
              outputName
            }
            ... on ExecutionStepInputEvent {
              timestamp
              message
              level
              stepKey
              inputName
            }
            ... on ExecutionStepFailureEvent {
              timestamp
              message
              level
              stepKey
              error { message causes { message } }
            }
            ... on RunFailureEvent {
              timestamp
              message
              level
              error { message causes { message } }
            }
            ... on ExecutionStepUpForRetryEvent {
              timestamp
              message
              level
              stepKey
              secondsToWait
              error { message causes { message } }
            }
            ... on MaterializationEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on ObjectStoreOperationEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on HandledOutputEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on LoadedInputEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on EngineEvent {
              timestamp
              message
              level
              stepKey
              error { message causes { message } }
            }
            ... on RunStartEvent {
              timestamp
              message
              level
            }
            ... on RunSuccessEvent {
              timestamp
              message
              level
            }
            ... on RunStartingEvent {
              timestamp
              message
              level
            }
            ... on RunEnqueuedEvent {
              timestamp
              message
              level
            }
            ... on RunDequeuedEvent {
              timestamp
              message
              level
            }
            ... on RunCancelingEvent {
              timestamp
              message
              level
            }
            ... on RunCanceledEvent {
              timestamp
              message
              level
            }
          }
        }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"runId": run_id, "afterCursor": cursor, "limit": limit}, env=env)
    result = data.get("logsForRun", {})

    if level_filter and "events" in result:
        upper = level_filter.upper()
        error_types = ("ExecutionStepFailureEvent", "RunFailureEvent")
        result["events"] = [
            e
            for e in result["events"]
            if e.get("level") == upper or (upper == "ERROR" and e.get("__typename") in error_types)
        ]

    return result


def get_run_stats(run_id: str, env: str | None = None) -> dict:
    """Get per-step execution statistics for a run: timing, materializations, and expectations.

    Returns runId, status, and a stepStats array where each entry has:
    stepKey, status, startTime, endTime, materializations (with labels),
    and expectationResults (with success flag and labels).

    When to use: to find slow steps (compare startTime/endTime), check which
    steps materialized assets, or verify expectation results.
    For failed runs, prefer get_run_failure_summary which includes step stats
    alongside error details and suggestions.
    """
    query = """
    query RunStats($runId: ID!) {
      runOrError(runId: $runId) {
        ... on Run {
          runId
          status
          stepStats {
            stepKey
            status
            startTime
            endTime
            materializations { label }
            expectationResults { success label }
          }
        }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"runId": run_id}, env=env)
    return data.get("runOrError", {})


def get_run_failure_summary(run_id: str, env: str | None = None) -> dict:
    """Get a consolidated failure diagnosis for a run in a single call.

    This is the BEST tool to use when investigating a failed or canceled run.
    It combines status, step stats, and error logs into one response, avoiding
    the need to call get_run_status + get_run_logs + get_run_stats separately.

    Returns:
    - status, job_name, duration_seconds
    - failed_steps: list of {step_key, duration, error} for each failed step
    - root_cause_error: the RunFailureEvent error (if any)
    - all_step_durations: timing for every step (not just failed ones)
    - suggestions: automated diagnostic hints (e.g. 'Multiple steps failed',
      'Step was retried before failing', 'Run was canceled')

    If the run did not fail, returns {message: 'Run did not fail.'}.

    When to use: always prefer this over get_run_logs for failed runs.
    Use get_run_logs only when you need the full event stream.
    """
    # 1. Fetch run status + step stats in one query
    status_query = """
    query FailureSummary($runId: ID!) {
      runOrError(runId: $runId) {
        ... on Run {
          runId
          status
          jobName
          startTime
          endTime
          stepStats {
            stepKey
            status
            startTime
            endTime
          }
        }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    run_data = gql(status_query, {"runId": run_id}, env=env).get("runOrError", {})

    if "message" in run_data:
        return run_data

    status = run_data.get("status", "")
    if status not in ("FAILURE", "CANCELED"):
        return {"run_id": run_id, "status": status, "message": "Run did not fail."}

    # 2. Collect error events from logs (paginate up to 500 events)
    error_events: list[dict] = []
    cursor = None
    for _ in range(5):
        log_query = """
        query FailureLogs($runId: ID!, $afterCursor: String) {
          logsForRun(runId: $runId, afterCursor: $afterCursor, limit: 100) {
            ... on EventConnection {
              cursor
              hasMore
              events {
                __typename
                ... on ExecutionStepFailureEvent {
                  timestamp
                  stepKey
                  error { message causes { message } }
                }
                ... on RunFailureEvent {
                  timestamp
                  error { message causes { message } }
                }
                ... on ExecutionStepUpForRetryEvent {
                  timestamp
                  stepKey
                  secondsToWait
                  error { message causes { message } }
                }
              }
            }
            ... on RunNotFoundError { message }
          }
        }
        """
        log_data = gql(log_query, {"runId": run_id, "afterCursor": cursor}, env=env).get(
            "logsForRun", {}
        )
        events = log_data.get("events", [])
        for e in events:
            if e.get("__typename") in (
                "ExecutionStepFailureEvent",
                "RunFailureEvent",
                "ExecutionStepUpForRetryEvent",
            ):
                error_events.append(e)
        if not log_data.get("hasMore"):
            break
        cursor = log_data.get("cursor")

    # 3. Build step durations
    step_stats = run_data.get("stepStats", [])
    all_step_durations = []
    for s in step_stats:
        dur = None
        if s.get("startTime") and s.get("endTime"):
            dur = round(s["endTime"] - s["startTime"], 2)
        all_step_durations.append(
            {
                "step_key": s["stepKey"],
                "status": s["status"],
                "duration_seconds": dur,
            }
        )

    # 4. Build failed steps with errors
    failed_step_keys = {s["stepKey"] for s in step_stats if s["status"] == "FAILURE"}
    step_errors: dict[str, dict] = {}
    for e in error_events:
        sk = e.get("stepKey")
        if sk and sk in failed_step_keys and sk not in step_errors:
            step_errors[sk] = e.get("error", {})

    failed_steps = []
    for s in step_stats:
        if s["stepKey"] in failed_step_keys:
            dur = None
            if s.get("startTime") and s.get("endTime"):
                dur = round(s["endTime"] - s["startTime"], 2)
            failed_steps.append(
                {
                    "step_key": s["stepKey"],
                    "duration_seconds": dur,
                    "error": step_errors.get(s["stepKey"], {}),
                }
            )

    # 5. Root cause error (run-level failure or first step failure)
    root_cause = None
    run_failure = [e for e in error_events if e.get("__typename") == "RunFailureEvent"]
    if run_failure:
        root_cause = run_failure[0].get("error", {})
    elif failed_steps:
        root_cause = failed_steps[0].get("error", {})

    # 6. Suggestions
    suggestions: list[str] = []
    retries = [e for e in error_events if e.get("__typename") == "ExecutionStepUpForRetryEvent"]
    if retries:
        retry_keys = {e["stepKey"] for e in retries}
        suggestions.append(f"Steps retried before failing: {', '.join(sorted(retry_keys))}")
    if len(failed_steps) > 1:
        suggestions.append(
            f"Multiple steps failed ({len(failed_steps)}). "
            f"First failure: {failed_steps[0]['step_key']} — downstream failures may be cascading."
        )
    if status == "CANCELED":
        suggestions.append("Run was canceled, not all steps may have executed.")

    run_dur = None
    if run_data.get("startTime") and run_data.get("endTime"):
        run_dur = round(run_data["endTime"] - run_data["startTime"], 2)

    return {
        "run_id": run_id,
        "status": status,
        "job_name": run_data.get("jobName"),
        "duration_seconds": run_dur,
        "failed_steps": failed_steps,
        "root_cause_error": root_cause,
        "all_step_durations": all_step_durations,
        "suggestions": suggestions,
    }
