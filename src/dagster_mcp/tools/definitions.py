"""Dagster MCP definitions tools."""

from dagster_mcp.graphql import gql

# ── Jobs & Schedules & Sensors ────────────────────────────────────────────────


def list_jobs(env: str | None = None) -> list[dict]:
    """List all jobs across all code locations. Use this to discover available jobs.

    Returns per job: repository name, code location name, job name, and description.

    When to use: as a starting point to explore what jobs exist, or to find the
    exact job name and repository_location needed for launch_job.
    """
    query = """
    query ListJobs {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes {
            name
            location { name }
            jobs {
              name
              description
            }
          }
        }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, env=env)
    repos = data.get("repositoriesOrError", {}).get("nodes", [])
    result = []
    for repo in repos:
        for job in repo.get("jobs", []):
            result.append(
                {
                    "repository": repo["name"],
                    "location": repo["location"]["name"],
                    "job": job["name"],
                    "description": job.get("description", ""),
                }
            )
    return result


def list_schedules(env: str | None = None) -> list[dict]:
    """List all schedules with their status, cron expression, target job, and next tick.

    Returns per schedule: name, cron expression, status (RUNNING/STOPPED),
    next_tick timestamp, target job name, repository, and code location.

    When to use: to check which schedules are active, verify cron timing,
    or find schedules that are stopped and might need attention.
    If a schedule is RUNNING but jobs aren't executing, use
    get_tick_history to inspect recent ticks for errors.
    """
    query = """
    query ListSchedules {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes {
            name
            location { name }
            schedules {
              name
              cronSchedule
              scheduleState { status }
              futureTicks(limit: 1) { results { timestamp } }
              pipelineName
            }
          }
        }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, env=env)
    repos = data.get("repositoriesOrError", {}).get("nodes", [])
    result = []
    for repo in repos:
        for sched in repo.get("schedules", []):
            next_ticks = sched.get("futureTicks", {}).get("results", [])
            result.append(
                {
                    "repository": repo["name"],
                    "location": repo["location"]["name"],
                    "schedule": sched["name"],
                    "cron": sched.get("cronSchedule"),
                    "status": sched.get("scheduleState", {}).get("status"),
                    "next_tick": next_ticks[0]["timestamp"] if next_ticks else None,
                    "job": sched.get("pipelineName"),
                }
            )
    return result


def list_sensors(env: str | None = None) -> list[dict]:
    """List all sensors with their status and target jobs.

    Returns per sensor: name, status (RUNNING/STOPPED), list of target job names,
    repository, and code location.

    When to use: to check which sensors are active and what jobs they trigger.
    If a sensor is RUNNING but not producing runs, use get_tick_history to
    inspect recent ticks — it will show skipped ticks, errors, or runs launched.
    """
    query = """
    query ListSensors {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes {
            name
            location { name }
            sensors {
              name
              sensorState { status }
              targets { pipelineName }
            }
          }
        }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, env=env)
    repos = data.get("repositoriesOrError", {}).get("nodes", [])
    result = []
    for repo in repos:
        for sensor in repo.get("sensors", []):
            targets = [t["pipelineName"] for t in sensor.get("targets", [])]
            result.append(
                {
                    "repository": repo["name"],
                    "location": repo["location"]["name"],
                    "sensor": sensor["name"],
                    "status": sensor.get("sensorState", {}).get("status"),
                    "targets": targets,
                }
            )
    return result


def _find_instigator_repository(
    instigator_name: str,
    instigator_type: str,
    env: str | None = None,
) -> dict | None:
    query = """
    query FindInstigatorRepository {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes {
            name
            location { name }
            schedules {
              name
              scheduleState { id }
            }
            sensors {
              name
              sensorState { id }
            }
          }
        }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, env=env)
    repos_or_error = data.get("repositoriesOrError", {})
    if "message" in repos_or_error:
        return repos_or_error

    collection_name = "schedules" if instigator_type == "SCHEDULE" else "sensors"
    state_name = "scheduleState" if instigator_type == "SCHEDULE" else "sensorState"
    for repo in repos_or_error.get("nodes", []):
        for instigator in repo.get(collection_name, []):
            if instigator.get("name") == instigator_name:
                instigator_id = instigator.get(state_name, {}).get("id")
                if not instigator_id:
                    raise RuntimeError("Dagster instigator response did not include state id.")
                return {
                    "instigator_id": instigator_id,
                    "repository": repo["name"],
                    "location": repo["location"]["name"],
                }
    return None


def get_tick_history(
    instigator_name: str,
    instigator_type: str,
    limit: int = 20,
    env: str | None = None,
) -> dict:
    """Get recent tick history for a schedule or sensor — essential for detecting silent failures.

    - instigator_name: exact name of the schedule or sensor (from list_schedules/list_sensors)
    - instigator_type: 'SCHEDULE' or 'SENSOR'
    - limit: max ticks to return (default 20)

    Returns per tick: tick_id, status (SUCCESS/FAILURE/SKIPPED), timestamp,
    error message (if failed), and run_ids (runs launched by this tick).

    When to use: when a schedule or sensor is RUNNING but data is not being
    produced. Common patterns to look for:
    - All ticks SKIPPED: sensor condition not met, or misconfigured
    - Ticks with FAILURE status: the schedule/sensor code is erroring
    - Ticks with SUCCESS but empty run_ids: sensor evaluated but decided not to launch
    - Missing ticks: daemon may be unhealthy (check get_instance_status)
    """
    instigator_type = instigator_type.upper()
    if instigator_type not in ("SCHEDULE", "SENSOR"):
        raise ValueError("instigator_type must be 'SCHEDULE' or 'SENSOR'.")

    repository = _find_instigator_repository(instigator_name, instigator_type, env=env)
    if repository is None:
        return {
            "name": instigator_name,
            "instigator_type": instigator_type,
            "message": f"{instigator_type.capitalize()} '{instigator_name}' not found.",
        }
    if "message" in repository:
        return repository

    query = """
    query TickHistory($selector: InstigationSelector!, $id: String, $limit: Int!) {
      instigationStateOrError(instigationSelector: $selector, id: $id) {
        ... on InstigationState {
          name
          instigationType
          ticks(limit: $limit) {
            tickId
            status
            timestamp
            error { message }
            runIds
          }
        }
        ... on InstigationStateNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    data = gql(
        query,
        {
            "selector": {
                "repositoryLocationName": repository["location"],
                "repositoryName": repository["repository"],
                "name": instigator_name,
            },
            "id": repository["instigator_id"],
            "limit": limit,
        },
        env=env,
    )
    state = data.get("instigationStateOrError", {})

    if "message" in state:
        return {
            "name": instigator_name,
            "instigator_type": instigator_type,
            "repository": repository["repository"],
            "location": repository["location"],
            "message": state["message"],
        }

    if state.get("instigationType") != instigator_type:
        return {
            "name": instigator_name,
            "instigator_type": instigator_type,
            "repository": repository["repository"],
            "location": repository["location"],
            "message": f"{instigator_type.capitalize()} '{instigator_name}' not found.",
        }

    return {
        "name": state["name"],
        "instigator_type": state["instigationType"],
        "repository": repository["repository"],
        "location": repository["location"],
        "ticks": [
            {
                "tick_id": t["tickId"],
                "status": t["status"],
                "timestamp": t["timestamp"],
                "error": t.get("error", {}).get("message") if t.get("error") else None,
                "run_ids": t.get("runIds", []),
            }
            for t in state.get("ticks", [])
        ],
    }


# ── Code Locations ────────────────────────────────────────────────────────────


def list_code_locations(env: str | None = None) -> list[dict]:
    """List all code locations and their load status.

    Returns per location: name, loadStatus (LOADED/LOADING), and either the
    repositories within it or a PythonError if loading failed.

    When to use: after a deployment to verify code locations loaded correctly,
    or when get_instance_status reports code location errors.
    If a location failed to load, use reload_code_location to retry.
    """
    query = """
    query CodeLocations {
      workspaceOrError {
        ... on Workspace {
          locationEntries {
            name
            loadStatus
            locationOrLoadError {
              ... on RepositoryLocation {
                name
                repositories { name }
              }
              ... on PythonError { message }
            }
          }
        }
      }
    }
    """
    data = gql(query, env=env)
    workspace = data.get("workspaceOrError", {})
    return workspace.get("locationEntries", [])


def get_instance_status(env: str | None = None) -> dict:
    """Get a global health check of the Dagster instance. START HERE for any monitoring workflow.

    Returns:
    - healthy: boolean — true only if all required daemons are healthy AND
      no code locations have errors
    - daemons: list of {type, healthy, last_heartbeat, required} for each daemon
      (scheduler, sensor, run coordinator, etc.)
    - queued_runs_count: number of runs waiting in queue (high count = bottleneck)
    - code_location_errors: list of {name, error} for locations that failed to load

    When to use: as the FIRST call in any diagnostic or monitoring flow.
    If healthy=false, check daemons for unhealthy entries and
    code_location_errors for loading failures.
    Follow up with list_code_locations or get_runs as needed.
    """
    query = """
    query InstanceStatus {
      instance {
        daemonHealth {
          allDaemonStatuses {
            daemonType
            required
            healthy
            lastHeartbeatTime
          }
        }
      }
      runsOrError(filter: {statuses: [QUEUED]}, limit: 100) {
        ... on Runs {
          results { runId }
        }
        ... on PythonError { message }
      }
      workspaceOrError {
        ... on Workspace {
          locationEntries {
            name
            loadStatus
            locationOrLoadError {
              ... on PythonError { message }
            }
          }
        }
      }
    }
    """
    data = gql(query, env=env)

    # Daemons
    daemon_statuses = data.get("instance", {}).get("daemonHealth", {}).get("allDaemonStatuses", [])
    daemons = [
        {
            "type": d["daemonType"],
            "healthy": d["healthy"],
            "last_heartbeat": d.get("lastHeartbeatTime"),
            "required": d["required"],
        }
        for d in daemon_statuses
    ]

    # Queued runs
    runs_or_error = data.get("runsOrError", {})
    queued_runs = runs_or_error.get("results", [])
    queued_count = len(queued_runs)

    # Code location errors
    location_entries = data.get("workspaceOrError", {}).get("locationEntries", [])
    code_location_errors = []
    for loc in location_entries:
        err = loc.get("locationOrLoadError", {})
        if "message" in err:
            code_location_errors.append({"name": loc["name"], "error": err["message"]})

    all_required_healthy = all(d["healthy"] for d in daemons if d["required"])
    healthy = all_required_healthy and len(code_location_errors) == 0

    return {
        "healthy": healthy,
        "daemons": daemons,
        "queued_runs_count": queued_count,
        "code_location_errors": code_location_errors,
    }


def reload_code_location(location_name: str, env: str | None = None) -> dict:
    """Reload a code location to pick up new code (e.g. after a deploy).

    - location_name: exact name of the code location (from list_code_locations)

    Returns the new load status. If the location is not found or reload
    is not supported, returns an error message.

    When to use: after deploying new code, or when list_code_locations shows
    a location in an error state. This is equivalent to clicking 'Reload'
    in the Dagster UI.
    """
    query = """
    mutation ReloadLocation($location: String!) {
      reloadRepositoryLocation(repositoryLocationName: $location) {
        ... on WorkspaceLocationEntry {
          name
          loadStatus
          locationOrLoadError {
            ... on RepositoryLocation { name }
            ... on PythonError { message }
          }
        }
        ... on ReloadNotSupported { message }
        ... on RepositoryLocationNotFound { message }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"location": location_name}, env=env)
    return data.get("reloadRepositoryLocation", {})
