import unittest
from unittest.mock import patch

import dagster_mcp.server as server
from dagster_mcp.tools import get_runs
from dagster_mcp.tools.actions import launch_job, launch_job_with_partitions


class ServerCompositionTests(unittest.TestCase):
    def test_server_reexports_registered_tools(self) -> None:
        self.assertIs(server.get_runs, get_runs)


class LaunchJobWithPartitionsTests(unittest.TestCase):
    partition_sets_response = {
        "partitionSetsOrError": {
            "__typename": "PartitionSets",
            "results": [{"name": "actual_partition_set"}],
        }
    }

    def test_returns_success_payload(self) -> None:
        response = {
            "launchPartitionBackfill": {
                "__typename": "LaunchBackfillSuccess",
                "backfillId": "backfill-1",
                "launchedRunIds": [],
            }
        }

        with patch(
            "dagster_mcp.tools.actions.gql",
            side_effect=[self.partition_sets_response, response],
        ) as gql:
            result = launch_job_with_partitions(
                job_name="my_job",
                repository_location="my_location",
                partition_keys=["2026-07-20"],
                tags={"triggered_by": "test"},
            )

        self.assertEqual(result["backfillId"], "backfill-1")
        self.assertEqual(gql.call_count, 2)
        variables = gql.call_args_list[1].args[1]
        self.assertEqual(
            variables["backfillParams"]["selector"]["partitionSetName"],
            "actual_partition_set",
        )
        self.assertEqual(
            variables["backfillParams"]["tags"],
            [{"key": "triggered_by", "value": "test"}],
        )

    def test_raises_clear_error_for_non_success_result(self) -> None:
        response = {
            "launchPartitionBackfill": {
                "__typename": "PartitionKeysNotFoundError",
                "message": "Partition keys `['2026-07-20']` could not be found.",
            }
        }

        with patch(
            "dagster_mcp.tools.actions.gql",
            side_effect=[self.partition_sets_response, response],
        ):
            with self.assertRaisesRegex(RuntimeError, "PartitionKeysNotFoundError"):
                launch_job_with_partitions(
                    job_name="my_job",
                    repository_location="my_location",
                    partition_keys=["2026-07-20"],
                )

    def test_raises_when_success_payload_has_no_backfill_id(self) -> None:
        response = {
            "launchPartitionBackfill": {
                "__typename": "LaunchBackfillSuccess",
                "launchedRunIds": [],
            }
        }

        with patch(
            "dagster_mcp.tools.actions.gql",
            side_effect=[self.partition_sets_response, response],
        ):
            with self.assertRaisesRegex(
                RuntimeError, "LaunchBackfillSuccess.*launchedRunIds"
            ):
                launch_job_with_partitions(
                    job_name="my_job",
                    repository_location="my_location",
                    partition_keys=["2026-07-20"],
                )

    def test_raises_clear_error_when_partition_set_lookup_fails(self) -> None:
        response = {
            "partitionSetsOrError": {
                "__typename": "PipelineNotFoundError",
                "message": "Could not find job my_job",
            }
        }

        with patch("dagster_mcp.tools.actions.gql", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "PipelineNotFoundError"):
                launch_job_with_partitions(
                    job_name="my_job",
                    repository_location="my_location",
                    partition_keys=["2026-07-20"],
                )

    def test_explicit_partition_set_name_skips_lookup(self) -> None:
        response = {
            "launchPartitionBackfill": {
                "__typename": "LaunchBackfillSuccess",
                "backfillId": "backfill-1",
                "launchedRunIds": [],
            }
        }

        with patch("dagster_mcp.tools.actions.gql", return_value=response) as gql:
            launch_job_with_partitions(
                job_name="my_job",
                repository_location="my_location",
                partition_keys=["2026-07-20"],
                partition_set_name="explicit_partition_set",
            )

        self.assertEqual(gql.call_count, 1)
        variables = gql.call_args.args[1]
        self.assertEqual(
            variables["backfillParams"]["selector"]["partitionSetName"],
            "explicit_partition_set",
        )


class LaunchJobWithPartitionsRunConfigTests(unittest.TestCase):
    def test_launches_backfill_with_config_and_partitions(self) -> None:
        response = {
            "launchPartitionBackfill": {
                "__typename": "LaunchBackfillSuccess",
                "backfillId": "backfill-1",
                "launchedRunIds": [],
            }
        }
        run_config = {"ops": {"clean": {"config": {"dry_run": False, "approved": True}}}}

        with patch(
            "dagster_mcp.tools.actions.gql",
            side_effect=[LaunchJobWithPartitionsTests.partition_sets_response, response],
        ) as gql:
            result = launch_job_with_partitions(
                job_name="clean_job",
                repository_location="my_location",
                partition_keys=["2026-07-20", "2026-07-21"],
                run_config=run_config,
                tags={"triggered_by": "test"},
            )

        self.assertEqual(result["backfillId"], "backfill-1")
        self.assertEqual(gql.call_count, 2)

        variables = gql.call_args_list[1].args[1]
        self.assertEqual(variables["backfillParams"]["runConfigData"], run_config)
        self.assertEqual(
            variables["backfillParams"]["partitionNames"],
            ["2026-07-20", "2026-07-21"],
        )
        self.assertEqual(
            variables["backfillParams"]["selector"]["partitionSetName"],
            "actual_partition_set",
        )

    def test_moves_misplaced_run_config_tags_to_backfill_tags(self) -> None:
        response = {
            "launchPartitionBackfill": {
                "__typename": "LaunchBackfillSuccess",
                "backfillId": "backfill-1",
                "launchedRunIds": [],
            }
        }
        run_config = {
            "ops": {"clean": {"config": {"dry_run": False}}},
            "tags": {"triggered_by": "run_config"},
        }

        with patch(
            "dagster_mcp.tools.actions.gql",
            side_effect=[LaunchJobWithPartitionsTests.partition_sets_response, response],
        ) as gql:
            launch_job_with_partitions(
                job_name="clean_job",
                repository_location="my_location",
                partition_keys=["2026-07-20"],
                run_config=run_config,
                tags={"priority": "high"},
            )

        variables = gql.call_args_list[1].args[1]
        self.assertEqual(
            variables["backfillParams"]["runConfigData"],
            {"ops": {"clean": {"config": {"dry_run": False}}}},
        )
        self.assertEqual(
            variables["backfillParams"]["tags"],
            [
                {"key": "triggered_by", "value": "run_config"},
                {"key": "priority", "value": "high"},
            ],
        )

    def test_rejects_conflicting_misplaced_run_config_tags(self) -> None:
        with patch("dagster_mcp.tools.actions.gql") as gql:
            with self.assertRaisesRegex(ValueError, "tags conflict"):
                launch_job_with_partitions(
                    job_name="clean_job",
                    repository_location="my_location",
                    partition_keys=["2026-07-20"],
                    run_config={"tags": {"priority": "low"}},
                    tags={"priority": "high"},
                )

        gql.assert_not_called()


class LaunchJobRunConfigTests(unittest.TestCase):
    def test_moves_misplaced_run_config_tags_to_execution_metadata(self) -> None:
        response = {
            "launchRun": {
                "__typename": "LaunchRunSuccess",
                "run": {"runId": "run-1", "status": "QUEUED"},
            }
        }

        with patch("dagster_mcp.tools.actions.gql", return_value=response) as gql:
            launch_job(
                job_name="clean_job",
                repository_location="my_location",
                run_config={"ops": {}, "tags": {"triggered_by": "run_config"}},
            )

        variables = gql.call_args.args[1]
        self.assertEqual(variables["runConfigData"], {"ops": {}})
        self.assertEqual(
            variables["executionMetadata"],
            {"tags": [{"key": "triggered_by", "value": "run_config"}]},
        )


if __name__ == "__main__":
    unittest.main()
