import unittest
from unittest.mock import patch

import dagster_mcp.server as server
from dagster_mcp.tools import get_runs
from dagster_mcp.tools.actions import launch_job_with_partitions


class ServerCompositionTests(unittest.TestCase):
    def test_server_reexports_registered_tools(self) -> None:
        self.assertIs(server.get_runs, get_runs)


class LaunchJobWithPartitionsTests(unittest.TestCase):
    def test_returns_success_payload(self) -> None:
        response = {
            "launchPartitionBackfill": {
                "__typename": "LaunchBackfillSuccess",
                "backfillId": "backfill-1",
                "launchedRunIds": [],
            }
        }

        with patch("dagster_mcp.tools.actions.gql", return_value=response) as gql:
            result = launch_job_with_partitions(
                job_name="my_job",
                repository_location="my_location",
                partition_keys=["2026-07-20"],
                tags={"triggered_by": "test"},
            )

        self.assertEqual(result["backfillId"], "backfill-1")
        variables = gql.call_args.args[1]
        self.assertEqual(
            variables["backfillParams"]["selector"]["partitionSetName"],
            "my_job_partition_set",
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

        with patch("dagster_mcp.tools.actions.gql", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "PartitionKeysNotFoundError"):
                launch_job_with_partitions(
                    job_name="my_job",
                    repository_location="my_location",
                    partition_keys=["2026-07-20"],
                )


if __name__ == "__main__":
    unittest.main()
