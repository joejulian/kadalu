"""Pool-wide heal polling without a cluster or external dependencies."""

from functools import partial
import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch


MODULE = Path(__file__).resolve().parents[2] / "kadalu_operator" / "heal.py"
SPEC = importlib.util.spec_from_file_location("storage_heal", MODULE)
heal = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(heal)


def summary(count=3, status="Connected", total=0, pending=0, split=0, healing=0):
    brick = "\n".join((
        f"Status: {status}",
        f"Total Number of entries: {total}",
        f"Number of entries in heal pending: {pending}",
        f"Number of entries in split-brain: {split}",
        f"Number of entries possibly healing: {healing}",
    ))
    return "\n\n".join(brick for _ in range(count))


class HealPollingTests(unittest.TestCase):
    def setUp(self):
        self.now = 100.0
        self.sleeps = []
        self.servers = ["server-test-pool-0-0", "server-test-pool-1-0",
                        "server-test-pool-2-0"]
        clock = patch.object(heal.time, "monotonic", lambda: self.now)
        sleep = patch.object(heal.time, "sleep", self.sleep)
        clock.start()
        sleep.start()
        self.addCleanup(clock.stop)
        self.addCleanup(sleep.stop)

    def sleep(self, duration):
        self.sleeps.append(duration)
        self.now += duration

    def wait(self, query, **kwargs):
        heal.wait_for_storage_heal("test-pool", self.servers,
                                   query=query, **kwargs)

    def test_complete_dirty_summary_runs_once_per_interval(self):
        query = Mock(side_effect=[summary(total=1), summary()])
        self.wait(query)
        self.assertEqual([call.args[0] for call in query.call_args_list],
                         [self.servers[0], self.servers[0]])
        self.assertEqual(self.sleeps, [5])

    def test_failed_query_falls_back_and_reuses_successful_server(self):
        query = Mock(side_effect=[heal.HealQueryError("unavailable"),
                                  summary(pending=1), summary()])
        self.wait(query)
        self.assertEqual([call.args[0] for call in query.call_args_list],
                         [self.servers[0], self.servers[1], self.servers[1]])
        self.assertEqual(self.sleeps, [5])

    def test_incomplete_query_falls_back(self):
        query = Mock(side_effect=[summary(count=2), summary()])
        self.wait(query)
        self.assertEqual([call.args[0] for call in query.call_args_list],
                         self.servers[:2])
        self.assertEqual(self.sleeps, [])

    def test_dirty_pool_times_out_without_extra_queries_or_oversleep(self):
        query = Mock(return_value=summary(split=1))
        with self.assertRaisesRegex(TimeoutError, "zero heal entries"):
            self.wait(query, timeout_seconds=7, poll_interval=5)
        self.assertEqual(query.call_count, 2)
        self.assertEqual(self.sleeps, [5, 2])
        self.assertEqual([call.args[1] for call in query.call_args_list], [7, 2])

    def test_complete_disconnected_summary_cannot_pass(self):
        query = Mock(return_value=summary(status="Disconnected"))
        with self.assertRaises(TimeoutError):
            self.wait(query, timeout_seconds=1)
        self.assertEqual(query.call_count, 1)

    def test_all_failed_queries_retry_after_interval(self):
        query = Mock(side_effect=heal.HealQueryError("unavailable"))
        with self.assertRaises(TimeoutError):
            self.wait(query, timeout_seconds=6)
        self.assertEqual(query.call_count, 6)
        self.assertEqual(self.sleeps, [5, 1])

    def test_only_complete_connected_zero_summaries_are_clean(self):
        self.assertTrue(heal.storage_heal_summary_is_clean(summary(), 3))
        invalid = [None, "", summary(count=2), summary(count=4),
                   summary(status="Disconnected"), summary(total=1),
                   summary(pending=1), summary(split=1), summary(healing=1),
                   summary().replace("possibly healing: 0", "possibly healing: -"),
                   summary().replace("Status: Connected", "Status:"),
                   summary().replace("heal pending: 0", "heal pending: -1")]
        for value in invalid:
            with self.subTest(summary=value):
                self.assertFalse(heal.storage_heal_summary_is_clean(value, 3))

    def test_invalid_polling_arguments_fail_immediately(self):
        for args in ({"server_names": []}, {"timeout_seconds": 0},
                     {"poll_interval": 0}):
            values = {"volname": "test-pool", "server_names": self.servers,
                      "query": Mock()}
            values.update(args)
            with self.subTest(args=args), self.assertRaises(ValueError):
                heal.wait_for_storage_heal(**values)
            values["query"].assert_not_called()

    def test_command_uses_remaining_timeout_and_explicit_container(self):
        with patch.object(heal.subprocess, "run",
                          return_value=Mock(stdout=summary())) as run:
            heal.wait_for_storage_heal("test-pool", self.servers,
                                       timeout_seconds=3.5, query=partial(
                                           heal.query_summary, "test-pool",
                                           namespace="test-namespace"))
        command = run.call_args.args[0]
        self.assertEqual(command[:8], ("/usr/bin/kubectl", "-n", "test-namespace",
                                      "exec", self.servers[0], "-c", "server", "--"))
        self.assertEqual(run.call_args.kwargs["timeout"], 3.5)

    def test_subprocess_timeout_falls_back_to_another_server(self):
        with patch.object(heal.subprocess, "run", side_effect=[
                subprocess.TimeoutExpired("kubectl", 1), Mock(stdout=summary())
        ]) as run:
            heal.wait_for_storage_heal("test-pool", self.servers)
        self.assertEqual(run.call_count, 2)
        self.assertEqual(run.call_args.args[0][4], self.servers[1])


if __name__ == "__main__":
    unittest.main()
