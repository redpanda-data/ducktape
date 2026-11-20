# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class TestScheduler(object):
    """This class tracks tests which are scheduled to run, and provides an ordering based on the current cluster state.

    The ordering is "on-demand"; calling next returns the first test in the list which fits in the currently
    available cluster nodes. The list is sorted from the largest cluster user to the smallest, or, when expected
    run times are known, from the longest test to the shortest.

    Starting the longest tests first keeps a long test that needs few nodes from starting last and holding the
    session open after every other test has finished.
    """

    def __init__(self, test_contexts, cluster, expected_run_times=None, logger=None):
        """
        :param expected_run_times: dict of TestContext.run_time_key -> seconds from an earlier run, or None.
            Tests that are missing from it are started first.
        :param logger: logger that is told which tests are missing from expected_run_times, or None.
        """
        self.cluster = cluster
        self._expected_run_times = expected_run_times or {}
        self._logger = logger

        # Track tests which would never be offered up by the scheduling algorithm due to insufficient
        # cluster resources
        self._test_context_list = test_contexts.copy()

        self._sort_test_context_list()

    def __len__(self):
        """Number of tests currently in the scheduler"""
        return len(self._test_context_list)

    def __iter__(self):
        return self

    def filter_unschedulable_tests(self):
        """
        Filter out tests that cannot be scheduled with the current cluster, remove them from
        this scheduler and return them.
        """
        all = self.cluster.all()
        unschedulable = []
        for test_context in self._test_context_list:
            if not all.nodes.can_remove_spec(test_context.expected_cluster_spec):
                unschedulable.append(test_context)
        for u in unschedulable:
            self._test_context_list.remove(u)
        return unschedulable

    def _sort_test_context_list(self):
        """Replace self.test_context_list with a sorted shallow copy

        Tests without an expected run time come first, then the rest from the longest expected run time to the
        shortest. Tests that tie are sorted from the largest cluster user to the smallest, which is the whole
        order when there are no expected run times.

        A test without an expected run time may be a long one. Started first, it cannot be what holds the session
        open at the end.
        """
        run_times = self._expected_run_times
        expected = [(tc, run_times.get(tc.run_time_key)) for tc in self._test_context_list]

        if run_times and self._logger:
            for tc, run_time in expected:
                if run_time is None:
                    self._logger.info("test %s not in historical report, starting it first", tc.test_id)

        expected.sort(key=lambda e: (e[1] is None, e[1] or 0, e[0].expected_num_nodes), reverse=True)
        self._test_context_list = [tc for tc, _ in expected]

    def peek(self):
        """Locate and return the next object to be scheduled, without removing it internally.

        :return test_context for the next test to be scheduled.
            If scheduler is empty, or no test can currently be scheduled, return None.
        """
        for tc in self._test_context_list:
            if self.cluster.available().nodes.can_remove_spec(tc.expected_cluster_spec):
                return tc

        return None

    def remove(self, tc):
        """Remove test context object from this scheduler.
        Intended usage is to peek() first, then perform whatever validity checks,
        and if they pass, remove() it from the scheduler.
        """
        if tc:
            self._test_context_list.remove(tc)
