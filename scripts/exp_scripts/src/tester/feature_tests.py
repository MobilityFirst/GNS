"""Includes basic feature tests of GNS system. The different classes in this module run tests
either on a local machine or on a set of remote distributed machines. Further, the some classes can run GNS in
an un-replicated mode where there is only one active replica and one replica controller for each name.

Limitations:
1. If all tests are run in succession on a local machine, tests start to fail after running 5-10 tests.
2. We do not check the correctness of the values returned by GNS. Fake, incorrect responses will also pass this test.
"""


import unittest
import ConfigParser
import os
import sys
import inspect

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

from workload.write_workload import get_trace_filename, RequestType, WorkloadParams, ExpType
import local.exp_config
import local.generate_config_file
from test_utils import *
from local_setup import TestSetupLocal
from remote_setup import TestSetupRemote

__author__ = 'abhigyan'


class FeatureTestLocal(TestSetupLocal):
    """This is the baseline test which GNS is expected to pass under local tests. It tests several features
    of GNS for a 3 name server and 1 local name server  setup. It does not measure throughput of GNS"""

    special_char_set = '!@#$%^&*()-=_+{}|[]\\;\':",.<>?`~'

    unsupported_special_chars = '/'

    lns = 1

    def test_a_1name(self):
        """Test add, remove, lookup, and delete operations for a single name"""

        name = 'test_name'
        delay_ms = 2500
        requests = [[name, RequestType.ADD], [delay_ms, RequestType.DELAY],
                    [name, RequestType.LOOKUP],
                    [name, RequestType.UPDATE],
                    [name, RequestType.LOOKUP], [delay_ms, RequestType.DELAY],
                    [name, RequestType.REMOVE]]

        exp_duration_sec = 10 + 2 * delay_ms / 1000
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(exp_duration_sec))
        output_stats = self.run_exp(requests)

        self.assertEqual(output_stats.requests, 5, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 5, "Successful requests mismatch")
        self.assertEqual(output_stats.read, 2, "Successful reads mismatch")
        self.assertEqual(output_stats.write, 1, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 1, "Successful removes mismatch")

    def test_b_1name_n_times(self):
        """Repeat N times the test for add, remove, lookup, and delete operations for a single name"""
        n = 5

        name = 'test_name'
        delay_ms = 3000
        requests = [[name, RequestType.ADD], [delay_ms, RequestType.DELAY],
                    [name, RequestType.LOOKUP],
                    [name, RequestType.UPDATE],
                    [name, RequestType.LOOKUP], [delay_ms, RequestType.DELAY],
                    [name, RequestType.REMOVE], [delay_ms, RequestType.DELAY]]
        request_n_times = []
        for i in range(n):
            request_n_times.extend(requests)
        exp_duration = int(n * (1 + 3*delay_ms/1000))

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(exp_duration))  # in seconds
        output_stats = self.run_exp(request_n_times)

        self.assertEqual(output_stats.requests, 5 * n, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 5 * n, "Successful requests mismatch")
        self.assertEqual(output_stats.read, 2 * n, "Successful reads mismatch")
        self.assertEqual(output_stats.write, 1 * n, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1 * n, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 1 * n, "Successful removes mismatch")

    def test_c0_1name_restart(self):
        """Add a name to GNS, restart GNS, and test if we can perform requests for the name.
        This tests if log recovery works correctly."""

        name = 'test_name'

        # run first experiment
        delay_ms = 2500
        requests = [[name, RequestType.ADD], [delay_ms, RequestType.DELAY],
                    [name, RequestType.LOOKUP],
                    [name, RequestType.UPDATE]]
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(5))  # in seconds
        output_stats = self.run_exp(requests)

        self.assertEqual(output_stats.requests, 3, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 3, "Successful requests mismatch")
        self.assertEqual(output_stats.read, 1, "Successful reads mismatch")
        self.assertEqual(output_stats.write, 1, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 0, "Successful removes mismatch")

        # run second experiment: restart GNS without deleting state
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'clean_start', False)  # in seconds
        # sleep after starting NS to give ns time to recover
        requests = [[name, RequestType.LOOKUP],
                    [name, RequestType.UPDATE], [delay_ms, RequestType.DELAY],
                    [name, RequestType.REMOVE]]
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(5))  # in seconds
        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, 3, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 3, "Successful requests mismatch")
        self.assertEqual(output_stats.read, 1, "Successful reads mismatch")
        self.assertEqual(output_stats.write, 1, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 0, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 1, "Successful removes mismatch")

    def test_c1_1name_remove_restart(self):
        """Add and remove a name to GNS, restart GNS, and test if we can again add and remove it.
        This check if name is cleanly removed on log recovery as well.."""
        self.test_a_1name()
        # self.config_parse.set(ConfigParser.DEFAULTSECT, 'ns_sleep', str(5))  # in seconds
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'clean_start', False)
        self.test_a_1name()

    def test_c2_name_special_characters(self):
        """ Test if we can handle special characters in name field"""

        delay_ms = 2500
        requests = []
        for ch in self.special_char_set:
            requests.append([self.get_special_character_name(ch), RequestType.ADD])
        requests.append([delay_ms, RequestType.DELAY])
        for ch in self.special_char_set:
            requests.append([self.get_special_character_name(ch), RequestType.LOOKUP])
            requests.append([self.get_special_character_name(ch), RequestType.UPDATE])
            requests.append([self.get_special_character_name(ch), RequestType.LOOKUP])
            requests.append([self.get_special_character_name(ch), RequestType.UPDATE])
        requests.append([delay_ms, RequestType.DELAY])
        for ch in self.special_char_set:
            requests.append([self.get_special_character_name(ch), RequestType.REMOVE])

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', 20)  # in seconds
        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, 6 * len(self.special_char_set), "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 6 * len(self.special_char_set), "Successful requests mismatch")
        self.assertEqual(output_stats.read, 2 * len(self.special_char_set), "Successful reads mismatch")
        self.assertEqual(output_stats.write, 2 * len(self.special_char_set), "Successful writes mismatch")
        self.assertEqual(output_stats.add, len(self.special_char_set), "Successful adds mismatch")
        self.assertEqual(output_stats.remove, len(self.special_char_set), "Successful removes mismatch")

    def test_c3_name_special_characters_restart(self):
        """ Test if log recovery works correctly for special characters"""

        # add names with special characters and do some write operations for each name
        delay_ms = 2500
        requests = []
        for ch in self.special_char_set:
            requests.append([self.get_special_character_name(ch), RequestType.ADD])
        requests.append([delay_ms, RequestType.DELAY])
        for ch in self.special_char_set:
            requests.append([self.get_special_character_name(ch), RequestType.UPDATE])
            requests.append([self.get_special_character_name(ch), RequestType.UPDATE])
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', 10 + delay_ms/1000)  # in seconds
        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.add, len(self.special_char_set), "Successful adds mismatch")
        self.assertEqual(output_stats.write, 2 * len(self.special_char_set), "Successful writes mismatch")

        # now restart nodes and do some more operations
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'clean_start', False)
        requests = []
        for ch in self.special_char_set:
            requests.append([self.get_special_character_name(ch), RequestType.LOOKUP])
            requests.append([self.get_special_character_name(ch), RequestType.UPDATE])
            requests.append([self.get_special_character_name(ch), RequestType.LOOKUP])
            requests.append([self.get_special_character_name(ch), RequestType.UPDATE])
        requests.append([delay_ms, RequestType.DELAY])
        for ch in self.special_char_set:
            requests.append([self.get_special_character_name(ch), RequestType.REMOVE])

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', 20)  # in seconds
        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, 5 * len(self.special_char_set), "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 5 * len(self.special_char_set), "Successful requests mismatch")
        self.assertEqual(output_stats.read, 2 * len(self.special_char_set), "Successful reads mismatch")
        self.assertEqual(output_stats.write, 2 * len(self.special_char_set), "Successful writes mismatch")
        self.assertEqual(output_stats.add, 0, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, len(self.special_char_set), "Successful removes mismatch")

    @staticmethod
    def get_special_character_name(ch):
        return 'test' + ch + 'name'

    def test_d0_1name_read_write_latency(self):
        """For 1 name, measure read and write latencies on a local machine"""
        request_rate = 50
        duration = 100
        output_stats = self.run_exp_reads_writes(request_rate, duration)
        print 'Read latency (90-percentile)', output_stats.read_perc90, 'ms', 'Write latency (90-percentile)', \
            output_stats.write_perc90, 'ms',
        self.assertLess(output_stats.read_perc90, 20.0, "High latency reads")
        self.assertLess(output_stats.write_perc90, 50.0, "High latency writes")

    def test_d1_medium_objects_read_write_latency(self):
        """For objects of medium size (1K, 10K, 100K), check if we can perform requests"""
        medium_sizes_kb = [1, 10, 100]  #
        for size in medium_sizes_kb:
            print 'Testing object size: ', size, 'KB'
            self.workload_conf.set(ConfigParser.DEFAULTSECT, WorkloadParams.OBJECT_SIZE, size)
            self.test_a_1name()

    @unittest.expectedFailure
    def test_d2_large_objects_test(self):
        """Stress test to check if we can handle large object of several MBs. This test is expected to fail when the
        response latency becomes greater than the max wait time for a request at LNS (usually 10 sec) """
        large_sizes_kb = [1000, 2000, 4000, 8000, 16000, 32000]  #
        for size in large_sizes_kb:
            print 'Testing object size: ', size/1000, 'MB'
            self.config_parse.set(ConfigParser.DEFAULTSECT, "is_debug_mode", False)
            self.workload_conf.set(ConfigParser.DEFAULTSECT, WorkloadParams.OBJECT_SIZE, size)
            self.test_a_1name()

    def test_e_1name_read_write_emulated_latency(self):
        """For 1 name, measure read and write latencies while emulating wide-area latency between nodes"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'emulate_ping_latencies', True)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'emulation_type', local.exp_config.CONSTANT_DELAY)
        request_rate = 20
        duration = 30
        output_stats = self.run_exp_reads_writes(request_rate, duration)
        print 'Read latency (90-percentile)', output_stats.read_perc90, 'ms', 'Write latency (90-percentile)', \
            output_stats.write_perc90, 'ms',
        self.assertLess(output_stats.read_perc90, 2 * local.exp_config.DEFAULT_CONST_DELAY, "High latency reads")
        self.assertLess(output_stats.write_perc90, 5 * local.exp_config.DEFAULT_CONST_DELAY, "High latency writes")

    def test_f_single_node_failure(self):
        """Fail all name servers one by one and check if requests are successful"""
        if self.ns < 3:
            return
        print
        for i in range(self.ns):
            print 'Testing failure of node', i
            # failure will be detected in 3 seconds
            self.config_parse.set(ConfigParser.DEFAULTSECT, 'failure_detection_msg_interval', 1)
            self.config_parse.set(ConfigParser.DEFAULTSECT, 'failure_detection_timeout_interval', 3)
            self.config_parse.set(ConfigParser.DEFAULTSECT, 'ns_sleep', 5)  # wait for 5 sec after running ns so that
            self.config_parse.set(ConfigParser.DEFAULTSECT, 'failed_nodes', i)
            self.test_a_1name()

    def test_g_invalid_request_failure(self):
        """Is failure returning quickly for invalid requests, e.g., reads for non-existent names"""
        # do we log failure quickly?
        requests = []
        len_name = 20
        req_rate = 100
        num_requests = 1000
        name = 'test_name'
        requests.append([name, RequestType.ADD])  # add a name
        delay = 2000
        requests.append([delay, RequestType.DELAY])
        requests.append([req_rate, RequestType.RATE])
        #  the requests below are all expected to fail, e.g., add for an existing name; lookup, update, and remove for
        # non-existing names.
        for i in range(num_requests):
            requests.extend([[name, RequestType.ADD],  # adding same name repeatedly will fail
                             [gen_random_string(len_name), RequestType.LOOKUP],
                             [gen_random_string(len_name), RequestType.UPDATE],
                             [gen_random_string(len_name), RequestType.REMOVE]])
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', len(requests)/req_rate)

        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, 4*num_requests + 1, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 1, "Successful requests mismatch")
        self.assertEqual(output_stats.read_failed, num_requests, "Failed reads mismatch")
        self.assertEqual(output_stats.write_failed, num_requests, "Failed writes mismatch")
        self.assertEqual(output_stats.add_failed, num_requests, "Failed adds mismatch")
        self.assertEqual(output_stats.remove_failed, num_requests, "Failed removes mismatch")
        self.assertLess(output_stats.failed_read_perc90, 100, "High latency failed reads")
        self.assertLess(output_stats.failed_write_perc90, 100, "High latency failed writes")
        self.assertLess(output_stats.failed_add_perc90, 100, "High latency failed adds")
        self.assertLess(output_stats.failed_remove_perc90, 100, "High latency failed removes")

    def test_h_group_change_1name(self):
        """ Change the group of replicas of a name N times and check if requests are successful after each change"""
        name = 'test_name'
        delay_ms = 2500

        initial_version = 1
        group_size = self.get_group_size()

        assert group_size <= self.ns
        num_group_changes = 3

        requests = [[name, RequestType.ADD], [delay_ms, RequestType.DELAY]]
        for i in range(num_group_changes):
            version = (i + 1) + initial_version
            group = get_new_group_members_str(range(self.ns), group_size)
            grp_change_request = [name, RequestType.GROUP_CHANGE, version, group]
            # print 'New group members:', group, ' Size:', group_size
            requests.extend([grp_change_request,
                             [delay_ms, RequestType.DELAY],
                             [name, RequestType.UPDATE],
                             [name, RequestType.LOOKUP],
                             [name, RequestType.UPDATE],
                             [name, RequestType.LOOKUP],
                             [delay_ms, RequestType.DELAY]])

        exp_duration_sec = 10 + delay_ms * (num_group_changes * 2) / 1000

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(exp_duration_sec))
        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, 1 + 5 * num_group_changes, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 1 + 5 * num_group_changes, "Successful requests mismatch")
        self.assertEqual(output_stats.read, 2 * num_group_changes, "Successful reads mismatch")
        self.assertEqual(output_stats.write, 2 * num_group_changes, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 0, "Successful removes mismatch")
        self.assertEqual(output_stats.group_change, num_group_changes, "Successful group change mismatch")

    def test_i_group_change_1name_with_restart(self):
        """ Do multiple group changes for a name, and restart the system. Are requests for that name successful?"""
        # add a name and change group a few times
        self.test_h_group_change_1name()
        # restart gns and recover from existing logs
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'clean_start', False)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'ns_sleep', 5)
        # test if requests are successful
        name = 'test_name'
        delay = 5000
        requests = [[name, RequestType.LOOKUP],
                    [name, RequestType.UPDATE],
                    [delay, RequestType.DELAY],
                    [name, RequestType.REMOVE]]

        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, 3, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 3, "Successful requests mismatch")
        self.assertEqual(output_stats.read, 1, "Successful reads mismatch")
        self.assertEqual(output_stats.write, 1, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 0, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 1, "Successful removes mismatch")

    def test_j0_ttl_caching(self):
        """ Test if zero TTLs see non-cached responses and non-zero TTLs see mostly cached responses.
        """

        name = 'test_name'
        delay_ms = 2500
        requests = [[name, RequestType.ADD], [delay_ms, RequestType.DELAY]]
        req_rate = 50
        requests.append([req_rate, RequestType.RATE])
        num_lookups = 1000
        requests.extend([[name, RequestType.LOOKUP]]*num_lookups)

        exp_duration_sec = num_lookups/req_rate + delay_ms/1000
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', exp_duration_sec)

        # with zero TTL, no reads return a cached response.
        # self.workload_conf.set(ConfigParser.DEFAULTSECT, 'ttl', 0)
        #
        # output_stats = self.run_exp(requests)
        # self.assertEqual(output_stats.requests, num_lookups + 1, "Total number of requests mismatch")
        # self.assertEqual(output_stats.success, num_lookups + 1, "Successful requests mismatch")
        # self.assertEqual(output_stats.read, num_lookups, "Successful reads mismatch")
        # self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        # self.assertEqual(output_stats.read_cached, 0, "Successful cached reads mismatch")

        # with TTL > test duration, almost all reads return a cached response.
        self.workload_conf.set(ConfigParser.DEFAULTSECT, 'ttl', 100)

        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, num_lookups + 1, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, num_lookups + 1, "Successful requests mismatch")
        self.assertEqual(output_stats.read, num_lookups, "Successful reads mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        # almost all lookups will be cached
        self.assertGreater(output_stats.read_cached, 0.9*num_lookups, "Successful cached reads mismatch")

    def test_j1_ttl_caching_group_change(self):
        """ Test if TTL values are correctly transferred on a group change of replicas of a name."""

        name = 'test_name'
        delay_ms = 2500
        initial_version = 1
        group_size = min(5, self.ns)
        requests = [[name, RequestType.ADD], [delay_ms, RequestType.DELAY]]
        version = initial_version + 1
        grp_change_request = [name, RequestType.GROUP_CHANGE, version,
                              get_new_group_members_str(range(self.ns), group_size)]
        requests.extend([grp_change_request, [delay_ms, RequestType.DELAY]])
        num_lookups = 1000
        requests.extend([[name, RequestType.LOOKUP]]*num_lookups)

        exp_duration_sec = 10 + 2*delay_ms/1000
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', exp_duration_sec)

        # with zero TTL, no reads return a cached response.
        self.workload_conf.set(ConfigParser.DEFAULTSECT, 'ttl', 0)

        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, num_lookups + 2, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, num_lookups + 2, "Successful requests mismatch")
        self.assertEqual(output_stats.read, num_lookups, "Successful reads mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.read_cached, 0, "Successful cached reads mismatch")
        self.assertEqual(output_stats.group_change, 1, "Successful group change mismatch")

        # with TTL > test duration, almost all reads return a cached response.
        self.workload_conf.set(ConfigParser.DEFAULTSECT, 'ttl', 100)

        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, num_lookups + 2, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, num_lookups + 2, "Successful requests mismatch")
        self.assertEqual(output_stats.read, num_lookups, "Successful reads mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        # almost all lookups will be cached
        self.assertGreater(output_stats.read_cached, 0.9*num_lookups, "Successful cached reads mismatch")
        self.assertEqual(output_stats.group_change, 1, "Successful group change mismatch")

    def test_j2_ttl_caching_restarts(self):
        """ Are TTL values recovered correctly on restarting the system?"""

        name = 'test_name'

        # with zero TTL, no reads return a cached response.
        self.workload_conf.set(ConfigParser.DEFAULTSECT, 'ttl', 0)

        # step 1: start GNS and add a name
        requests = [[name, RequestType.ADD]]
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', 3)
        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")

        # step 2: restart system and send lookups. these should not be cached
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'clean_start', False)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'ns_sleep', 5)  # wait for 5 sec after running ns so that
        num_lookups = 1000
        requests = ([[name, RequestType.LOOKUP]]*num_lookups)
        exp_duration_sec = 10
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', exp_duration_sec)

        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, num_lookups, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, num_lookups, "Successful requests mismatch")
        self.assertEqual(output_stats.read, num_lookups, "Successful reads mismatch")
        self.assertEqual(output_stats.read_cached, 0, "Successful cached reads mismatch")

        # with TTL > test duration, almost all reads return a cached response.
        self.workload_conf.set(ConfigParser.DEFAULTSECT, 'ttl', 100)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'clean_start', True)  # IMP: start with fresh system to ADD name

        # step 1: start GNS and add a name
        requests = [[name, RequestType.ADD]]
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', 3)
        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")

        # step 2: restart system and send lookups. almost all should be cached
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'clean_start', False)
        num_lookups = 1000
        requests = ([[name, RequestType.LOOKUP]]*num_lookups)
        exp_duration_sec = 10
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', exp_duration_sec)

        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, num_lookups, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, num_lookups, "Successful requests mismatch")
        self.assertEqual(output_stats.read, num_lookups, "Successful reads mismatch")
        self.assertGreater(output_stats.read_cached, 0.9*num_lookups, "Successful cached reads mismatch")

    def test_k_random_node_ids(self):
        """ Test if four basic operations (add, remove, lookup, update) and group changes work with random nodeIDs
        for name servers."""
        random_seed = 12345
        ns_ids, lns_ids = local.generate_config_file.generate_node_ids(self.ns, 1, random_seed)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'random_node_ids', random_seed)
        trace_folder = os.path.join(self.gns_folder, local.exp_config.DEFAULT_WORKING_DIR, 'trace')
        self.trace_filename = get_trace_filename(trace_folder, lns_ids[0])
        name = 'test_name'
        delay_ms = 2500

        initial_version = 1
        if self.config_parse.has_option(ConfigParser.DEFAULTSECT, 'primary_name_server') and \
           int(self.config_parse.get(ConfigParser.DEFAULTSECT, 'primary_name_server')) == 1:
            group_size = 1
        else:
            group_size = min(5, self.ns)
        assert group_size <= self.ns

        requests = [[name, RequestType.ADD], [delay_ms, RequestType.DELAY]]
        version = initial_version + 1
        group = get_new_group_members_str(ns_ids, group_size)
        print "New group of active replicas:", group
        grp_change_request = [name, RequestType.GROUP_CHANGE, version, group]
        requests.extend([grp_change_request,
                         [delay_ms, RequestType.DELAY],
                         [name, RequestType.UPDATE],
                         [name, RequestType.LOOKUP],
                         [name, RequestType.UPDATE],
                         [name, RequestType.LOOKUP],
                         [delay_ms, RequestType.DELAY],
                         [name, RequestType.REMOVE]])

        exp_duration_sec = 10 + delay_ms*2 / 1000

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(exp_duration_sec))
        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, 6, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 6, "Successful requests mismatch")
        self.assertEqual(output_stats.read, 2, "Successful reads mismatch")
        self.assertEqual(output_stats.write, 2, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 1, "Successful removes mismatch")

    def test_l_dynamic_replication(self):
        """ Test dynamic replication in GNS based on read rate, write rate, and geo-distribution of LNS"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, "replication_interval", str(10))
        request_rate = 1000
        duration = 50
        self.run_exp_reads_writes(request_rate, duration)

    @unittest.expectedFailure
    def test_m_static_replication(self):
        """ Tests if static replication works in GNS. In this case, replicas of a name are co-located with replica
        controllers and never change their location"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, "scheme", "static")
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', str(self.ns))
        self.test_a_1name()

    def test_n_read_writes_groupchanges(self):
        """ Tests a workload where reads and writes for 1 name are inter-mixed with group changes"""
        request_rate = 50
        duration = 50
        group_change_interval = 10
        self.run_exp_reads_writes_groupchanges(request_rate, duration, group_change_interval)

    def run_exp_reads_writes(self, request_rate, exp_duration):
        """Runs an experiment with equal number of reads and writes for a name at a given request rate.
        It also checks if all requests are successful."""

        name = 'test_name'
        n = int(request_rate * exp_duration)
        requests = [[name, RequestType.ADD]]
        delay = 5000  # ms
        requests.append([delay, RequestType.DELAY])  # wait after an add request to ensure name is added
        requests.append([request_rate*2, RequestType.RATE])
        for i in range(n):
            requests.append([name, RequestType.LOOKUP])
            requests.append([name, RequestType.UPDATE])
        # wait before sending remove to ensure all previous requests are complete
        delay = 5000  # ms
        requests.append([delay, RequestType.DELAY])
        requests.append([name, RequestType.REMOVE])

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', exp_duration + delay/1000*2)  # in sec

        output_stats = self.run_exp(requests)

        self.assertEqual(output_stats.requests, 2 + n * 2, "Total number of requests mismatch")
        # some read and write can fail at start of test while name is being added
        self.assertEqual(output_stats.success, 2 + n * 2, "Successful requests mismatch")
        self.assertEqual(output_stats.read, n, "Successful reads mismatch")
        self.assertEqual(output_stats.write, n, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 1, "Successful removes mismatch")
        return output_stats

    def run_exp_reads_writes_groupchanges(self, request_rate, exp_duration, group_change_interval):
        """Runs an experiment with equal number of reads and writes for a name at a given request rate.
        It also checks if all requests are successful."""

        name = 'test_name'
        assert group_change_interval <= exp_duration
        num_group_changes = int(exp_duration/group_change_interval)
        n = int(request_rate * group_change_interval)
        initial_version = 1
        requests = [[name, RequestType.ADD]]
        group_size = self.get_group_size()
        delay = 5000  # ms
        exp_duration += delay/1000
        requests.append([delay, RequestType.DELAY])  # wait after an add request to ensure name is added
        requests.append([request_rate*2, RequestType.RATE])

        for j in range(num_group_changes):
            for i in range(n):
                requests.append([name, RequestType.LOOKUP])
                requests.append([name, RequestType.UPDATE])
            # add a group change request
            version = (j + 1) + initial_version
            group = get_new_group_members_str(range(self.ns), group_size)
            grp_change_request = [name, RequestType.GROUP_CHANGE, version, group]
            requests.append(grp_change_request)
        # wait before sending remove to ensure all previous requests are complete
        delay = 5000
        requests.append([delay, RequestType.DELAY])
        exp_duration += delay/1000
        requests.append([name, RequestType.REMOVE])

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', exp_duration)  # in seconds
        output_stats = self.run_exp(requests)

        self.assertEqual(output_stats.requests, 2 + (2*n + 1) * num_group_changes, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 2 + (2*n + 1) * num_group_changes, "Successful requests mismatch")
        self.assertEqual(output_stats.read, n*num_group_changes, "Successful reads mismatch")
        self.assertEqual(output_stats.write, n*num_group_changes, "Successful writes mismatch")
        self.assertEqual(output_stats.group_change, num_group_changes, "Successful group change mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 1, "Successful removes mismatch")
        return output_stats

    def get_group_size(self):
        if self.config_parse.has_option(ConfigParser.DEFAULTSECT, 'primary_name_server') and \
           int(self.config_parse.get(ConfigParser.DEFAULTSECT, 'primary_name_server')) == 1:
            group_size = 1
        else:
            group_size = min(5, self.ns)
        return group_size


class FeatureTestDistributed(TestSetupRemote, FeatureTestLocal):
    """ Runs local_tests.FeatureTestMultiNode in a distributed setup. The setup tasks are performed by
    remote_setup.TestSetupRemote and test cases are provided by FeatureTestMultiNode"""
    pass


class FeatureTestUnreplicatedLocal(FeatureTestLocal):
    """On local machine, tests a GNS in which both name records and replica controllers are unreplicated"""

    def setUp(self):
        FeatureTestLocal.setUp(self)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', str(1))


class FeatureTestUnreplicatedDistributed(FeatureTestDistributed):
    """On local machine, tests a GNS in which both name records and replica controllers are unreplicated"""

    def setUp(self):
        FeatureTestDistributed.setUp(self)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', str(1))


class ConnectTimeMeasureTest(TestSetupLocal):
    """Test setup to measure time-to-connect, i.e., time to obtain the correct value of a name"""

    def test_a_connect_time_test(self):
        """ Measures connect time for a name."""
        self.lns = 2
        self.ns = 3
        self.workload_conf.set(ConfigParser.DEFAULTSECT, WorkloadParams.EXP_TYPE,  ExpType.CONNECT_TIME)
        exp_duration = 600
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(exp_duration))
        mobile_update_intervals = [1, 10, 100, 1000, 10000]
        for mui in mobile_update_intervals:
            self.workload_conf.set(ConfigParser.DEFAULTSECT, WorkloadParams.MOBILE_UPDATE_INTERVAL, str(mui))
            print 'Running experiment with update = ', mui
            output_dir = 'connect_time/update_' + str(mui)
            self.exp_output_folder = os.path.join(self.local_output_folder, output_dir)
            print 'Exp outupt folder = ', self.exp_output_folder
            self.run_exp_multi_lns()


class ConnectTimeMeasureTestDistributed(TestSetupRemote, ConnectTimeMeasureTest):
    """Runs connect time measurement in a distributed setup """
