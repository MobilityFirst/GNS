import unittest
import ConfigParser
import os
import sys
import inspect
import random

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

from workload.write_workload import get_trace_filename, RequestType
from util.write_utils import write_tuple_array
from logparse.read_final_stats import FinalStats
import local.exp_config


class Test3NodeLocal(unittest.TestCase):
    """
    Tests a 3 name server and 1 local name server GNS; all records are replicated on all three name servers.
    """
    ns = 3

    def setUp(self):
        self.config_file = os.path.join(parent_folder, 'resources', 'local_test_env.ini')
        self.config_parse = ConfigParser.ConfigParser()
        self.config_parse.optionxform=str
        self.config_parse.read(self.config_file)
        self.gns_folder = self.config_parse.get(ConfigParser.DEFAULTSECT, 'gns_folder')

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'num_ns', self.ns)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'num_lns', 1)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'is_experiment_mode', True)
        trace_folder = os.path.join(self.gns_folder, 'trace')
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'trace_folder', trace_folder)
        self.trace_filename = get_trace_filename(trace_folder, self.ns)

    def test_a_1name(self):
        """Test add, remove, lookup, and delete operations for a single name"""

        name = 'test_name'
        delay_ms = 2500
        requests = [[name, RequestType.ADD], [delay_ms, RequestType.DELAY],
                [name, RequestType.LOOKUP],
                [name, RequestType.UPDATE],
                [name, RequestType.LOOKUP], [delay_ms, RequestType.DELAY],
                [name, RequestType.REMOVE]]
        exp_duration_sec = 10 + 2*delay_ms/1000
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
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(n*4))  # n seconds
        name = 'test_name'
        requests = [[name, RequestType.ADD],
                    [name, RequestType.LOOKUP],
                    [name, RequestType.UPDATE],
                    [name, RequestType.LOOKUP],
                    [name, RequestType.REMOVE]]
        request_n_times = []
        for i in range(n):
            request_n_times.extend(requests)

        output_stats = self.run_exp(request_n_times)

        self.assertEqual(output_stats.requests, 5*n, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 5*n, "Successful requests mismatch")
        self.assertEqual(output_stats.read, 2*n, "Successful reads mismatch")
        self.assertEqual(output_stats.write, 1*n, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1*n, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 1*n, "Successful removes mismatch")

    def test_c_1name_restart(self):
        """Add a name to GNS, restart GNS, and test if we can perform requests for the name"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(5))  # n seconds
        name = 'test_name'

        # run first experiment
        requests = [[name, RequestType.ADD],
                    [name, RequestType.LOOKUP],
                    [name, RequestType.UPDATE]]

        output_stats = self.run_exp(requests)

        self.assertEqual(output_stats.requests, 3, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 3, "Successful requests mismatch")
        self.assertEqual(output_stats.read, 1, "Successful reads mismatch")
        self.assertEqual(output_stats.write, 1, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 0, "Successful removes mismatch")

        # run second experiment: restart GNS without deleting state
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'clean_start', False)  # n seconds

        requests = [[name, RequestType.LOOKUP],
                    [name, RequestType.UPDATE],
                    [name, RequestType.REMOVE]]

        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, 3, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 3, "Successful requests mismatch")
        self.assertEqual(output_stats.read, 1, "Successful reads mismatch")
        self.assertEqual(output_stats.write, 1, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 0, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 1, "Successful removes mismatch")

    def test_d_1name_read_write_latency(self):
        """For 1 name, measure read and write latencies on a local machine"""
        request_rate = 50
        duration = 20
        output_stats = self.run_exp_reads_writes(request_rate, duration)
        print 'Read latency (90-percentile)', output_stats.read_perc90, 'ms', 'Write latency (90-percentile)',\
            output_stats.write_perc90, 'ms',
        self.assertLess(output_stats.read_perc90, 20.0, "High latency reads")
        self.assertLess(output_stats.write_perc90, 50.0, "High latency writes")

    def test_e_1name_read_write_emulated_latency(self):
        """For 1 name, measure read and write latencies while emulating wide-area latency between nodes"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'emulate_ping_latencies', True)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'emulation_type', local.exp_config.CONSTANT_DELAY)
        request_rate = 50
        duration = 30
        output_stats = self.run_exp_reads_writes(request_rate, duration)
        print 'Read latency (90-percentile)', output_stats.read_perc90, 'ms', 'Write latency (90-percentile)',\
            output_stats.write_perc90, 'ms',
        self.assertLess(output_stats.read_perc90, 2*local.exp_config.DEFAULT_CONST_DELAY, "High latency reads")
        self.assertLess(output_stats.write_perc90, 5*local.exp_config.DEFAULT_CONST_DELAY, "High latency writes")

    def test_f_read_write_throughput(self):
        """For 1 name, measure read write throughput with equal number of reads and writes"""
        request_rate = 50
        duration = 30
        throughput = 0
        try:
            while True:
                print 'Testing throughput ', request_rate, 'req/sec'
                self.run_exp_reads_writes(request_rate, duration)
                throughput = request_rate
                request_rate *= 2
        except AssertionError:
            pass
        print 'Throughput:', throughput, 'reads/sec. ', throughput, 'writes/sec'

    def test_g_add_throughput(self):
        """Measures throughput of add requests"""
        request_rate = 50
        duration = 30
        throughput = 0
        try:
            while True:
                print 'Testing throughput', request_rate, 'req/sec'
                self.run_exp_add(request_rate, duration)
                throughput = request_rate
                request_rate *= 2
        except AssertionError:
            pass
        print 'Throughput:', throughput, 'add requests/sec. '

    # def test_h_add_throughput(self):
    #     """Measures throughput of add requests"""
    #     request_rate = 50
    #     duration = 30
    #     throughput = 0
    #     try:
    #         while True:
    #             print 'Testing throughput ', request_rate, 'req/sec'
    #             self.run_exp_add(request_rate, duration)
    #             throughput = request_rate
    #             request_rate *= 2
    #     except AssertionError:
    #         pass
    #     print 'Throughput:', throughput, 'add requests/sec. '

    def test_h_single_node_failure(self):
        """Fail the three name servers one by one and check if requests are successful."""
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

    def test_i_invalid_request_failure(self):
        """Is failure returning quickly for invalid requests, e.g., reads for non-existent names"""
        # do we log failure quickly?
        requests = []
        len_name = 20
        num_requests = 100
        name = 'test_name'
        requests.append([name, RequestType.ADD])  # add a name
        delay = 2000
        requests.append([delay, RequestType.DELAY])
        #  the requests below are all expected to fail, e.g., add for an existing name.  Lookup, update, remove for
        # non-existing names.
        for i in range(num_requests):
            requests.extend([[name, RequestType.ADD],
                             [gen_random_string(len_name), RequestType.LOOKUP],
                             [gen_random_string(len_name), RequestType.UPDATE],
                             [gen_random_string(len_name), RequestType.REMOVE]])
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', 10)
        output_stats = self.run_exp(requests)
        self.assertEqual(output_stats.requests, 401, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 1, "Successful requests mismatch")
        self.assertEqual(output_stats.read_failed, 100, "Failed reads mismatch")
        self.assertEqual(output_stats.write_failed, 100, "Failed writes mismatch")
        self.assertEqual(output_stats.add_failed, 100, "Failed adds mismatch")
        self.assertEqual(output_stats.remove_failed, 100, "Failed removes mismatch")
        self.assertLess(output_stats.failed_read_perc90, 100, "High latency failed reads")
        self.assertLess(output_stats.failed_write_perc90, 100, "High latency failed writes")
        self.assertLess(output_stats.failed_add_perc90, 100, "High latency failed adds")
        self.assertLess(output_stats.failed_remove_perc90, 100, "High latency failed removes")

    def run_exp(self, requests):
        """Not a test. Run experiments given a set of requests."""
        write_tuple_array(requests, self.trace_filename, p=False)

        # write config
        temp_config_file = os.path.join(parent_folder, 'resources/tmp.ini')
        self.config_parse.write(open(temp_config_file, 'w'))

        exp_script = os.path.join(parent_folder, 'local', 'run_all_local.py')

        outfile = os.path.join(parent_folder, 'resources', 'test.out')
        errfile = os.path.join(parent_folder, 'resources', 'test.err')
        os.system(exp_script + ' ' + temp_config_file + ' > ' + outfile + ' 2> ' + errfile)

        stats_folder = os.path.join(self.gns_folder, local.exp_config.DEFAULT_WORKING_DIR,
                                    local.exp_config.DEFAULT_STATS_FOLDER)
        return FinalStats(stats_folder)

    def run_exp_reads_writes(self, request_rate, exp_duration):
        """Runs an experiment with equal number of reads and writes for a name at a given request rate.
        It also checks if all requests are successful."""

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', exp_duration)  # n seconds
        name = 'test_name'
        n = int(request_rate * exp_duration)
        requests = [[name, RequestType.ADD]]
        delay = 2000 # ms
        requests.append([delay, RequestType.DELAY])  # wait after an add request to ensure name is added
        for i in range(n):
            requests.append([name, RequestType.LOOKUP])
            requests.append([name, RequestType.UPDATE])
        # wait before sending remove to ensure all previous requests are complete
        delay = 2000 # ms
        requests.append([delay, RequestType.DELAY])
        requests.append([name, RequestType.REMOVE])

        output_stats = self.run_exp(requests)

        self.assertEqual(output_stats.requests, 2 + n*2, "Total number of requests mismatch")
        # some read and write can fail at start of test while name is being added
        self.assertEqual(output_stats.success, 2 + n*2, "Successful requests mismatch")
        self.assertEqual(output_stats.read, n, "Successful reads mismatch")
        self.assertEqual(output_stats.write, n, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 1, "Successful removes mismatch")
        return output_stats

    def run_exp_add(self, request_rate, exp_duration):
        """Runs an experiment which adds random records at given rate for given duration.
        It also checks if all requests are successful."""

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', exp_duration)  # n seconds
        # name = 'test_name'
        n = int(request_rate * exp_duration)
        requests = []
        name_length = 10
        for i in range(n):
            requests.append([gen_random_string(name_length), RequestType.ADD])

        output_stats = self.run_exp(requests)

        self.assertEqual(output_stats.requests, n, "Total number of requests mismatch")
        # some read and write can fail at start of test while name is being added
        self.assertEqual(output_stats.success, n, "Successful requests mismatch")
        self.assertEqual(output_stats.add, n, "Successful adds mismatch")
        return output_stats


class Test1NodeLocal(Test3NodeLocal):
    """
    Tests a 1 name server and 1 local name server GNS.
    """
    ns = 1

    def setUp(self):
        Test3NodeLocal.setUp(self)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', 1)


def gen_random_string(size):
    chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    s = ''
    while len(s) < size:
        s += chars[random.randint(0, len(chars)-1)]
    return s


