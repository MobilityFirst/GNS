import unittest
import ConfigParser
import os
import sys
import inspect

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

from workload.write_workload import get_trace_filename, RequestType
from util.write_utils import write_tuple_array
from logparse.read_final_stats import FinalStats
import local.exp_config


class Test3NodeLocal(unittest.TestCase):

    def setUp(self):
        self.config_file = os.path.join(parent_folder, 'resources', 'local_test_env.ini')
        self.config_parse = ConfigParser.ConfigParser()
        self.config_parse.optionxform=str
        self.config_parse.read(self.config_file)
        self.gns_folder = self.config_parse.get(ConfigParser.DEFAULTSECT, 'gns_folder')
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'num_ns', 3)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'num_lns', 1)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'is_experiment_mode', True)
        trace_folder = os.path.join(self.gns_folder, 'trace')
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'trace_folder', trace_folder)
        self.trace_filename = get_trace_filename(trace_folder, 3)

    def test_a_1name(self):
        """Test add, remove, lookup, and delete operations for a single name"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(5))
        name = 'test_name'
        requests = [[name, RequestType.ADD],
                    [name, RequestType.LOOKUP],
                    [name, RequestType.UPDATE],
                    [name, RequestType.LOOKUP],
                    [name, RequestType.REMOVE]]
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

        n = 500
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(2*n/50))  # n seconds
        name = 'test_name'
        requests = [[name, RequestType.ADD]]
        for i in range(n):
            requests.append([name, RequestType.LOOKUP])
            requests.append([name, RequestType.UPDATE])
        output_stats = self.run_exp(requests)

        self.assertEqual(output_stats.requests, 1 + n*2, "Total number of requests mismatch")
        # some read and write can fail at start of test while name is being added
        self.assertGreater(output_stats.success, 1 + 0.9*n*2, "Successful requests mismatch")
        self.assertGreater(output_stats.read, 0.9*n, "Successful reads mismatch")
        self.assertGreater(output_stats.write, 0.9*n, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 0, "Successful removes mismatch")
        self.assertLess(output_stats.read_perc90, 20.0, "High latency reads")
        self.assertLess(output_stats.write_perc90, 50.0, "High latency writes")

    def test_e_1name_read_write_emulated_latency(self):
        """For 1 name, measure read and write latencies while emulating wide-area latency between nodes"""
        n = 500
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', (2*n/50))  # n seconds
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'emulate_ping_latencies', True)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'emulation_type', local.exp_config.CONSTANT_DELAY)

        name = 'test_name'
        requests = [[name, RequestType.ADD]]
        for i in range(n):
            requests.append([name, RequestType.LOOKUP])
            requests.append([name, RequestType.UPDATE])
        output_stats = self.run_exp(requests)

        self.assertEqual(output_stats.requests, 1 + n*2, "Total number of requests mismatch")
        # some read and write can fail at start of test while name is being added
        self.assertGreater(output_stats.success, 1 + 0.9*n*2, "Successful requests mismatch")
        self.assertGreater(output_stats.read, 0.9*n, "Successful reads mismatch")
        self.assertGreater(output_stats.write, 0.9*n, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 0, "Successful removes mismatch")
        self.assertLess(output_stats.read_perc90, 2*local.exp_config.DEFAULT_CONST_DELAY, "High latency reads")
        self.assertLess(output_stats.write_perc90, 5*local.exp_config.DEFAULT_CONST_DELAY, "High latency writes")

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


    # def test_choice(self):
    #     element = random.choice(self.seq)
    #     self.assertTrue(element in self.seq)
    #
    # def test_sample(self):
    #     with self.assertRaises(ValueError):
    #         random.sample(self.seq, 20)
    #     for element in random.sample(self.seq, 5):
    #         self.assertTrue(element in self.seq)


class TestSequenceFunctions2(unittest.TestCase):

    def setUp(self):
        self.seq = range(10)

    # def test_shuffle(self):
    #     # make sure the shuffled sequence does not lose any elements
    #     random.shuffle(self.seq)
    #     self.seq.sort()
    #     self.assertEqual(self.seq, range(10))
    #
    #     # should raise an exception for an immutable sequence
    #     self.assertRaises(TypeError, random.shuffle, (1, 2, 3))
    #
    # def test_choice(self):
    #     element = random.choice(self.seq)
    #     self.assertTrue(element in self.seq)
    #
    # def test_sample(self):
    #     with self.assertRaises(ValueError):
    #         random.sample(self.seq, 20)
    #     for element in random.sample(self.seq, 5):
    #         self.assertTrue(element in self.seq)

#suite = unittest.TestLoader().loadTestsFromTestCase(TestSequenceFunctions)
#unittest.TextTestRunner(verbosity=2).run(suite)


