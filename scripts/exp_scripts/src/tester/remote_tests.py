import unittest
import ConfigParser
import os
import sys
import inspect
import random


script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

from workload.write_workload import RequestType, workload_writer
import distributed.exp_config
from logparse.read_final_stats import FinalStats
from nodeconfig.node_config_writer import deployment_config_writer
from nodeconfig.node_config_latency_calculator import default_latency_calculator

__author__ = 'abhigyan'


class TestSetupRemote(unittest.TestCase):
    """ Performs common setup tasks for running distributed tests. Other tests use this as base class.
    """
    ns = 8
    lns = 4
    nsfile = ''
    lnsfile = ''
    workload_conf = ConfigParser.ConfigParser()

    trace_folder = ''
    def setUp(self):
        self.config_file = os.path.join(parent_folder, 'resources', 'distributed_test_env.ini')
        self.config_parse = ConfigParser.ConfigParser()
        self.config_parse.optionxform = str
        self.config_parse.read(self.config_file)
        # used for reading final output from test
        self.local_output_folder = self.config_parse.get(ConfigParser.DEFAULTSECT, 'local_output_folder')
        os.system('mkdir -p ' + self.local_output_folder)

        self.ns_file = self.config_parse.get(ConfigParser.DEFAULTSECT, 'ns_file')
        self.ns = get_line_count(self.ns_file)
        self.lns_file = self.config_parse.get(ConfigParser.DEFAULTSECT, 'lns_file')
        self.lns = get_line_count(self.lns_file)

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'is_experiment_mode', True)
        self.trace_folder = os.path.join(self.local_output_folder, 'trace')
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'update_trace', self.trace_folder)
        # self.trace_filename = get_trace_filename(trace_folder, self.ns)
        node_config_file = os.path.join(self.local_output_folder, 'node_config_file')
        deployment_config_writer(self.ns_file, self.lns_file, node_config_file,)

        node_config_folder = os.path.join(self.local_output_folder, 'node_config_folder')
        default_latency_calculator(node_config_file, node_config_folder, filename_id=False)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'config_folder', node_config_folder)

    def run_exp(self):
        """Not a test. Run experiments given a set of requests."""
        # write_tuple_array(requests, self.trace_filename, p=False)
        # workload already written
        # work_dir = os.path.join(self.local_output_folder)
        # write config

        temp_workload_config_file = os.path.join(self.local_output_folder, 'tmp_w.ini')
        self.workload_conf.write(open(temp_workload_config_file, 'w'))

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'wfile', temp_workload_config_file)

        temp_config_file = os.path.join(self.local_output_folder, 'tmp.ini')
        self.config_parse.write(open(temp_config_file, 'w'))
        exp_folder = os.path.join(parent_folder, 'distributed')
        outfile = os.path.join(self.local_output_folder, 'test.out')
        errfile = os.path.join(self.local_output_folder, 'test.err')
        # run_distributed.py can only be run from its current folder.
        os.system('cd ' + exp_folder + '; ./run_distributed.py ' + temp_config_file) # + ' > ' + outfile + ' 2> ' + errfile

        stats_folder = os.path.join(self.local_output_folder, distributed.exp_config.DEFAULT_STATS_FOLDER)
        return FinalStats(stats_folder)


class DistributedTests(TestSetupRemote):

    def test_a_first_test(self):
        """This test checks whether we can run distributed tests successfully"""

        delay_ms = 2500
        lns_req = {}
        for lns in open(self.lns_file):
            lns = lns.strip()
            print 'abcd'
            print lns
            print 'abcd'
            name = 'test_name_' + lns
            lns_req[lns] = [[name, RequestType.ADD], [delay_ms, RequestType.DELAY],
                            [name, RequestType.LOOKUP],
                            [name, RequestType.UPDATE],
                            [name, RequestType.LOOKUP], [delay_ms, RequestType.DELAY],
                            [name, RequestType.REMOVE]]
        workload_writer(lns_req, self.trace_folder)

        exp_duration_sec = 10 + 2 * delay_ms / 1000
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(exp_duration_sec))
        output_stats = self.run_exp()

        self.assertEqual(output_stats.requests, 5, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, 5, "Successful requests mismatch")
        self.assertEqual(output_stats.read, 2, "Successful reads mismatch")
        self.assertEqual(output_stats.write, 1, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 1, "Successful removes mismatch")


def get_line_count(filename):
    """ Returns the number of non-empty lines
    """
    lines = open(filename).readlines()
    count = 0
    for line in lines:
        if len(line.strip()) > 0:
            count += 1
    return count
