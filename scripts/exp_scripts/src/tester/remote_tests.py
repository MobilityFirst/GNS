import unittest
import ConfigParser
import os
import sys
import inspect
import random


script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

import multinode.exp_config
# from util.write_utils import write_tuple_array
from logparse.read_final_stats import FinalStats

__author__ = 'abhigyan'





class TestSetupRemote(unittest.TestCase):
    """ Performs common setup tasks for running distributed tests. Other tests use this as base class.
    """
    ns = 8
    lns = 4
    workload_conf = ConfigParser.ConfigParser()

    def setUp(self):
        self.config_file = os.path.join(parent_folder, 'resources', 'distributed_test_env.ini')
        self.config_parse = ConfigParser.ConfigParser()
        self.config_parse.optionxform = str
        self.config_parse.read(self.config_file)
        # used for reading final output from test
        self.local_output_folder = self.config_parse.get(ConfigParser.DEFAULTSECT, 'local_output_folder')
        # number of name servers and local name servers to run tests for.
        if self.config_parse.has_option(ConfigParser.DEFAULTSECT, 'num_ns'):
            self.ns = int(self.config_parse.get(ConfigParser.DEFAULTSECT, 'num_ns'))
        else:
            ns_file = self.config_parse.get(ConfigParser.DEFAULTSECT, 'ns_file')
            self.ns = get_line_count(ns_file)
            # self.config_parse.set(ConfigParser.DEFAULTSECT, 'num_ns', self.ns)
        if self.config_parse.has_option(ConfigParser.DEFAULTSECT, 'num_lns'):
            self.lns = int(self.config_parse.get(ConfigParser.DEFAULTSECT, 'num_lns'))
        else:
            lns_file = self.config_parse.get(ConfigParser.DEFAULTSECT, 'lns_file')
            self.lns = get_line_count(lns_file)

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'is_experiment_mode', True)
        trace_folder = os.path.join(self.local_output_folder, 'trace')
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'trace_folder', trace_folder)
        # self.trace_filename = get_trace_filename(trace_folder, self.ns)

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
        exp_script = os.path.join(parent_folder, 'local', 'run_all_local.py')
        outfile = os.path.join(self.local_output_folder, 'test.out')
        errfile = os.path.join(self.local_output_folder, 'test.err')
        os.system(exp_script + ' ' + temp_config_file + ' > ' + outfile + ' 2> ' + errfile)

        stats_folder = os.path.join(self.local_output_folder, multinode.exp_config.DEFAULT_STATS_FOLDER)
        return FinalStats(stats_folder)

def get_line_count(filename):
    """ Returns the number of non-empty lines
    """
    lines = open(filename).readlines()
    count = 0
    for line in lines:
        if len(line.strip()) > 0:
            count += 1
    return count
