
""" Module performs setup tasks necessary to run a test on a multiple remote machines.
It does not include any test cases."""

import ConfigParser
import os
import sys
import inspect

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

import distributed.exp_config
from logparse.read_final_stats import FinalStats
from nodeconfig.node_config_writer import emulation_config_writer  # deployment_config_writer
from nodeconfig.node_config_latency_calculator import default_latency_calculator, geo_latency_calculator
from workload.write_workload import workload_writer
from test_utils import *

from local_setup import BasicSetup

__author__ = 'abhigyan'


class TestSetupRemote(BasicSetup):
    """ Performs common setup tasks for running distributed tests. Other tests use this as base class"""

    # list of nodes to run name server
    ns_file = None

    # list of nodes to run local name server
    lns_file = None

    lns_geo_file = None

    ns_geo_file = None

    # type of emulation to be used. can be : None/geographic/const
    emulation = None

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
        if self.config_parse.has_option(ConfigParser.DEFAULTSECT, 'lns_geo_file'):
            self.lns_geo_file = self.config_parse.get(ConfigParser.DEFAULTSECT, 'lns_geo_file')

        if self.config_parse.has_option(ConfigParser.DEFAULTSECT, 'ns_geo_file'):
            self.ns_geo_file = self.config_parse.get(ConfigParser.DEFAULTSECT, 'ns_geo_file')

        if not self.config_parse.has_option(ConfigParser.DEFAULTSECT, 'is_experiment_mode'):
            self.config_parse.set(ConfigParser.DEFAULTSECT, 'is_experiment_mode', True)

        if not self.config_parse.has_option(ConfigParser.DEFAULTSECT, 'update_trace'):
            self.trace_folder = os.path.join(self.local_output_folder, 'trace')
            self.config_parse.set(ConfigParser.DEFAULTSECT, 'update_trace', self.trace_folder)
        else:
            self.trace_folder = self.config_parse.get(ConfigParser.DEFAULTSECT, 'update_trace')

    def run_exp(self, requests):
        """Run experiments given a set of requests for a single local name server"""
        lns_requests = {}
        for i in range(self.lns):
            lns_id = self.ns + i
            if i == 0:
                lns_requests[lns_id] = requests
            else:
                lns_requests[lns_id] = []
        workload_writer(lns_requests, self.trace_folder)
        return self.run_exp_multi_lns()

    def run_exp_multi_lns(self):
        """ Runs experiments with multiple local name servers sending requests"""

        node_config_file = os.path.join(self.local_output_folder, 'node_config_file')
        emulation_config_writer(self.ns, self.lns, node_config_file, self.ns_file, self.lns_file)
        # deployment_config_writer(self.ns_file, self.lns_file, node_config_file)

        node_config_folder = os.path.join(self.local_output_folder, 'node_config_folder')

        if self.emulation is None:
            default_latency_calculator(node_config_file, node_config_folder)
        elif self.emulation == 'geographic':
            geo_latency_calculator(node_config_folder, node_config_file, self.ns_geo_file, self.lns_geo_file)

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'config_folder', node_config_folder)
        temp_workload_config_file = os.path.join(self.local_output_folder, 'tmp_w.ini')
        self.workload_conf.write(open(temp_workload_config_file, 'w'))

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'wfile', temp_workload_config_file)

        temp_config_file = os.path.join(self.local_output_folder, 'tmp.ini')

        if self.exp_output_folder is not None and self.exp_output_folder != '':
            self.config_parse.set(ConfigParser.DEFAULTSECT, 'local_output_folder', self.exp_output_folder)
        else:
            self.exp_output_folder = self.local_output_folder
            
        # write the config file here
        self.config_parse.write(open(temp_config_file, 'w'))

        out_file = os.path.join(self.local_output_folder, 'test.out')
        err_file = os.path.join(self.local_output_folder, 'test.err')
        exp_folder = os.path.join(parent_folder, 'distributed')

        output_to_console = True
        x = ' > ' + out_file + ' 2> ' + err_file
        if output_to_console:
            x = ''
        print '\nStarting experiment ....\n'
        # the script 'run_distributed.py' can only be run from its current folder. so we 'cd' to its folder
        os.system('cd ' + exp_folder + '; ./run_distributed.py ' + temp_config_file + x)

        print '\nEXPERIMENT OVER \n'
        stats_folder = os.path.join(self.exp_output_folder, distributed.exp_config.DEFAULT_STATS_FOLDER)
        return FinalStats(stats_folder)
