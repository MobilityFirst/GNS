""" Module performs setup tasks necessary to run a test on local machine. It does not include any test cases."""


import unittest
import ConfigParser
import os
import sys
import inspect


script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)


from workload.write_workload import workload_writer
from logparse.read_final_stats import FinalStats
import local.exp_config
import local.generate_config_file


class BasicSetup(unittest.TestCase):
    """Base class for all unittests for gns. Describes the common parameters needed by all tests"""

    # number of name servers
    ns = 3
    # number of local name servers
    lns = 1
    lns_id = None

    # include all workload parameters here
    workload_conf = ConfigParser.ConfigParser()
    # include all other config parameters
    config_parse = ConfigParser.ConfigParser()

    # folder where workload trace is stored
    trace_folder = ''

    # folder where output is stored on local machine
    local_output_folder = ''

    # folder where output from a single experiment is stored. this is used if we want to save the output from
    # some experiments in a given test. by default, exp_output_folder = local_output_folder
    exp_output_folder = ''


class TestSetupLocal(BasicSetup):
    """ Performs common setup tasks for running local tests. Other tests use this as base class"""

    def setUp(self):
        self.config_file = os.path.join(parent_folder, 'resources', 'local_test_env.ini')
        self.config_parse.optionxform = str
        self.config_parse.read(self.config_file)
        self.gns_folder = self.config_parse.get(ConfigParser.DEFAULTSECT, 'gns_folder')
        self.lns_id = self.ns

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'is_experiment_mode', True)
        self.local_output_folder = os.path.join(self.gns_folder, local.exp_config.DEFAULT_WORKING_DIR)
        self.trace_folder = os.path.join(self.gns_folder, local.exp_config.DEFAULT_WORKING_DIR, 'trace')
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'trace_folder', self.trace_folder)

    def run_exp(self, requests):
        """Not a test. Run experiments given a set of requests for a single local name server"""
        workload_writer({self.lns_id: requests}, self.trace_folder)
        return self.run_exp_multi_lns()

    def run_exp_multi_lns(self):
        """ Runs an experiment involving multiple local name servers"""

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'num_ns', self.ns)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'num_lns', self.lns)

        work_dir = os.path.join(self.gns_folder, local.exp_config.DEFAULT_WORKING_DIR)

        # write config
        temp_workload_config_file = os.path.join(work_dir, 'tmp_w.ini')
        self.workload_conf.write(open(temp_workload_config_file, 'w'))

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'wfile', temp_workload_config_file)

        if self.exp_output_folder is not None and self.exp_output_folder != '':
            self.config_parse.set(ConfigParser.DEFAULTSECT, 'output_folder', self.exp_output_folder)
        else:
            self.exp_output_folder = self.local_output_folder

        temp_config_file = os.path.join(work_dir, 'tmp.ini')
        self.config_parse.write(open(temp_config_file, 'w'))
        exp_script = os.path.join(parent_folder, 'local', 'run_all_local.py')
        outfile = os.path.join(work_dir, 'test.out')
        errfile = os.path.join(work_dir, 'test.err')
        os.system(exp_script + ' ' + temp_config_file + ' > ' + outfile + ' 2> ' + errfile)

        stats_folder = os.path.join(self.exp_output_folder, local.exp_config.DEFAULT_STATS_FOLDER)
        # stats_folder = os.path.join(work_dir, exp_config.DEFAULT_STATS_FOLDER)
        return FinalStats(stats_folder)


