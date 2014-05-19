import unittest
import ConfigParser
import os
import sys
import inspect
import random

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

import distributed.exp_config
from logparse.read_final_stats import FinalStats
from nodeconfig.node_config_writer import emulation_config_writer  # deployment_config_writer
from nodeconfig.node_config_latency_calculator import default_latency_calculator, geo_latency_calculator
from workload.write_workload import RequestType, WorkloadParams, workload_writer
from test_utils import *
from util.exp_events import NodeCrashEvent, write_events_to_file
from workload.gen_add_requests import gen_add_requests
from workload.gen_geolocality_workload import gen_geolocality_trace

import local_tests

__author__ = 'abhigyan'


class TestSetupRemote(local_tests.BasicSetup):
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

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'is_experiment_mode', True)

        if not self.config_parse.has_option(ConfigParser.DEFAULTSECT, 'update_trace'):
            self.trace_folder = os.path.join(self.local_output_folder, 'trace')
            self.config_parse.set(ConfigParser.DEFAULTSECT, 'update_trace', self.trace_folder)
        else:
            self.trace_folder = self.config_parse.get(ConfigParser.DEFAULTSECT, 'update_trace')

    def run_exp(self, requests):
        """Run experiments given a set of requests for a single local name server"""
        workload_writer({self.ns: requests}, self.trace_folder)
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
        print '\nStarting experiment ....\n'
        # the script 'run_distributed.py' can only be run from its current folder. so we 'cd' to its folder
        os.system('cd ' + exp_folder + '; ./run_distributed.py ' + temp_config_file)
        #+ ' > ' + out_file + ' 2> ' + err_file)
        print '\nEXPERIMENT OVER \n'
        stats_folder = os.path.join(self.exp_output_folder, distributed.exp_config.DEFAULT_STATS_FOLDER)
        return FinalStats(stats_folder)


class FeatureTestDistributed(TestSetupRemote, local_tests.FeatureTestMultiNodeLocal):
    """ Runs local_tests.FeatureTestMultiNode in a distributed setup. The setup tasks are performed by
    TestSetupRemote and test cases are provided by local_tests.FeatureTestMultiNode"""
    pass


class ThroughputTestDistributed(TestSetupRemote):
    """ Tests throughput of basic operations: Add, remove, lookup, delete.
    For each operation, we run a series of tests doubling the request rate each time until
    maximum throughput is reached. These tests take tens of minutes to complete due to multiple
    tests with each operation. """

    ns = 3

    # what fraction of requests must be successful for a throughput test to pass.
    success_threshold = 1.0

    def test_a_latency(self):
        """ Measures read and write latency for varying number of replicas at moderate load"""

        num_replica_set = range(3, self.ns + 1, 2)
        print "Replica set: ", num_replica_set
        for num_replica in num_replica_set:
            output_dir = 'read_write_latency/replica_' + str(num_replica)
            # output from experiment will go in this folder
            self.exp_output_folder = os.path.join(self.local_output_folder, output_dir)
            print 'Testing read/write latency with replicas =', num_replica, 'Folder =', self.exp_output_folder
            self.run_latency_test(num_replica, num_names=10000, req_rate=500, num_requests=100000)

    def test_a1_latency_low_rate(self):
        """ Measures read and write latency for varying number of replicas at very low load (used on planetlab)"""
        ns_sleep = 100  # wait longer on PL so that all nodes are up.
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'ns_sleep', str(ns_sleep))
        extra_pl_wait = 150  # wait longer on PL so that all nodes are up.
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'extra_wait', str(extra_pl_wait))
        is_debug_mode = True
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'is_debug_mode', str(is_debug_mode))

        num_replica_set = range(3, self.ns + 1, 2)
        print "Replica set: ", num_replica_set
        for num_replica in num_replica_set:
            output_dir = 'read_write_latency/replica_' + str(num_replica)
            # output from experiment will go in this folder
            self.exp_output_folder = os.path.join(self.local_output_folder, output_dir)
            print 'Testing read/write latency with replicas =', num_replica, 'Folder =', self.exp_output_folder
            self.run_latency_test(num_replica, num_names=10000, req_rate=100, num_requests=0)

    def test_b_latency_read_coordination(self):
        """ Measures read latency when we replicas coordinate on reads also"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'read_coordination', str(True))
        self.test_a_latency()

    def test_b1_latency_no_log(self):
        """ Measures latency when paxos does not write any messages to disk"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'no_paxos_log', str(True))
        self.test_b_latency_read_coordination()

    def test_c_group_change_latency(self):
        """ Measure latency of group changes in GNS. LNS sends group change requests and NS replies back to LNS
        which helps us measure group change latency"""
        num_names = 10000
        num_replica_set = range(3, self.ns + 1, 2)
        print "Replica set: ", num_replica_set
        for num_replica in num_replica_set:
            output_dir = 'group_change_latency/replica_' + str(num_replica)
            # output from experiment will go in this folder
            self.exp_output_folder = os.path.join(self.local_output_folder, output_dir)
            print 'Testing group change latency with replicas =', num_replica, 'Folder =', self.exp_output_folder
            try:
                self.run_group_change_test(num_replica, num_names)
            except AssertionError:
                print " \n\n********** Assertion error. but continuing ***** \n\n"

    def test_c1_group_change_latency_nolog(self):
        """Measures latency of group changes when paxos does not write any messages to disk"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'no_paxos_log', str(True))
        self.test_c_group_change_latency()

    def test_d_write_throughput(self):
        """ Run write throughput tests for varying number of replicas of name.
        Number of name servers equals number of replicas so that all names are replicated at all locations."""

        num_names = 10000
        num_replica_set = range(3, self.ns + 1, 2)
        print "Replica set: ", num_replica_set
        for num_replica in num_replica_set:
            print 'Testing write throughput with replicas =', num_replica
            self.run_write_throughput_test(num_replica, num_names)

    def test_e_coordinated_read_throughput(self):
        """ Run (coordinated) read throughput tests for varying number of name servers.
        Number of name servers equals number of replicas so that all names are replicated at all locations."""

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'read_coordination', str(True))

        num_names = 10000
        num_replica_set = range(3, self.ns + 1, 2)
        print "Replica set: ", num_replica_set
        for num_replica in num_replica_set:
            print 'Testing coordinated read throughput with replicas =', num_replica
            self.run_coordinated_read_throughput_test(num_replica, num_names)

    def test_f_group_change_throughput(self):
        """ Run (coordinated) read throughput tests for varying number of replicas of a name."""
        # assert False  # "not implemented"
        num_names = 10000
        num_replica_set = range(3, self.ns + 1, 2)
        print "Replica set: ", num_replica_set
        for num_replica in num_replica_set:
            print 'Testing group change throughput with replicas =', num_replica
            self.run_group_change_throughput_test(num_replica, num_names)

    def test_g_write_throughput_4k_object(self):
        """ Test write throughput with a 4KB object size. Other experiments are with a 200-byte object size"""

        size_kb = 4
        print 'Testing read throughput with object size: ', size_kb, 'KB'
        self.workload_conf.set(ConfigParser.DEFAULTSECT, WorkloadParams.OBJECT_SIZE, str(size_kb))
        self.test_d_write_throughput()

    def test_h_read_throughput_4k_object(self):
        """ Test write throughput with a 4KB object size. Other experiments are with a 200-byte object size"""

        size_kb = 4
        print 'Testing read throughput with object size: ', size_kb, 'KB'
        self.workload_conf.set(ConfigParser.DEFAULTSECT, WorkloadParams.OBJECT_SIZE, str(size_kb))
        self.test_e_coordinated_read_throughput()

    def test_i_write_throughput_node_failure(self):
        """Test write throughput when a node fails in the middle of the experiment"""
        num_names = 10000
        num_replica = 7
        # write a failure event
        exp_duration = 150
        req_rate = 2000

        failure_event_time = 50
        failed_node = 4
        event = NodeCrashEvent(failed_node, failure_event_time)
        exp_events = [event]
        event_file = os.path.join(self.local_output_folder, 'event_file')
        write_events_to_file(exp_events, event_file)
        print 'Event file is:', event_file
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'event_file', event_file)

        self.ns = num_replica
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', num_replica)

        self.exp_output_folder = os.path.join(self.local_output_folder, 'node_failure_test', 'failed_'
                                                                                             + str(failed_node))

        # self.run_write_throughput_test(num_replica, num_names, duration=exp_duration)
        try:
            self.run_exp_writes(num_names, req_rate, exp_duration)
        except AssertionError:
            # Failures expected due to node crash
            pass

    def test_i1_write_throughput_node_failure_static_replication(self):
        """This test measures throughput immediately after a node failure when using a static replication.
        This test is  written only to compare performance against GNS's locality-based placement. Locality-based
        placement creates one paxos instance per name but static placement creates only one paxos instance per node"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'scheme', 'static')
        self.test_i_write_throughput_node_failure()

    ######## methods below are not tests. they are helper methods for running tests above

    def run_latency_test(self, num_replica, num_names, req_rate, num_requests):
        """Measures latency of add, remove, lookup, and delete operations with given number
        of names such that a name is replicated at given number of nodes. It tests if all requests are successful"""

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', num_replica)

        total_time = 0

        delay = 5000
        names = []

        for i in range(num_names):
            names.append('test_name' + str(i))

        request_set = [[req_rate, RequestType.RATE]]

        from random import shuffle

        # add all names
        shuffle(names)
        request_set.extend([[name, RequestType.ADD] for name in names])
        total_time += len(names) / req_rate
        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        # send lookups in random order
        num_lookups = num_requests / 2
        for i in range(num_lookups):
            name = names[random.randint(0, num_names - 1)]
            request_set.append([name, RequestType.LOOKUP])
        total_time += num_lookups / req_rate

        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        # send updates in random order
        num_updates = num_requests / 2
        for i in range(num_updates):
            name = names[random.randint(0, num_names - 1)]
            request_set.append([name, RequestType.UPDATE])
        total_time += num_updates / req_rate

        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        shuffle(names)
        request_set.extend([[name, RequestType.REMOVE] for name in names])
        total_time += len(names) / req_rate
        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        print 'Total time', total_time
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', total_time)  # in seconds
        output_stats = self.run_exp(request_set)
        self.assertEqual(output_stats.requests, num_names * 2 + num_lookups + num_updates, "Total requests mismatch")
        self.assertEqual(output_stats.success, num_names * 2 + num_lookups + num_updates,
                         "Successful requests mismatch")
        self.assertEqual(output_stats.read, num_lookups, "Successful reads mismatch")
        self.assertEqual(output_stats.write, num_updates, "Successful writes mismatch")
        self.assertEqual(output_stats.add, num_names, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, num_names, "Successful removes mismatch")

    def run_group_change_test(self, num_replica, num_names):
        """ Add given names, and changes the replica set of each name N (= 3) times. It tests if add and group change
        request are successful"""
        total_time = 0
        req_rate = 100
        delay = 5000
        names = []

        for i in range(num_names):
            names.append('test_name' + str(i))

        request_set = [[req_rate, RequestType.RATE]]

        from random import shuffle

        # add all names
        request_set.extend([[name, RequestType.ADD] for name in names])
        total_time += len(names) / req_rate
        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        # send group changes in random order

        initial_version = 1
        group_size = min(num_replica, self.ns)
        assert group_size <= self.ns
        num_group_changes = 3

        for i in range(num_group_changes):
            version = (i + 1) + initial_version
            shuffle(names)
            for name in names:
                grp_change_request = [name, RequestType.GROUP_CHANGE, version,
                                      get_new_group_members_str(range(self.ns), group_size)]
                request_set.append(grp_change_request)
            total_time += len(names) / req_rate

        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        print 'Total time', total_time
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', total_time)  # in seconds
        output_stats = self.run_exp(request_set)

        self.assertEqual(output_stats.requests, num_names * (1 + num_group_changes), "Total requests mismatch")
        self.assertEqual(output_stats.success, num_names * (1 + num_group_changes), "Successful requests mismatch")
        self.assertEqual(output_stats.add, num_names, "Successful adds mismatch")
        self.assertEqual(output_stats.group_change, num_group_changes * num_names, "Successful group change mismatch")

    def run_write_throughput_test(self, num_replica, num_names, duration=100):
        """Tests throughput of write requests for a given number of replicas of name"""
        self.ns = num_replica
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', num_replica)
        min_rate = 500
        max_rate = 1500
        width = 250

        while max_rate - min_rate > width:
            mid_rate = (max_rate + min_rate) / 2
            output_dir = 'write_throughput_test/replica_' + str(num_replica) + '/rate_' + str(mid_rate)
            self.exp_output_folder = os.path.join(self.local_output_folder, output_dir)
            print 'Testing throughput ', mid_rate, 'req/sec'
            try:
                self.run_exp_writes(num_names, mid_rate, duration)
                min_rate = mid_rate
            except AssertionError:
                max_rate = mid_rate
        print 'throughput = ', min_rate

    def run_exp_writes(self, num_names, req_rate, duration):
        """ Adds names and sends writes to randomly chosen names at given rate, and tests if requests are successful"""

        total_time = 0
        delay = 10000
        names = ['test_name' + str(i) for i in range(num_names)]

        # add all names
        add_rate = 300
        if self.workload_conf.has_option(ConfigParser.DEFAULTSECT, WorkloadParams.OBJECT_SIZE):
            size_kb = self.workload_conf.get(ConfigParser.DEFAULTSECT, WorkloadParams.OBJECT_SIZE)
            if size_kb >= 1:
                add_rate = 100
        request_set = [[add_rate, RequestType.RATE]]  # add requests are sent at fixed rate so that add
        # does not become bottleneck.

        request_set.extend([[name, RequestType.ADD] for name in names])
        total_time += len(names) / add_rate

        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        request_set.append([req_rate, RequestType.RATE])  # send write requests at given rate
        # send 'num_writes' write requests, choosing names randomly
        num_writes = req_rate * duration
        for i in range(num_writes):
            name = names[random.randint(0, len(names) - 1)]
            request_set.append([name, RequestType.UPDATE])
        total_time += duration

        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        print 'Total time', total_time
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', total_time)  # in seconds
        output_stats = self.run_exp(request_set)

        self.assertEqual(output_stats.write, num_writes, "Successful writes mismatch")
        # todo debug why some adds are missing
        # self.assertEqual(output_stats.requests, num_names + num_writes, "Total number of requests mismatch")
        # self.assertEqual(output_stats.success, num_names + num_writes,  "Successful requests mismatch")
        # self.assertEqual(output_stats.write, num_writes, "Successful writes mismatch")
        # self.assertEqual(output_stats.add, num_names, "Successful adds mismatch")

    def run_coordinated_read_throughput_test(self, num_replica, num_names):
        """Tests throughput of coordinated read requests for a given number of replicas of name"""
        self.ns = num_replica
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', str(num_replica))
        min_rate = 0
        max_rate = 8000
        width = 250
        duration = 100
        while max_rate - min_rate > width:
            mid_rate = (max_rate + min_rate) / 2
            output_dir = 'coordinated_read_throughput/replica_' + str(num_replica) + '/rate_' + str(mid_rate)
            self.exp_output_folder = os.path.join(self.local_output_folder, output_dir)
            print 'Testing throughput ', mid_rate, 'req/sec'
            try:
                self.run_exp_coordinated_reads(num_names, mid_rate, duration)
                min_rate = mid_rate
            except AssertionError:
                max_rate = mid_rate
        print 'throughput = ', min_rate

    def run_exp_coordinated_reads(self, num_names, req_rate, duration):
        """ Adds names and sends (coordinated) reads to random names at given rate. Tests if requests are successful.
        Coordinated reads means all replicas of name coordinate upon read requests, like they do upon a write."""

        total_time = 0
        delay = 5000
        names = ['test_name' + str(i) for i in range(num_names)]

        # add all names
        add_rate = 500
        if self.workload_conf.has_option(ConfigParser.DEFAULTSECT, WorkloadParams.OBJECT_SIZE):
            size_kb = self.workload_conf.getint(ConfigParser.DEFAULTSECT, WorkloadParams.OBJECT_SIZE)
            if size_kb >= 1:
                add_rate = 200
        request_set = [[add_rate, RequestType.RATE]]  # add requests are sent at fixed rate so that add does not become
        # bottleneck.

        request_set.extend([[name, RequestType.ADD] for name in names])
        total_time += len(names) / add_rate

        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        request_set.append([req_rate, RequestType.RATE])  # send read requests at given rate
        # send 'num_reads' read requests, choosing names randomly
        num_reads = req_rate * duration
        for i in range(num_reads):
            name = names[random.randint(0, len(names) - 1)]
            request_set.append([name, RequestType.LOOKUP])
        total_time += duration

        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        print 'Total time', total_time
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', total_time)  # in seconds
        output_stats = self.run_exp(request_set)

        self.assertEqual(output_stats.requests, num_names + num_reads, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, num_names + num_reads, "Successful requests mismatch")
        self.assertEqual(output_stats.read, num_reads, "Successful reads mismatch")
        self.assertEqual(output_stats.add, num_names, "Successful adds mismatch")

    def run_group_change_throughput_test(self, group_size, num_names):
        """ Tests throughput of group change requests for a given group size"""
        min_rate = 500
        max_rate = 1500
        width = 125
        duration = 100
        while max_rate - min_rate > width:
            mid_rate = (max_rate + min_rate) / 2
            output_dir = 'group_change_throughput/replica_' + str(group_size) + '/rate_' + str(mid_rate)
            self.exp_output_folder = os.path.join(self.local_output_folder, output_dir)
            print 'Testing throughput ', mid_rate, 'req/sec'
            try:
                self.run_exp_group_changes(num_names, mid_rate, duration, group_size)
                min_rate = mid_rate
            except AssertionError:
                max_rate = mid_rate
        print '\n\n******** Group change throughput = ', min_rate, '******************\n\n'

    def run_exp_group_changes(self, num_names, req_rate, duration, group_size):
        """ Adds names and sends (coordinated) reads to random names at given rate. Tests if requests are successful.
        Coordinated reads means all replicas of name coordinate upon read requests, like they do upon a write."""

        total_time = 0
        delay = 5000
        names = ['test_name' + str(i) for i in range(num_names)]

        # add all names
        add_rate = 500
        request_set = [[500, RequestType.RATE]]  # add requests are sent at fixed rate so that add does not become
        # bottleneck.

        request_set.extend([[name, RequestType.ADD] for name in names])
        total_time += len(names) / add_rate

        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        request_set.append([req_rate, RequestType.RATE])  # send group change requests at given rate

        # send 'group_change_requests' requests, choosing names in a round-robin order
        group_change_requests = req_rate * duration

        initial_version = 1  # version number of group (should increment by one for each group change for a name)
        for i in range(group_change_requests):
            name = names[i % len(names)]
            version = (i / len(names) + 1) + initial_version
            grp_change_request = [name, RequestType.GROUP_CHANGE, version,
                                  get_new_group_members_str(range(self.ns), group_size)]
            request_set.append(grp_change_request)
        total_time += duration

        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        print 'Total time', total_time
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', total_time)  # in seconds
        output_stats = self.run_exp(request_set)

        self.assertEqual(output_stats.requests, num_names + group_change_requests, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, num_names + group_change_requests, "Successful requests mismatch")
        self.assertEqual(output_stats.group_change, group_change_requests, "Successful group change mismatch")
        self.assertEqual(output_stats.add, num_names, "Successful adds mismatch")

    ############## older code below. it may be deleted.  #################

    def test_f_read_write_throughput(self):
        """For 1 name, measure read write throughput with equal number of reads and writes"""
        request_rate = 400
        duration = 100
        throughput = 0
        try:
            while True:
                print 'Testing throughput ', request_rate, 'req/sec'
                self.run_exp_reads_writes(request_rate, duration)
                throughput = request_rate
                request_rate *= 2
        except AssertionError:
            pass
        print 'Read throughput:', throughput, 'req/sec. Write throughput:', throughput, 'writes/sec'

    def test_g_add_throughput(self):
        """Measures throughput of add requests"""
        request_rate = 400
        duration = 100
        throughput = 0
        try:
            while True:
                print 'Testing throughput', request_rate, 'req/sec'
                self.run_exp_add(request_rate, duration)
                throughput = request_rate
                request_rate *= 2
        except AssertionError:
            pass
        print 'Add Throughput:', throughput, 'req/sec. '

    def test_h_remove_throughput(self):
        """Measures throughput of add requests"""
        request_rate = 400
        duration = 200
        throughput = 0
        try:
            while True:
                print 'Testing throughput', request_rate, 'req/sec'
                self.run_exp_add_remove(request_rate, duration)
                throughput = request_rate
                request_rate *= 2
        except AssertionError:
            pass
        print 'Remove throughput:', throughput, 'req/sec. '

    def run_exp_reads_writes1(self, lns_ids, num_names, num_replicas, request_rate, exp_duration):
        """ Runs experiment by generating workload as per given parameters.
        """

    def run_exp_reads_writes(self, request_rate, exp_duration):
        """Runs an experiment with equal number of reads and writes for a name at a given request rate.
        It also checks if all requests are successful."""

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', exp_duration)  # in seconds
        name = 'test_name'
        n = int(request_rate * exp_duration)
        requests = [[name, RequestType.ADD]]
        delay = 2000  # ms
        requests.append([delay, RequestType.DELAY])  # wait after an add request to ensure name is added
        for i in range(n):
            requests.append([name, RequestType.LOOKUP])
            requests.append([name, RequestType.UPDATE])
        # wait before sending remove to ensure all previous requests are complete
        requests.append([delay, RequestType.DELAY])
        requests.append([name, RequestType.REMOVE])

        output_stats = self.run_exp(requests)

        self.assertEqual(output_stats.requests, 2 + n * 2, "Total number of requests mismatch")
        # some read and write can fail at start of test while name is being added
        self.assertEqual(output_stats.success, 2 + n * 2, "Successful requests mismatch")
        self.assertEqual(output_stats.read, n, "Successful reads mismatch")
        self.assertEqual(output_stats.write, n, "Successful writes mismatch")
        self.assertEqual(output_stats.add, 1, "Successful adds mismatch")
        self.assertEqual(output_stats.remove, 1, "Successful removes mismatch")
        return output_stats

    def run_exp_add(self, request_rate, exp_duration):
        """Runs an experiment which adds random records at given rate for given duration.
        It also checks if all requests are successful."""

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', exp_duration)  # in seconds
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

    def run_exp_add_remove(self, request_rate, exp_duration):
        """ Runs an experiment which first add a set of names to GNS and then removes them"""

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', exp_duration)  # in seconds
        # name = 'test_name'
        n = int(request_rate * exp_duration)
        requests = []
        name_length = 10
        for i in range(n / 2):
            requests.append([gen_random_string(name_length), RequestType.ADD])
        for i in range(n / 2):
            requests.append([gen_random_string(name_length), RequestType.REMOVE])
        output_stats = self.run_exp(requests)

        self.assertEqual(output_stats.requests, n, "Total number of requests mismatch")
        # some read and write can fail at start of test while name is being added
        self.assertEqual(output_stats.success, n, "Successful requests mismatch")
        self.assertEqual(output_stats.add, n, "Successful adds mismatch")
        return output_stats


class ThroughputTestLocal(local_tests.TestSetupLocal, ThroughputTestDistributed):
    """ Runs throughput tests on a local machine"""


class TestWorkloadWithGeoLocality(TestSetupRemote):
    """ Generates workloads with geo-locality and runs experiment on a wide-area platform"""

    def test_a_request_latency(self):
        """[Sequential consistency] Test with a geo-locality based workload and measures read and write latencies"""

        assert self.lns_geo_file is not None

        # self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', str(self.ns))

        self.ns = 24
        self.lns = 24

        total_time = 0
        # write workload. decide experiment time
        lns_ids_int = range(self.ns, self.ns + self.lns)
        lns_ids = [str(lns_id) for lns_id in lns_ids_int]

        num_names = 1000
        prefix = 'test_name'
        # create new trace
        create_empty_trace_files(lns_ids, self.trace_folder)

        # delay = (self.ns + self.lns) * 1000  # (wait so that each node can ping all nodes and get their addresses)
        delay = 10000
        append_request_to_all_files([delay, RequestType.DELAY], lns_ids, self.trace_folder)
        total_time += delay / 1000
        # set a request rate
        add_req_rate = 10

        append_request_to_all_files([add_req_rate, RequestType.RATE], lns_ids, self.trace_folder)

        total_time += num_names / len(lns_ids) / add_req_rate

        gen_add_requests(self.trace_folder, number_names=num_names, append_to_file=True, lns_ids=lns_ids,
                         name_prefix=prefix)

        delay = 30000
        append_request_to_all_files([delay, RequestType.DELAY], lns_ids, self.trace_folder)
        total_time += delay / 1000
        read_write_req_rate = 200

        reads_per_epoch_per_name = 800
        writes_per_epoch_per_name = 400
        total_lookups = 0
        total_updates = 0
        num_geo_locality_changes = 2
        for i in range(num_geo_locality_changes):
            num_lookups = num_names * reads_per_epoch_per_name
            num_updates = num_names * writes_per_epoch_per_name
            expected_time = (num_lookups + num_updates) / len(lns_ids) / read_write_req_rate
            actual_lookups, actual_updates = gen_geolocality_trace(self.trace_folder, self.lns_geo_file,
                                                                   number_names=num_names, num_lookups=num_lookups,
                                                                   num_updates=num_updates, append_to_file=True,
                                                                   lns_ids=lns_ids, name_prefix=prefix,
                                                                   exp_duration=expected_time,
                                                                   locality_parameter=4,
                                                                   locality_percent=1.0,
                                                                   num_lns=self.lns,
                                                                   seed=(i+1000))
            total_time += expected_time
            total_lookups += actual_lookups
            total_updates += actual_updates

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', str(total_time))
        print 'Total time: ', total_time
        output_stats = self.run_exp_multi_lns()
        self.assertEqual(output_stats.requests, num_names + total_lookups + total_updates,
                         "Total number of requests mismatch")
        self.assertEqual(output_stats.success, num_names + total_lookups + total_updates,
                         "Successful requests mismatch")
        self.assertEqual(output_stats.add, num_names, "Successful adds mismatch")
        self.assertEqual(output_stats.read, total_lookups, "Successful reads mismatch")
        self.assertEqual(output_stats.write, total_updates, "Successful writes mismatch")

    def run_exp_with_given_workload(self):
        pass

    def test_a2_check_latency_emulation(self):
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'emulate_ping_latencies', str(True))
        self.emulation = 'geographic'
        self.test_a_request_latency()

    def test_a3_request_latency_replication(self):
        """Test replication of name records based on geo-locality"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'replication_interval', str(75))
        self.test_a2_check_latency_emulation()

    def test_b_request_latency_read_coordination(self):
        """[Linearizable consistency] Tests request latency with reads also coordinate with other replicas"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, "read_coordination", str(True))
        self.test_a_request_latency()

    def test_c_request_latency_eventual(self):
        """[Eventual consistency] Tests request latency with an eventual consistency protocol"""
        # not implemented
        assert False
        self.config_parse.set(ConfigParser.DEFAULTSECT, "eventual_consistency", str(True))
        self.test_a_request_latency()

    def test_d_fluid_replication(self):
        """Generate a workload with varying geo-locality and change placement at fast time-scales"""
        # not implemented
        assert False


class RunTestPreConfigured(TestSetupRemote):

    def test_a_run_given_trace(self):
        """Runs an experiment for which config file specifies all setup parameters"""
        # set these parameters
        self.run_exp_multi_lns()


class ConnectTimeMeasureTestDistributed(TestSetupRemote, local_tests.ConnectTimeMeasureTest):
    """Runs connect time measurement in a distributed setup """


def create_empty_trace_files(lns_ids, trace_folder):
    """Creates empty trace files for all local name servers"""
    os.system('mkdir -p ' + trace_folder)
    os.system('rm ' + trace_folder + '/*')
    for lns_id in lns_ids:
        file_name = os.path.join(trace_folder, str(lns_id))
        fw = open(file_name, 'w')
        fw.close()


def append_request_to_all_files(request, lns_ids, trace_folder):
    """Appends request to trace of all files in lns"""
    for lns_id in lns_ids:
        file_name = os.path.join(trace_folder, str(lns_id))
        fw = open(file_name, 'a')
        for t in request:
            fw.write(str(t))
            fw.write('\t')
        fw.write('\n')
        fw.close()


