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
from nodeconfig.node_config_writer import deployment_config_writer
from nodeconfig.node_config_latency_calculator import default_latency_calculator
from workload.write_workload import RequestType, WorkloadParams, workload_writer
from test_utils import *

import local_tests

__author__ = 'abhigyan'

class TestSetupRemote(local_tests.BasicSetup):
    """ Performs common setup tasks for running distributed tests. Other tests use this as base class"""

    # list of nodes to run name server
    ns_file = None

    # list of nodes to run local name server
    lns_file = None

    def setUp(self):

        print "Command line arguments: ",  sys.argv
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

    def run_exp(self, requests):
        """Not a test. Run experiments given a set of requests."""
        tmp_nsfile = self.ns_file
        if self.ns != get_line_count(self.ns_file):
            # write a new ns file with only 'self.ns' name servers
            tmp_nsfile = '/tmp/ns_file'
            fw = open(tmp_nsfile, 'w')
            for i, line in enumerate(open(self.ns_file)):
                fw.write(line)
                if i == self.ns - 1:
                    break
            fw.close()

            self.config_parse.set(ConfigParser.DEFAULTSECT, 'ns_file', tmp_nsfile)

        assert self.ns == get_line_count(tmp_nsfile)

        lns_id = self.ns
        workload_writer({lns_id: requests}, self.trace_folder)

        node_config_file = os.path.join(self.local_output_folder, 'node_config_file')
        deployment_config_writer(tmp_nsfile, self.lns_file, node_config_file)

        node_config_folder = os.path.join(self.local_output_folder, 'node_config_folder')
        default_latency_calculator(node_config_file, node_config_folder, filename_id=False)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'config_folder', node_config_folder)
        temp_workload_config_file = os.path.join(self.local_output_folder, 'tmp_w.ini')
        self.workload_conf.write(open(temp_workload_config_file, 'w'))

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'wfile', temp_workload_config_file)

        temp_config_file = os.path.join(self.local_output_folder, 'tmp.ini')

        if self.exp_output_folder is not None and self.exp_output_folder != '':
            self.config_parse.set(ConfigParser.DEFAULTSECT, 'local_output_folder', self.exp_output_folder)

        # write the config file here
        self.config_parse.write(open(temp_config_file, 'w'))

        outfile = os.path.join(self.local_output_folder, 'test.out')
        errfile = os.path.join(self.local_output_folder, 'test.err')
        exp_folder = os.path.join(parent_folder, 'distributed')
        # the script 'run_distributed.py' can only be run from its current folder. so we 'cd' to its folder
        os.system('cd ' + exp_folder + '; ./run_distributed.py ' + temp_config_file)  # + ' > ' + outfile + ' 2> '
        # + errfile
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

    def test_a_read_write_latency(self):
        """ Measures read and write latency for varying number of replicas at low load"""

        num_names = 10000
        num_replica_set = range(3, self.ns + 1, 2)
        print "Replica set: ", num_replica_set
        for num_replica in num_replica_set:
            output_dir = 'read_write_latency/replica_' + str(num_replica)
            # output from experiment will go in this folder
            self.exp_output_folder = os.path.join(self.local_output_folder, output_dir)
            print 'Testing read/write latency with replicas =', num_replica, 'Folder =', self.exp_output_folder
            self.run_latency_test(num_replica, num_names)

    def test_b_read_write_latency(self):
        """ Measures read latency when we replicas coordinate on reads also"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'read_coordination', True)
        self.test_a_read_write_latency()

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
            self.run_group_change_test(num_replica, num_names)

    def test_d_write_throughput(self):
        """ Run write throughput tests for varying number of replicas of name.
        Number of name servers equals number of replicas so that all names are replicated at all locations."""

        num_names = 10000
        num_replica_set = range(5, self.ns + 1, 2)
        print "Replica set: ", num_replica_set
        for num_replica in num_replica_set:
            print 'Testing write throughput with replicas =', num_replica
            self.run_write_throughput_test(num_replica, num_names)


    def test_e_coordinated_read_throughput(self):
        """ Run (coordinated) read throughput tests for varying number of name servers.
        Number of name servers equals number of replicas so that all names are replicated at all locations."""

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'read_coordination', True)

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

    def test_g_object_size_throughput(self):
        """ Test write throughput with a 1KB object size. Other experiments are with a 10-byte object size"""

        size_kb = 4
        print 'Testing throughput with object size: ', size_kb, 'KB'
        self.workload_conf.set(ConfigParser.DEFAULTSECT, WorkloadParams.OBJECT_SIZE, size_kb)
        self.test_d_write_throughput()

    ######## methods below are not tests. they are helper methods for running tests above

    def run_latency_test(self, num_replica, num_names):
        """Measures latency of add, remove, lookup, and delete operations with given number
        of names such that a name is replicated at given number of nodes. It tests if all requests are successful"""

        self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', num_replica)

        total_time = 0
        req_rate = 500
        delay = 5000
        names = []

        for i in range(num_names):
            names.append('test_name' + str(i))

        request_set = [[req_rate, RequestType.RATE]]

        from random import shuffle

        # add all names
        shuffle(names)
        request_set.extend([[name, RequestType.ADD] for name in names])
        total_time += len(names)/req_rate
        request_set.append([delay, RequestType.DELAY])
        total_time += delay/1000

        # send lookups in random order
        for i in range(3):
            shuffle(names)
            request_set.extend([[name, RequestType.LOOKUP] for name in names])
            total_time += len(names)/req_rate

        request_set.append([delay, RequestType.DELAY])
        total_time += delay/1000

        # send updates in random order
        for i in range(3):
            shuffle(names)
            request_set.extend([[name, RequestType.UPDATE] for name in names])
            total_time += len(names)/req_rate

        request_set.append([delay, RequestType.DELAY])
        total_time += delay/1000

        shuffle(names)
        request_set.extend([[name, RequestType.REMOVE] for name in names])
        total_time += len(names)/req_rate
        request_set.append([delay, RequestType.DELAY])
        total_time += delay/1000

        print 'Total time', total_time
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', total_time)  # in seconds
        output_stats = self.run_exp(request_set)
        self.assertEqual(output_stats.requests, num_names * 8, "Total requests mismatch")
        self.assertEqual(output_stats.success, num_names * 8, "Successful requests mismatch")
        self.assertEqual(output_stats.read, num_names * 3, "Successful reads mismatch")
        self.assertEqual(output_stats.write, num_names * 3, "Successful writes mismatch")
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
        total_time += len(names)/req_rate
        request_set.append([delay, RequestType.DELAY])
        total_time += delay/1000

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
            total_time += len(names)/req_rate

        request_set.append([delay, RequestType.DELAY])
        total_time += delay/1000

        print 'Total time', total_time
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', total_time)  # in seconds
        output_stats = self.run_exp(request_set)

        self.assertEqual(output_stats.requests, num_names * (1 + num_group_changes), "Total requests mismatch")
        self.assertEqual(output_stats.success, num_names * (1 + num_group_changes), "Successful requests mismatch")
        self.assertEqual(output_stats.add, num_names, "Successful adds mismatch")
        self.assertEqual(output_stats.group_change, num_group_changes*num_names, "Successful group change mismatch")

    def run_write_throughput_test(self, num_replica, num_names):
        """Tests throughput of write requests for a given number of replicas of name"""
        self.ns = num_replica
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', num_replica)
        min_rate = 0
        max_rate = 1500
        width = 250
        duration = 100
        while max_rate - min_rate > width:
            mid_rate = (max_rate + min_rate)/2
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
        delay = 5000
        names = ['test_name' + str(i) for i in range(num_names)]

        # add all names
        add_rate = 250
        request_set = [[500, RequestType.RATE]]   # add requests are sent at fixed rate so that add does not become
                                                  # bottleneck.

        request_set.extend([[name, RequestType.ADD] for name in names])
        total_time += len(names)/add_rate

        request_set.append([delay, RequestType.DELAY])
        total_time += delay/1000

        request_set.append([req_rate, RequestType.RATE])   # send write requests at given rate
        # send 'num_writes' write requests, choosing names randomly
        num_writes = req_rate * duration
        for i in range(num_writes):
            name = names[random.randint(0, len(names) - 1)]
            request_set.append([name, RequestType.UPDATE])
        total_time += duration

        request_set.append([delay, RequestType.DELAY])
        total_time += delay/1000

        print 'Total time', total_time
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', total_time)  # in seconds
        output_stats = self.run_exp(request_set)

        self.assertEqual(output_stats.requests, num_names + num_writes, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, num_names + num_writes,  "Successful requests mismatch")
        self.assertEqual(output_stats.write, num_writes, "Successful writes mismatch")
        self.assertEqual(output_stats.add, num_names, "Successful adds mismatch")

    def run_coordinated_read_throughput_test(self, num_replica, num_names):
        """Tests throughput of coordinated read requests for a given number of replicas of name"""
        self.ns = num_replica
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', num_replica)
        min_rate = 2000
        max_rate = 10000
        width = 250
        duration = 100
        while max_rate - min_rate > width:
            mid_rate = (max_rate + min_rate)/2
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
        request_set = [[500, RequestType.RATE]]   # add requests are sent at fixed rate so that add does not become
                                                  # bottleneck.

        request_set.extend([[name, RequestType.ADD] for name in names])
        total_time += len(names)/add_rate

        request_set.append([delay, RequestType.DELAY])
        total_time += delay/1000

        request_set.append([req_rate, RequestType.RATE])   # send read requests at given rate
        # send 'num_reads' read requests, choosing names randomly
        num_reads = req_rate * duration
        for i in range(num_reads):
            name = names[random.randint(0, len(names) - 1)]
            request_set.append([name, RequestType.LOOKUP])
        total_time += duration

        request_set.append([delay, RequestType.DELAY])
        total_time += delay/1000

        print 'Total time', total_time
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', total_time)  # in seconds
        output_stats = self.run_exp(request_set)

        self.assertEqual(output_stats.requests, num_names + num_reads, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, num_names + num_reads,  "Successful requests mismatch")
        self.assertEqual(output_stats.read, num_reads, "Successful reads mismatch")
        self.assertEqual(output_stats.add, num_names, "Successful adds mismatch")

    def run_group_change_throughput_test(self, group_size, num_names):
        """ Tests throughput of group change requests for a given group size"""
        min_rate = 500
        max_rate = 1500
        width = 125
        duration = 100
        while max_rate - min_rate > width:
            mid_rate = (max_rate + min_rate)/2
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
        request_set = [[500, RequestType.RATE]]   # add requests are sent at fixed rate so that add does not become
                                                  # bottleneck.

        request_set.extend([[name, RequestType.ADD] for name in names])
        total_time += len(names)/add_rate

        request_set.append([delay, RequestType.DELAY])
        total_time += delay/1000

        request_set.append([req_rate, RequestType.RATE])   # send group change requests at given rate

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
        total_time += delay/1000

        print 'Total time', total_time
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'experiment_run_time', total_time)  # in seconds
        output_stats = self.run_exp(request_set)

        self.assertEqual(output_stats.requests, num_names + group_change_requests, "Total number of requests mismatch")
        self.assertEqual(output_stats.success, num_names + group_change_requests,  "Successful requests mismatch")
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
        """ Runs an experiment which first add a set of names to GNS and then removes them.
        """
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

