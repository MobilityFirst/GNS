"""Includes tests to measure latency and throughput of common operations in GNS (add, read, write, remove, groupchange).
Tests in this module can be run either on a local machine or on a set of remote distributed machines. In distributed
case, these tests should be run with a small number of  machines (around 10). These tests do not evaluate
geo-distributed replication features of GNS. """


import ConfigParser
import os
import sys
import inspect

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)


from workload.write_workload import RequestType, WorkloadParams
from test_utils import *
from util.exp_events import NodeCrashEvent, write_events_to_file
from remote_setup import TestSetupRemote
from local_setup import TestSetupLocal

__author__ = 'abhigyan'


class ThroughputTestDistributed(TestSetupRemote):
    """ Tests throughput of basic operations: Add, remove, lookup, delete.
    For each operation, we run a series of tests doubling the request rate each time until
    maximum throughput is reached. These tests take tens of minutes to complete due to multiple
    tests with each operation. """

    def test_a_latency(self):
        """ Measures read and write latency for varying number of replicas at moderate load"""

        # num_replica_set = range(3, self.ns + 1, 2)
        num_replica_set = [3]
        print "Replica set: ", num_replica_set
        for num_replica in num_replica_set:
            output_dir = 'read_write_latency/replica_' + str(num_replica)
            # output from experiment will go in this folder
            self.exp_output_folder = os.path.join(self.local_output_folder, output_dir)
            print 'Testing read/write latency with replicas =', num_replica, 'Folder =', self.exp_output_folder
            self.run_latency_test(num_replica, num_names=1000, req_rate=100, num_requests=1000)

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
        num_names = 1000
        num_replica_set = range(3, self.ns + 1, 2)
        print "Replica set: ", num_replica_set
        for num_replica in num_replica_set:
            output_dir = 'group_change_latency/replica_' + str(num_replica)
            # output from experiment will go in this folder
            self.exp_output_folder = os.path.join(self.local_output_folder, output_dir)
            print 'Testing group change latency with replicas =', num_replica, 'Folder =', self.exp_output_folder
            self.run_group_change_test(num_replica, num_names)

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
        req_rate = 20
        delay = 5000
        names = []

        for i in range(num_names):
            names.append('test_name' + str(i))

        request_set = []

        request_set.append([req_rate, RequestType.RATE])

        from random import shuffle

        # add all names
        request_set.extend([[name, RequestType.ADD] for name in names])
        total_time += len(names) / req_rate
        request_set.append([delay, RequestType.DELAY])
        total_time += delay / 1000

        # send group changes in random order
        req_rate = 100
        request_set.append([req_rate, RequestType.RATE])
        initial_version = 1
        group_size = min(num_replica, self.ns)
        assert group_size <= self.ns
        num_group_changes = 10

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


class ThroughputTestLocal(TestSetupLocal, ThroughputTestDistributed):
    """ Runs throughput tests on a local machine"""

