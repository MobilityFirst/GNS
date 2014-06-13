""" Most critical tests for GNS which generates a workload that has geo-locality and tests whether replication
of name records happens based on geo-locality of names.

This test is best run with a large number of nodes (50 or more) so that we can easily see the vast reduction in request
latency due to locality-based replication.

Limitations:
1. haven't run this on a local machine
2. we only check if the requests are failed/successful but do not check if locality-based replication
 reduces the delay of requests.
"""

import ConfigParser
import os
import sys
import inspect

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

from workload.write_workload import RequestType
from workload.gen_add_requests import gen_add_requests, gen_add_requests_based_on_placement_file
from workload.gen_geolocality_workload import gen_geolocality_trace
from remote_setup import TestSetupRemote


__author__ = 'abhigyan'


class TestWorkloadWithGeoLocality(TestSetupRemote):
    """ Generates workloads with geo-locality and runs experiment on a wide-area platform"""

    def test_a_request_latency(self):
        """[Sequential consistency] Test with a workload that has geo-locality and measures read and write latencies"""

        assert self.lns_geo_file is not None

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
        add_req_rate = 50

        append_request_to_all_files([add_req_rate, RequestType.RATE], lns_ids, self.trace_folder)

        total_time += num_names / len(lns_ids) / add_req_rate

        gen_add_requests(self.trace_folder, number_names=num_names, append_to_file=True, lns_ids=lns_ids,
                         name_prefix=prefix)

        delay = 20000
        append_request_to_all_files([delay, RequestType.DELAY], lns_ids, self.trace_folder)
        total_time += delay / 1000
        read_write_req_rate = 170

        reads_per_epoch_per_name = 800
        writes_per_epoch_per_name = 400
        total_lookups = 0
        total_updates = 0
        num_geo_locality_changes = 3
        for i in range(num_geo_locality_changes):
            num_lookups = num_names * reads_per_epoch_per_name
            num_updates = num_names * writes_per_epoch_per_name
            expected_time = (num_lookups + num_updates) / len(lns_ids) / read_write_req_rate
            print 'Read write send time', expected_time
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

    def test_a2_check_latency_emulation(self):
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'emulate_ping_latencies', str(True))
        self.emulation = 'geographic'
        self.test_a_request_latency()

    def test_a3_request_latency_replication(self):
        """Test replication of name records based on geo-locality"""
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'replication_interval', str(75))
        self.test_a2_check_latency_emulation()

    def test_a4_latency_multiexp(self):
        ns = 24
        lns = 24
        read_write_req_rate = 200
        reads_per_epoch_per_name = 10
        writes_per_epoch_per_name = 5
        num_geo_locality_changes = 1
        min_names = 100
        max_names = 1000000

        names = min_names
        while names <= max_names:
            print '\nRunning experiment:', names, 'names\n'
            self.exp_output_folder = os.path.join(self.local_output_folder, 'multiexp', 'names_' + str(names))
            print 'Exp output folder:', self.exp_output_folder
            self.run_exp_geo_locality(ns, lns, names, read_write_req_rate, reads_per_epoch_per_name,
                                      writes_per_epoch_per_name, num_geo_locality_changes)
            names *= 10

    def test_a5_latency_preconfigured_placement(self):
        """Run experiment with a pre-configured placement."""
        placement_file = "abcd"
        ns = 24
        lns = 24
        names = 100
        req_rate = 10
        reads_per_epoch_per_name = 100
        writes_per_epoch_per_name = 50
        num_geo_locality_changes = 1
        self.run_exp_geo_locality(ns, lns, names, int(req_rate), reads_per_epoch_per_name,
                                  writes_per_epoch_per_name, num_geo_locality_changes, placement_file=placement_file)

    def test_a5_locality_replication_multiexp(self):

        min_names = 100000
        max_names = 100000

        ns = 48
        lns = 48
        physical_servers = 8
        total_capacity = 4000*physical_servers

        reads_per_epoch_per_name = 100
        writes_per_epoch_per_name = 50
        load = 1  # 50% load due to reads and writes

        num_geo_locality_changes = 3

        per_server_capacity = total_capacity/ns
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'max_req_rate', str(per_server_capacity))

        avg_num_replicas = 5

        msg_factor = (writes_per_epoch_per_name*avg_num_replicas + reads_per_epoch_per_name)*1.0 / \
                     (writes_per_epoch_per_name + reads_per_epoch_per_name)

        req_rate = load*total_capacity/lns/msg_factor

        names = min_names
        while names <= max_names:
            print '\nRunning experiment:', names, 'names\n'

            replication_interval = int(names * 30.0 / (total_capacity * 0.1)) + 1
            assert replication_interval > 0
            print 'Replication Interval: ', replication_interval
            self.config_parse.set(ConfigParser.DEFAULTSECT, 'replication_interval', replication_interval)

            self.exp_output_folder = os.path.join(self.local_output_folder, 'replication_exp', 'names_' + str(names))
            print 'Exp output folder:', self.exp_output_folder
            self.run_exp_geo_locality(ns, lns, names, int(req_rate), reads_per_epoch_per_name,
                                      writes_per_epoch_per_name, num_geo_locality_changes)
            names *= 10

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

    def run_exp_geo_locality(self, num_ns, num_lns, num_names, read_write_req_rate,
                             reads_per_epoch_per_name, writes_per_epoch_per_name, num_geo_locality_changes,
                             initial_delay=10, placement_file=None):

        assert self.lns_geo_file is not None
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'emulate_ping_latencies', str(True))
        self.emulation = 'geographic'

        self.ns = num_ns
        self.lns = num_lns

        total_time = 0
        # write workload. decide experiment time
        lns_ids_int = range(self.ns, self.ns + self.lns)
        lns_ids = [str(lns_id) for lns_id in lns_ids_int]

        # num_names = 1000
        prefix = 'test_name'
        # create new trace
        create_empty_trace_files(lns_ids, self.trace_folder)

        # delay = (self.ns + self.lns) * 1000  # (wait so that each node can ping all nodes and get their addresses)
        append_request_to_all_files([initial_delay*1000, RequestType.DELAY], lns_ids, self.trace_folder)
        total_time += initial_delay
        # set a request rate
        add_req_rate = read_write_req_rate/4

        append_request_to_all_files([add_req_rate, RequestType.RATE], lns_ids, self.trace_folder)

        total_time += num_names / len(lns_ids) / add_req_rate

        if placement_file is None:
            gen_add_requests(self.trace_folder, number_names=num_names, append_to_file=True, lns_ids=lns_ids,
                             name_prefix=prefix)
        else:
            gen_add_requests_based_on_placement_file(self.trace_folder, placement_file, lns_ids, append_to_file=True,
                                                     name_prefix=prefix)
        delay = 20  # sec
        append_request_to_all_files([delay * 1000, RequestType.DELAY], lns_ids, self.trace_folder)
        total_time += delay

        total_lookups = 0
        total_updates = 0
        # num_geo_locality_changes = 1
        for i in range(num_geo_locality_changes):
            num_lookups = num_names * reads_per_epoch_per_name
            num_updates = num_names * writes_per_epoch_per_name
            expected_time = (num_lookups + num_updates) / 1.0 / len(lns_ids) / read_write_req_rate
            print 'Expected time', expected_time
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


class TestWorkloadWithGeoLocalityUnreplicatedGNS(TestWorkloadWithGeoLocality):
    """On a local machine, tests a GNS in which both name records and replica controllers are unreplicated"""

    def setUp(self):
        TestWorkloadWithGeoLocality.setUp(self)
        self.config_parse.set(ConfigParser.DEFAULTSECT, 'primary_name_server', str(1))


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
