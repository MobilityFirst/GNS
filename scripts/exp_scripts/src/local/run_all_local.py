#!/usr/bin/env python2.7

import os
import sys
import inspect
import time
import argparse

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

import exp_config

# first we initialize parameter in exp_config before importing any local module.

parser = argparse.ArgumentParser("run_all_local", "Runs a GNS setup on local machine based on config file")
parser.add_argument("config_file", help="config file describing all experiments")
args = parser.parse_args()
print "Config file:", args.config_file
#print "Output folder:", args.output_folder
exp_config.initialize(args.config_file)

from generate_config_file import write_local_config_file
import name_server
import local_name_server

from logparse.parse_log import parse_log  # added parent_folder to path to import parse_log module here


def run_exp():
    from kill_local import kill_local_gnrs
    kill_local_gnrs()
    node_config = exp_config.node_config
    # folder where GNS logs are output
    log_output_folder = os.path.join(exp_config.output_folder, exp_config.DEFAULT_GNS_OUTPUT_FOLDER)
    # folder where GNS stats are output
    stats_folder = os.path.join(exp_config.output_folder, exp_config.DEFAULT_STATS_FOLDER)

    print 'Clearing old GNS logs ..'
    if not os.path.exists(log_output_folder):
        os.system('mkdir -p ' + log_output_folder)
    os.system('rm -rf ' + log_output_folder + '/*')

    if exp_config.clean_start:
        print 'Doing clean start ...'
        os.system('rm -rf ' + exp_config.paxos_log_folder + '/*')
        os.system(script_folder + '/kill_mongodb_local.sh ' + exp_config.mongodb_data_folder)
        os.system(script_folder + '/run_mongodb_local.sh ' + exp_config.mongo_bin_folder + ' ' +
                  exp_config.mongodb_data_folder + ' ' + str(exp_config.mongo_port))
    else:
        print 'Resuming gns ...'

    # generate config file
    write_local_config_file(exp_config.node_config, exp_config.num_ns, exp_config.num_lns,
                            exp_config.const_latency_value, exp_config.random_node_ids)

    # generate workloads
    # todo move workload generation from here
    # generate_all_traces()

    #os.system('sleep 100')
    #os.system('pssh -h hosts.txt "killall -9 java"')
    f = open(node_config)
    first_lns = False
    for line in f:
        tokens = line.split()
        id = tokens[0]
        hostname = tokens[2]
        if tokens[1] == 'yes':
            if is_failed_node(id):
                print 'NODE FAILED = ', id
                continue
            name_server.run_name_server(id, log_output_folder)
        else:
            if first_lns is False:
                time.sleep(exp_config.ns_sleep)  # sleep so that name servers load name records into DB
                first_lns = True
            update_trace = None
            if exp_config.is_experiment_mode and exp_config.experiment_run_time >= 0:
                update_trace = get_update_trace(exp_config.trace_folder, id)
            local_name_server.run_local_name_server(id, log_output_folder, update_trace)

    # kill local
    if not exp_config.is_experiment_mode or exp_config.experiment_run_time < 0:
        print 'Not experiment mode. GNS running.'
        return
        
    print 'Sleeping until experiment finishes ...'
    time.sleep(exp_config.experiment_run_time + exp_config.extra_wait)

    kill_local_gnrs()

    # if exp_config.output_folder.endswith('/'):
    #     stats_folder = exp_config.output_folder[:-1] + '_stats'
    parse_log(log_output_folder, stats_folder)
    # parse logs and generate output

    # not doing restart stuff right now
    sys.exit(2)
    restart_period = 20
    number_restarts = 1
    restart_node = '2'
    for i in range(number_restarts):
        time.sleep(restart_period)
        print 'Restarting node ' + restart_node 
        run_name_server(restart_node, log_output_folder, node_config)
        print 'Done'


def get_update_trace(trace_folder, node_id):
    update_trace = os.path.join(trace_folder, node_id)
    if os.path.exists(update_trace):
        return update_trace
    return None


def generate_all_traces():
    if exp_config.gen_workload:
        for i in range(exp_config.num_lns):
            lns_id = i + exp_config.num_ns
            generate_trace(lns_id)


def generate_trace(lns_id):

    from generate_sequence import write_single_name_trace, write_random_name_trace
    name = '0'
    number = exp_config.lookup_count
    filename = os.path.join(exp_config.trace_folder, 'lookupLocal/' + str(lns_id))
    if exp_config.regular_workload + exp_config.mobile_workload == 1:
        write_single_name_trace(filename, number, name)
    else:
        write_random_name_trace(filename, exp_config.regular_workload + exp_config.mobile_workload, number)

    number = exp_config.update_count
    filename = os.path.join(exp_config.trace_folder, 'updateLocal/' + str(lns_id))
    if exp_config.regular_workload + exp_config.mobile_workload == 1:
        write_single_name_trace(filename, number, name)
    else:
        write_random_name_trace(filename, exp_config.regular_workload + exp_config.mobile_workload, number)


def is_failed_node(node_id):
    node_id = int(node_id)
    if exp_config.failed_nodes is None:
        return False
    for node_id1 in exp_config.failed_nodes:
        if node_id1 == node_id:
            return True
    return False

run_exp()
