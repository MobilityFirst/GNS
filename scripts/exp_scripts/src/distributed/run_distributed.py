#!/usr/bin/env python

"""
This module implements the main control program to run a distributed experiment with GNS. It requires several resources
to run an experiment, but it does not generate any of these resources itself. The locations (or values) of these
resources are specified in config file. Its function is to copy the resources to different nodes, start and
stop different components, and copy back the result at the end of an experiment.

Resources:

(1) list of name server, local name server nodes
(2) user name to login to machines
(3) ssh key
(4) location of mongodb, and java bin folders on those machines
(5) location of jar file on local machine
(6) remote folder location where gns output is stored
(7) folder where node config files of each name server/local name server is stored
(8) workload for running experiments: lookup trace folder and update trace folder for each LNS
(9) workload for running experiments: a file describing other workload parameters, e.g. object size, arrival process.

NOTE: this script can only be run from its own folder.
"""

import os
import sys
import inspect
import time
import argparse

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)


import exp_config
from logparse.parse_log import parse_log  # added parent_folder to path to import parse_log module here
from run_all_nodes import run_all_lns, run_all_ns
import copy_workload


def main():
    # first we initialize parameter in exp_config before importing any local module.
    parser = argparse.ArgumentParser("script_one", "Runs a GNS test on a set of remote machines based on config file")
    parser.add_argument("config_file", help="config file describing test")
    args = parser.parse_args()
    local_config_file = args.config_file
    exp_config.initialize(local_config_file)
    output_folder = os.path.join(exp_config.local_output_folder, 'log')
    run_one_experiment(output_folder, local_config_file)


def run_one_experiment(local_output_folder, local_config_file):

    exp_time_sec = exp_config.experiment_run_time
    time1 = time.time()

    if os.path.exists(local_output_folder):
        os.system('rm -rf ' + local_output_folder)
        # print '***** QUITTING!!! Output folder already exists:', output_folder, '*******'
        # sys.exit(2)
    if local_output_folder.endswith('/'):
        local_output_folder = local_output_folder[:-1]
    # copy config files to record experiment configuration
    print 'Creating local output folder: ' + local_output_folder
    os.system('mkdir -p ' + local_output_folder)

    ns_ids, lns_ids = get_node_ids(exp_config.local_ns_file, exp_config.local_lns_file)

    remote_workload_config = os.path.split(exp_config.wfile)[1]

    REMOTE_NODE_CONFIG = 'node_config'

    REMOTE_UPDATE_TRACE = 'request_trace'

    remote_config_file = os.path.join(exp_config.remote_gns_logs, os.path.split(local_config_file)[1])

    os.system('date')
    os.system('./killJava.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.local_ns_file + ' ' +
              exp_config.local_lns_file)

    if exp_config.clean_start:
        print 'Doing clean start of GNS ... '
        os.system('./rmLog.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.local_ns_file + ' ' +
                  exp_config.local_lns_file + ' ' + exp_config.paxos_log_folder + ' ' + exp_config.remote_gns_logs)
        os.system('./kill_mongodb.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.local_ns_file
                  + ' ' + exp_config.db_folder)
        os.system('./run_mongodb.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.local_ns_file
                  + ' ' + exp_config.remote_mongo_bin + ' ' + exp_config.db_folder + ' ' + str(exp_config.mongo_port))
        print "Waiting for mongod process to start fully ..."
        os.system('sleep ' + str(exp_config.mongo_sleep))  # ensures mongo is fully running

    # copy scripts and config files ...
    cmd = './cpScriptsConfigs.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.local_ns_file + ' '\
              +  exp_config.local_lns_file + ' ' + local_config_file + ' ' + exp_config.wfile + ' ' + exp_config.node_config_folder + ' ' + \
          exp_config.remote_gns_logs + ' ' + REMOTE_NODE_CONFIG
    os.system(cmd)  #

    # copy workload for local name servers ....
    copy_workload.copy_workload(exp_config.user, exp_config.ssh_key, lns_ids, exp_config.update_trace,
                                exp_config.remote_gns_logs, REMOTE_UPDATE_TRACE)
    # os.system('./cpWorkload.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.local_lns_file + ' '
    #           + exp_config.remote_gns_logs + ' ' + exp_config.update_trace + ' ' + REMOTE_UPDATE_TRACE)

    # copy GNS jar file ....
    if exp_config.copy_jar:
        remote_jar_folder = os.path.split(exp_config.remote_jar_file)[0]
        print remote_jar_folder
        os.system('./rmcpJar.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.local_ns_file + ' ' +
                  exp_config.local_lns_file + ' ' + exp_config.local_jar_file + ' ' + remote_jar_folder + ' ' + exp_config.remote_jar_file)
    elif exp_config.download_jar:
        # url of jar file is hardcoded
        # location of jar file is hard coded
        os.system('./getJarS3.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.local_ns_file + ' ' +
                  exp_config.local_lns_file)

    # run name servers ....
    run_all_ns(exp_config.user, exp_config.ssh_key, ns_ids, exp_config.remote_gns_logs, remote_config_file,
               REMOTE_NODE_CONFIG)
    print 'Name servers running ...'
    try:
        #os.system('sleep ' + str(exp_config.ns_sleep))
        print 'Waiting for name servers to start ' + str(exp_config.ns_sleep) + 'sec ..'
        sleep_for_time(exp_config.ns_sleep)
    except:
        print 'NS sleep interrupted. Starting LNS ...'

    # run local name servers ....
    run_all_lns(exp_config.user, exp_config.ssh_key, lns_ids, exp_config.remote_gns_logs, remote_config_file,
                REMOTE_NODE_CONFIG, REMOTE_UPDATE_TRACE, remote_workload_config)

    # this is the excess wait after given duration of experiment
    excess_wait = exp_config.extra_wait

    print 'LNS running. Now wait for ', (exp_time_sec + excess_wait) / 60, 'min ...'

    # sleep for experiment_time
    if exp_time_sec == -1:
        return
    sleep_time = 0
    while sleep_time < exp_time_sec + excess_wait:
        os.system('sleep 60')
        sleep_time += 60
        if sleep_time % 60 == 0: print 'Time = ', sleep_time / 60, 'min /', (exp_time_sec + excess_wait) / 60, 'min'

    print 'Ending experiment ..'
    os.system('./killJava.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.local_ns_file + ' ' + exp_config.local_lns_file)

    os.system('./getLog.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.local_ns_file + ' ' + exp_config.local_lns_file + ' ' + local_output_folder + '  ' + exp_config.remote_gns_logs)

    print 'Output stats ...'
    stats_folder = local_output_folder + '_stats'
    if local_output_folder.endswith('/'):
        stats_folder = local_output_folder[:-1] + '_stats'
    parse_log(local_output_folder, stats_folder)

    time2 = time.time()

    diff = time2 - time1
    print 'TOTAL EXPERIMENT TIME:', int(diff / 60), 'minutes'
    sys.exit(2)


def get_node_ids(local_ns_file, local_lns_file):

    ns_ids = {}
    lns_ids = {}
    for i, line in enumerate(open(local_ns_file)):
        ns_ids[i] = line.strip()

    for i, line in enumerate(open(local_lns_file)):
        lns_ids[i + len(ns_ids)] = line.strip()
    return ns_ids, lns_ids


def sleep_for_time(sleep_time):
    if sleep_time < 10:
        time.sleep(sleep_time)
        return
    count = int(sleep_time / 10)

    for j in range(count):
        time.sleep(10)
        print 'Sleep ', str(j * 10), ' sec / ' + str(sleep_time) + ' sec'


if __name__ == "__main__":
    main()