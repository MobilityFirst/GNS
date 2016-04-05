#!/usr/bin/env python

import os
import sys
import inspect
import time
import argparse

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

# first we initialize parameter in exp_config before importing any local module.
parser = argparse.ArgumentParser("script_one", "Runs a GNS test on a set of remote machines based on config file")
parser.add_argument("config_file", help="config file describing test")
args = parser.parse_args()
print "Config file1:", args.config_file

import exp_config
exp_config.initialize(args.config_file)


from logparse.parse_log import parse_log  # added parent_folder to path to import parse_log module here
from generate_multinode_config_file import generate_multinode_config_file
from run_all_lns import run_all_lns, run_all_ns


def main():
    """Runs this main file."""
    
    output_folder = os.path.join(exp_config.output_folder, 'log')

    exp_time_sec = exp_config.experiment_run_time
    
    lookupTrace = exp_config.lookupTrace
    
    updateTrace = exp_config.updateTrace

    run_one_experiment(output_folder, exp_time_sec, lookupTrace, updateTrace)


def run_one_experiment(output_folder, exp_time_sec, lookupTrace, updateTrace):
    time1 = time.time()
    
    if os.path.exists(output_folder):
        os.system('rm -rf ' + output_folder)
        # print '***** QUITTING!!! Output folder already exists:', output_folder, '*******'
        # sys.exit(2)
    if output_folder.endswith('/'):
        output_folder = output_folder[:-1]
    # copy config files to record experiment configuration
    print 'Creating local output folder: ' + output_folder
    os.system('mkdir -p ' + output_folder)
    
    #os.system(generate_config_script + ' ' +  str(exp_config.load))
    # os.system('cp local-name-server.py name-server.py exp_config.py  ' + output_folder)

    # used for workload generation
    generate_multinode_config_file(exp_config.load)

    os.system('date')
    os.system('./killJava.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file)
    os.system('./rmLog.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file + ' ' +  exp_config.paxos_log_folder + ' ' + exp_config.gns_output_logs)


    # ./cpPl.sh
    os.system('./cpPl.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file + ' ' + exp_config.config_folder + ' ' + exp_config.gns_output_logs) #

    # ./cpWorkload.sh
    os.system('./cpWorkload.sh '+ exp_config.user + ' ' + exp_config.ssh_key + ' ' +  exp_config.lns_file + ' ' + exp_config.gns_output_logs + ' ' + lookupTrace + ' ' + updateTrace)

    #os.system('./cpNameActives.sh ' + name_actives_local  + ' ' + name_actives_remote  + ' ')

    if exp_config.copy_jar:
        remote_jar_folder = os.path.split(exp_config.jar_file_remote)[0]
        print remote_jar_folder
        os.system('./rmcpJar.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file + ' ' + exp_config.jar_file  + ' ' + remote_jar_folder  + ' ' + exp_config.jar_file_remote)
    elif exp_config.download_jar:
        # url of jar file is hardcoded
        # location of jar file is hard coded
        os.system('./getJarS3.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file)

    #run mongo db
    if exp_config.run_db:
        os.system('./kill_mongodb_pl.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.db_folder)
        os.system('./run_mongodb_pl.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.mongo_bin + ' ' + exp_config.db_folder)
        os.system('sleep ' + str(exp_config.mongo_sleep))  # ensures mongo is fully running
        print "Waiting for mongod process to start fully ..."
    elif exp_config.restore_db:
        os.system('./kill_mongodb_pl.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.db_folder)
        os.system('./restore_backup.sh '  + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file + ' ' + exp_config.db_folder_backup + ' ' + exp_config.db_folder)
        os.system('./run_mongodb_pl.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.mongo_bin + ' ' + exp_config.db_folder)
        os.system('sleep ' + str(exp_config.mongo_sleep)) # ensures mongo is fully running

    # ./name-server.sh
    #os.system('./name-server.sh')

    run_all_ns(exp_config.gns_output_logs)
    print 'Name servers running ...'
    try:
        #os.system('sleep ' + str(exp_config.ns_sleep))
        print 'Waiting for NS to load all records for ' + str(exp_config.ns_sleep) + 'sec ..'
        sleep_for_time(exp_config.ns_sleep)
    except:
        print 'NS sleep interrupted. Starting LNS ...'

    # ./local-name-server.sh
    run_all_lns(exp_config.gns_output_logs)

    # this is the excess wait after given duration of experiment
    excess_wait = exp_config.extra_wait
    
    print 'LNS running. Now wait for ', (exp_time_sec + excess_wait)/60, 'min ...'
    
    # sleep for experiment_time
    if exp_time_sec == -1:
        return
    sleep_time = 0
    while sleep_time < exp_time_sec + excess_wait:
        os.system('sleep 60')
        sleep_time += 60
        if sleep_time % 60 == 0: print 'Time = ', sleep_time/60, 'min /', (exp_time_sec + excess_wait)/60, 'min'

    # ./endExperiment.sh output_folder
    print 'Ending experiment ..'
    os.system('./killJava.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file)

    os.system('./getLog.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file + ' ' + output_folder + '  ' + exp_config.gns_output_logs)

    # ./parse_log.py output_folder
    #os.system('logparse/parse_log.py ' + output_folder)
    stats_folder = output_folder + '_stats'
    if output_folder.endswith('/'):
        stats_folder = output_folder[:-1] + '_stats'
    parse_log(output_folder, stats_folder, False)

    time2 = time.time()
    
    diff = time2 - time1
    print 'TOTAL EXPERIMENT TIME:', int(diff/60), 'minutes'
    sys.exit(2)


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
