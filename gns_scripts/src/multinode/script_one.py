#!/usr/bin/env python
import os
import sys
import time

import exp_config
from generate_ec2_config_file import generate_ec2_config_file
from run_all_lns import run_all_lns, run_all_ns

import inspect
script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]

log_parse_script='/home/abhigyan/gnrs/logparse/parse_log.py'
sys.path.append(parent_folder)
from logparse.parse_log import parse_log  # added parent_folder to path to import parse_log module here

def main():
    """Runs this main file."""
    
    output_folder = os.path.join(exp_config.output_folder, 'log')

    exp_time_sec = exp_config.experiment_run_time
    
    lookupTrace = exp_config.lookupTrace
    
    updateTrace = exp_config.updateTrace
    
    # write local-name-server.py, name-server.py    
    #lns_py = 'local-name-server.py'
    #ns_py = 'name-server.py'
    run_one_experiment(output_folder, exp_time_sec, lookupTrace, updateTrace)


def run_one_experiment(output_folder, exp_time_sec, lookupTrace, updateTrace):
    time1 = time.time()
    
    if os.path.exists(output_folder):
        os.system('rm -rf ' + output_folder)
        #print '***** QUITTING!!! Output folder already exists:', output_folder, '*******'
        #sys.exit(2)
    if output_folder.endswith('/'):
        output_folder = output_folder[:-1]
    
    #run mongo db
    if exp_config.run_db:
        os.system('./kill_mongodb_pl.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.db_folder)

        os.system('./run_mongodb_pl.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.db_folder)
        os.system('sleep ' + str(exp_config.mongo_sleep)) # ensures mongo is fully running
    
    # copy config files to record experiment configuration
    print 'Creating local output folder: ' + output_folder
    os.system('mkdir -p ' + output_folder)
    
    #os.system(generate_config_script + ' ' +  str(exp_config.load))
    os.system('cp local-name-server.py name-server.py exp_config.py  ' + output_folder)

    # used for workload generation
    generate_ec2_config_file(exp_config.load)
    
    
    os.system('date')
    os.system('./killJava.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file)
    os.system('./rmLog.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file + ' ' +  exp_config.paxos_log_folder + ' ' + exp_config.gns_output_logs)
            
    # ./cpPl.sh
    os.system('./cpPl.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file + ' ' + exp_config.config_folder + ' ' + exp_config.gns_output_logs) #
    
    # ./cpWLU.sh
    os.system('./cpWLU.sh '+ exp_config.user + ' ' + exp_config.ssh_key + ' ' +  exp_config.lns_file + ' ' + exp_config.gns_output_logs + ' ' + lookupTrace + ' ' + updateTrace)
    #

    # ./cpNameActives.sh
    #name_actives_local = exp_config.name_actives_local # should be uncompressed filename
    #name_actives_remote = exp_config.name_actives_remote
    
    #if os.path.exists(name_actives_local): # if uncompressed version exists: compress it
    #    os.system('gzip -f ' + name_actives_local)
    #name_actives_local = exp_config.name_actives_local + '.gz' 
    #if not os.path.exists(name_actives_local):
    #    print 'Name actives does not exist:', name_actives_local
    #    sys.exit(2)
    
    #os.system('./cpNameActives.sh ' + name_actives_local  + ' ' + name_actives_remote  + ' ')

    if exp_config.copy_jar:
        remote_jar_folder = os.path.split(exp_config.jar_file_remote)[0]
        print remote_jar_folder
        os.system('./rmcpJar.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file + ' ' + exp_config.jar_file  + ' ' + remote_jar_folder  + ' ' + exp_config.jar_file_remote)
        
        #os.system('./getJarS3.sh')
    
            # +' 1>/dev/null 2>/dev/null')
    #sys.exit(2)
    # start cpu use monitoring
    #os.system('./run_mpstat.sh ' + exp_config.hosts_ns_file + ' ' + exp_config.remote_cpu_folder)

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
        sys.exit(2)

    # ./local-name-server.sh
    #os.system('./local-name-server.sh')
    run_all_lns(exp_config.gns_output_logs)

    # this is the excess wait after given duration of experiment
    excess_wait = exp_config.extra_wait
    
    print 'LNS running. Now wait for ', (exp_time_sec + excess_wait)/60, 'min ...'
    
    # sleep for experiment_time
    if exp_time_sec == -1:
        return
    sleep_time = 0
    while sleep_time < exp_time_sec + excess_wait:
        os.system('sleep 10')
        sleep_time += 10
        if sleep_time % 60 == 0: print 'Time = ', sleep_time/60, 'min /', (exp_time_sec + excess_wait)/60, 'min'

    # ./endExperiment.sh output_folder
    print 'Ending experiment ..'
    os.system('./killJava.sh ' + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file)

    os.system('./getLog.sh '  + exp_config.user + ' ' + exp_config.ssh_key + ' ' + exp_config.ns_file  + ' ' + exp_config.lns_file + ' ' + output_folder + '  ' + exp_config.gns_output_logs)
    
    # stop cpu use monitoring and copy cpu use data
    #os.system('./kill_mpstat.sh ' + exp_config.hosts_ns_file + ' ' + exp_config.remote_cpu_folder + ' ' + exp_config.local_cpu_folder)

    #os.system('./kill_mongodb_pl.sh')
    
    # ./parse_log.py output_folder
    #os.system('logparse/parse_log.py ' + output_folder)
    stats_folder = output_folder + '_stats'
    if output_folder.endswith('/'):
        stats_folder = output_folder[:-1] + '_stats'
    parse_log(output_folder, stats_folder, True)

    #os.system(log_parse_script + ' ' + output_folder  + ' ' + output_folder + '_stats local')

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
