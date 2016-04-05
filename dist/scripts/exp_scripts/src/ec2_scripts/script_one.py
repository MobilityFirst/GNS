#!/usr/bin/env python
import os
import sys
import time

import exp_config
from generate_ec2_config_file import generate_ec2_config_file
from run_all_lns import run_all_lns, run_all_ns

log_parse_script='/home/abhigyan/gnrs/logparse/parse_log.py'

def main():
    """Runs this main file."""
    
    output_folder = exp_config.output_folder
    
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
        print '***** QUITTING!!! Output folder already exists:', output_folder, '*******'
        sys.exit(2)
    
    #run mongo db
    #os.system('./kill_mongodb_pl.sh')
    #os.system('./run_mongodb_pl.sh')
    #os.system('sleep ' + str(exp_config.mongo_sleep))
    
    # copy config files to record experiment configuration
    os.system('mkdir -p ' + output_folder)
    
    #os.system(generate_config_script + ' ' +  str(exp_config.load))
    os.system('cp local-name-server.py name-server.py exp_config.py nameActives ' + output_folder)
    
    generate_ec2_config_file(exp_config.load)
    
    os.system('date')
    # ./cpPl.sh
    os.system('./cpPl.sh ' + exp_config.config_folder)
    
    # ./cpWorkload.sh
    os.system('./cpWorkload.sh ' + lookupTrace + ' ' + updateTrace  + '&')
    
    if exp_config.download_jar:
        os.system('./getJarS3.sh')
    # ./rmLog.sh
    os.system('./rmLog.sh ' +  exp_config.paxos_log_folder + ' ' + exp_config.gns_output_logs +' 1>/dev/null 2>/dev/null')
    
    # ./name-server.sh
    #os.system('./name-server.sh')
    run_all_ns(exp_config.gns_output_logs)

    #print 'NS running. sleep sleep ...'
    #exit(2)
    os.system('sleep ' + str(exp_config.ns_sleep))
    
    # ./local-name-server.sh
    #os.system('./local-name-server.sh')
    run_all_lns(exp_config.gns_output_logs, exp_config.num_ns)
    
    # this is the excess wait after experiment finishes
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
    os.system('./endExperiment.sh ' + output_folder + '  ' + exp_config.gns_output_logs)
    #os.system('./kill_mongodb_pl.sh')
    
    # ./parse_log.py output_folder
    #os.system('logparse/parse_log.py ' + output_folder)
    os.system(log_parse_script + ' ' + output_folder)
    
    time2 = time.time()
    
    diff = time2 - time1
    print 'TOTAL EXPERIMENT TIME:', int(diff/60), 'minutes'


if __name__ == "__main__":
    main()
