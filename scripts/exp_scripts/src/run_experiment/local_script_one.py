#!/usr/bin/env python
import os
import sys
import exp_config
from run_multi_local import restart_name_server
from generate_local_config_file import generate_local_config_file

wait_time = exp_config.extra_wait

restart = True
restart_delay = 300
restart_ns = exp_config.quit_node_id

def main():
    ns = exp_config.num_ns
    lns = exp_config.num_lns
    load = exp_config.load
    output_folder = exp_config.local_output_folder
    
    #'/home/abhigyan/gnrs/results/local_beehive_bugfix'
    exp_time_sec = exp_config.experiment_run_time
    run_local_experiment(ns, lns, load, output_folder, exp_time_sec)

        
def run_local_experiment(ns, lns, load, output_folder, exp_time_sec):
    global restart

    if os.path.exists(output_folder):
        print 'ERROR: Output folder already exists:', output_folder
        sys.exit(2)
    
    # copy config files to record experiment configuration
    os.system('mkdir -p ' + output_folder)
    os.system('cp local-name-server.py name-server.py exp_config.py ' + output_folder)
    
    # generate local config file
    generate_local_config_file(ns, lns, load)
    
    from get_norm_value import get_normalizing_constant_value
    normalizing_constant = get_normalizing_constant_value(load)
    
    # clear paxos logs
    print 'Deleting paxos logs: pssh -h hosts_ns.txt  "rm -rf ' + exp_config.paxos_log_folder + '/*'
    os.system('pssh -h hosts_ns.txt  "rm -rf ' + exp_config.paxos_log_folder + '/*"')
    
    # run NS and LNS
    from run_multi_local import run_all_experiments
    run_all_experiments(output_folder, normalizing_constant)
    if exp_time_sec == -1: 
        return
    # Wait for experiment to get over
    sleep_time = 0
    while sleep_time < exp_time_sec + wait_time:
        os.system('sleep 10')
        sleep_time += 10
        if sleep_time % 60 == 0:
            print 'Time = ', sleep_time/60, 'min /', (exp_time_sec + wait_time)/60, 'min'
        if restart and sleep_time >= restart_delay:
            restart = False
            restart_name_server(str(restart_ns), output_folder, normalizing_constant)

    # Kill programs
    os.system('pssh -h /home/abhigyan/gnrs/hosts_lns.txt "killall -9 java"')
    os.system('pssh -h /home/abhigyan/gnrs/hosts_ns.txt "killall -9 java"')

    
    # Parse log 
    os.system('/home/abhigyan/gnrs/logparse/parse_log.py ' + output_folder + ' ' + output_folder + '_stats local')




if __name__ == '__main__':
    main()
