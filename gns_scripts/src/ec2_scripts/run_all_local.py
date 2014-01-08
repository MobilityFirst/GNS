#!/usr/bin/env python2.7
import os
import sys
from os.path import join
import time
import exp_config

gnrs_dir = exp_config.gnrs_dir

def main():
    from kill_local import kill_local_gnrs
    kill_local_gnrs()
    config_file = join(gnrs_dir, 'local/local_config')
    working_dir = join(gnrs_dir, 'local/log_local/')
    print 'Clearing old GNS logs ..'
    if not os.path.exists(working_dir): 
        os.system('mkdir -p ' + working_dir)
    os.system('rm -rf ' + working_dir + '/*')

    if exp_config.delete_paxos_log:
        print 'Deleting paxos logs ..'
        os.system('rm -rf ' + exp_config.paxos_log_folder +'/*')
    else:
        print 'NOT deleting paxos logs ...'
    os.system('./kill_mongodb_local.sh;./run_mongodb_local.sh')

    # generate config file
    
    # generate workloads

    #os.system('sleep 100')
#os.system('pssh -h hosts.txt "killall -9 java"')
    f = open(config_file)
    first_lns = False
    for line in f:
        tokens = line.split()
        id = tokens[0]
        hostname = tokens[2]

        #if id == '0': 
        #    print 'MESSAGE: Not running node with ID = ' + id
        #    continue;
        if tokens[1] == 'yes':
            if is_failed_node(id):
                print 'NODE FAILED = ', id
                continue
            run_name_server(id, working_dir, config_file)
        else:
            if (first_lns == False):
                time.sleep(exp_config.ns_sleep) # sleep for 10 sec after starting name servers
                first_lns = True
            run_local_name_server(id, working_dir, config_file)
    # not doing restart stuff right now
    sys.exit(2)
    restart_period = 20
    number_restarts = 1
    restart_node = '2'
    for i in range(number_restarts):
        time.sleep(restart_period)
        print 'Restarting node ' + restart_node 
        run_name_server(restart_node, working_dir, config_file)
        print 'Done'
    
    
    #os.system('./kill_mongodb_local.sh')            

def run_name_server(id, working_dir, config_file):
    work_dir = os.path.join(working_dir, 'log_ns_' + id)
    os.system('cd ' + working_dir + '; rm -rf ' + work_dir )
    cmd = 'mkdir ' + work_dir +'; cd ' + work_dir + ';' + join(gnrs_dir, 'local/name-server.py') + ' --id ' + id + ' --jar ' + join(gnrs_dir, 'build/jars/GNS.jar') + ' --nsFile ' + config_file #+ ' > foo.out 2> foo.err < /dev/null'
    print (cmd)
    os.system(cmd)
    

def run_local_name_server(id, working_dir, config_file):
    work_dir = os.path.join(working_dir, 'log_lns_' + id)
    os.system('cd ' + working_dir + '; rm -rf ' + work_dir + '; mkdir ' + work_dir)
    cmd = 'cd ' + work_dir + ';' + join(gnrs_dir, 'local/local-name-server.py') \
        + ' --id ' + id \
        + ' --jar ' + join(gnrs_dir, 'build/jars/GNS.jar') \
        + ' --nsFile ' + config_file  # + ' > foo.out 2> foo.err < /dev/null
    
    lookup_trace = join(gnrs_dir, 'local/lookupLocal/' + id)
    if os.path.exists(lookup_trace):
        cmd += ' --lookupTrace ' + join(gnrs_dir, 'local/lookupLocal/' + id)

    update_trace = join(gnrs_dir, 'local/updateLocal/' + id)
    if os.path.exists(update_trace):
        cmd += ' --updateTrace ' + join(gnrs_dir, 'local/updateLocal/' + id) 


    print (cmd)
    os.system(cmd)


def is_failed_node(node_id):
    node_id = int(node_id)
    if exp_config.failed_nodes is None:
        return False
    for node_id1 in exp_config.failed_nodes:
        if node_id1 == node_id:
            return True
    return False

if __name__ == "__main__":
    main()
