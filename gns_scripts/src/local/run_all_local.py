#!/usr/bin/env python2.7
import os
import sys
import inspect
from os.path import join
import time
import exp_config
from generate_config_file import write_local_config_file

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory

parent_folder = os.path.split(script_folder)[0]

sys.path.append(parent_folder)

from logparse.parse_log import parse_log  # added parent_folder to path to import parse_log module here


def main():
    from kill_local import kill_local_gnrs
    kill_local_gnrs()
    config_file = exp_config.config_file
    output_folder = exp_config.output_folder

    print 'Clearing old GNS logs ..'
    if not os.path.exists(output_folder): 
        os.system('mkdir -p ' + output_folder)
    os.system('rm -rf ' + output_folder + '/*')

    if exp_config.delete_paxos_log:
        print 'Deleting paxos logs ..'
        os.system('rm -rf ' + exp_config.paxos_log_folder +'/*')
    else:
        print 'NOT deleting paxos logs ...'

    # start DB
    if exp_config.start_db:
        os.system(script_folder + '/kill_mongodb_local.sh')
        os.system(script_folder + '/run_mongodb_local.sh')

    # generate config file
    write_local_config_file(exp_config.config_file, exp_config.num_ns, exp_config.num_lns)

    # generate workloads
    generate_all_traces()

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
            run_name_server(id, output_folder, config_file)
        else:
            if first_lns is False:
                time.sleep(exp_config.ns_sleep) # sleep for 10 sec after starting name servers
                first_lns = True
            run_local_name_server(id, output_folder, config_file)

    # kill local

    print 'Sleeping until experiment finishes ...'
    time.sleep(exp_config.experiment_run_time + exp_config.extra_wait)

    kill_local_gnrs()

    stats_folder = exp_config.output_folder + '_stats'
    if exp_config.output_folder.endswith('/'):
        stats_folder = exp_config.output_folder[:-1] + '_stats'
    parse_log(exp_config.output_folder, stats_folder, True)
    # parse logs and generate output

    # not doing restart stuff right now
    sys.exit(2)
    restart_period = 20
    number_restarts = 1
    restart_node = '2'
    for i in range(number_restarts):
        time.sleep(restart_period)
        print 'Restarting node ' + restart_node 
        run_name_server(restart_node, output_folder, config_file)
        print 'Done'

    #os.system('./kill_mongodb_local.sh')            


def run_name_server(id, output_folder, config_file):
    work_dir = os.path.join(output_folder, 'log_ns_' + id)
    os.system('cd ' + output_folder + '; rm -rf ' + work_dir )
    cmd = 'mkdir ' + work_dir + '; cd ' + work_dir + ';' + script_folder + '/name-server.py' + ' --id ' + id\
          + ' --jar ' + exp_config.gnrs_jar + ' --nsFile ' + config_file
           #+ ' > foo.out 2> foo.err < /dev/null'
    print (cmd)
    os.system(cmd)
    

def run_local_name_server(id, output_folder, config_file):
    work_dir = os.path.join(output_folder, 'log_lns_' + id)
    os.system('cd ' + output_folder + '; rm -rf ' + work_dir + '; mkdir ' + work_dir)
    cmd = 'cd ' + work_dir + ';' + script_folder + '/local-name-server.py' \
        + ' --id ' + id \
        + ' --jar ' + exp_config.gnrs_jar \
        + ' --nsFile ' + config_file  # + ' > foo.out 2> foo.err < /dev/null
    
    lookup_trace = join(exp_config.trace_folder, 'lookupLocal/' + id)
    if os.path.exists(lookup_trace):
        cmd += ' --lookupTrace ' + lookup_trace

    update_trace = join(exp_config.trace_folder, 'updateLocal/' + id)
    if os.path.exists(update_trace):
        cmd += ' --updateTrace ' + update_trace

    print (cmd)
    os.system(cmd)


def generate_all_traces():
    if exp_config.gen_workload:
        for i in range(exp_config.num_lns):
            lns_id = i + exp_config.num_ns
            generate_trace(lns_id)


def generate_trace(lns_id):

    from generate_sequence import write_single_name_trace,write_random_name_trace
    name = '0'
    number = exp_config.lookup_count
    filename = os.path.join(exp_config.trace_folder,'lookupLocal/' + str(lns_id))
    if exp_config.regular_workload + exp_config.mobile_workload == 1:
        write_single_name_trace(filename, number, name)
    else:
        write_random_name_trace(filename,exp_config.regular_workload + exp_config.mobile_workload, number)

    number = exp_config.update_count
    filename = os.path.join(exp_config.trace_folder,'updateLocal/' + str(lns_id))
    if exp_config.regular_workload + exp_config.mobile_workload== 1:
        write_single_name_trace(filename, number, name)
    else:
        write_random_name_trace(filename,exp_config.regular_workload + exp_config.mobile_workload, number)


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
