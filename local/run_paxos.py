#!/usr/bin/env python
import os
import sys

#jar_file_path = '/Users/abhigyan/Documents/workspace/gnrs/out/production/gnrs/'
jar_file_path = '/Users/abhigyan/Documents/workspace/gnrs/dist/GNS.jar'

experiment_folder = '/Users/abhigyan/Documents/workspace/gnrs/local/'

node_config = '/Users/abhigyan/Documents/workspace/gnrs/local/node_config'

test_config = '/Users/abhigyan/Documents/workspace/gnrs/local/test_config'

paxos_log = '/Users/abhigyan/Documents/workspace/gnrs/local/paxos_log'

def main():
    
    replica_count = get_replica_count(test_config)
    # kill previous instances
    os.system(' cd ' + experiment_folder + ';./kill_local.py')
    
    # delete previous log
    os.system('rm -rf ' + experiment_folder + '/log/*; mkdir -p ' + experiment_folder + '/log')
    os.system('rm -rf ' + paxos_log + '/*')
    # run paxos replicas
    for i in range(replica_count):
        workdir = experiment_folder + '/log/log_' + str(i)
        
        os.system('mkdir -p ' + workdir)
        cmd = 'cd ' + workdir + ';nohup java -cp ' + jar_file_path + ' paxos.PaxosManager ' + node_config + ' ' + test_config + ' ' + paxos_log + ' ' + str(i) + ' > console_output &'
        print (cmd)
        os.system(cmd)
    import time
    time.sleep(5)
    # run a client
    workdir = experiment_folder + '/log/clientlog_' + str(replica_count)
    os.system('mkdir -p ' + workdir)
    cmd = 'cd ' + workdir +';java -cp ' + jar_file_path + ' paxos.NewClient ' + node_config + ' ' + test_config + ' ' + str(replica_count)
    print (cmd)
    os.system(cmd)
    os.system(' cd ' + experiment_folder + ';./kill_local.py')

def get_replica_count(test_config):
    f = open(test_config)
    for line in f:
        tokens = line.strip().split()
        if tokens[0] == 'NumberOfReplicas':
            return int(tokens[1])
    return 0
           

if __name__ == "__main__":
    main()
