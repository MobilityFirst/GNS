#!/usr/bin/env python
import os,sys

def main_old():
    hosts_file = sys.argv[1]   ## hosts file name
    copy_folder = sys.argv[2]  ## copy to this folder
    os.system('mkdir -p ' + copy_folder)
    f = open(hosts_file)
    for line in f:
        host = line.strip()
        cmd = 'scp -r ' + host + ':/state/partition1/paxos_log/* ' + copy_folder
        print cmd
        os.system(cmd)


def main():
    hosts_file = sys.argv[1]   ## hosts file name
    copy_folder = sys.argv[2]  ## copy to this folder
    os.system('mkdir -p ' + copy_folder)
    nodes = 80
    os.system('mkdir -p ' + copy_folder)
    for node in range(nodes):
        node_local_folder = os.path.join(copy_folder, 'log_' + str(node) + '/NODE' + str(node))
        os.system('mkdir -p ' + node_local_folder)
        remote_folder = os.path.join('/state/partition1/paxos_log', 'log_' + str(node) + '/NODE' + str(node))
        #os.system('pscp -h ' + hosts_file + ' ' + remote_folder + ' ' + node_local_folder)
        os.system('nohup cat ' + hosts_file + ' | parallel -j+100 scp -q -oStrictHostKeyChecking=no -oConnectTimeout=10 {}:' + remote_folder + '/* ' + node_local_folder + ' > /dev/null 2> /dev/null&')
        print 'Copied node log ' + str(node)
        #os.system('ls ' + node_local_folder)
    from time import sleep
    sleep(30)  # copy all logs in 30 sec

    ## now run java to generate missing tuples
    
    os.system('java -jar ')


if __name__ == '__main__':
    main()
