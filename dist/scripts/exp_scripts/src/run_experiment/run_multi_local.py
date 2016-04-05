#!/usr/bin/env python
import os
import sys
from run_single_local import run_name_server, run_local_name_server
from os.path import join
import exp_config

mongo_sleep = exp_config.mongo_sleep # wait time between starting mongo and starting NS
ns_sleep = exp_config.ns_sleep # wait time between starting starting NS and starting LNS


def main():
    config_dir = '/home/abhigyan/gnrs/configLocal/'
    #working_dir = '/home/abhigyan/gnrs/log_local/'
    working_dir = sys.argv[1]
    #if len(sys.argv) >= 2:
    #    working_dir = sys.argv[1]
    if not working_dir.startswith('/home/abhigyan'):
        working_dir = os.path.join('/home/abhigyan/gnrs', working_dir)

    run_all_experiments(working_dir, config_dir)
    os.system('date')


def run_all_experiments(working_dir, normalizing_constant, config_dir='/home/abhigyan/gnrs/configLocal/'):
    if not os.path.exists(config_dir):
        print 'Config dir does not exist:', config_dir
        return

    if not os.path.exists(working_dir):
        os.system('mkdir -p ' + working_dir)
    os.system('rm -rf ' + working_dir + '/*')
    print 'Working dir is:', working_dir

    # check if number of config files = numer of lines in each file
    config_files = get_config_files(config_dir)

    #print 'Config files are:', config_files
    if len(config_files) == 0:
        print 'ERROR: No config files in config_dir:', config_dir
        sys.exit(1)

    if not file_len(config_files[0]) == len(config_files):
        print 'ERROR: Number of config files:', len(config_files), 'Number of lines in each file:', \
            file_len(config_files[0])
        sys.exit(1)
    else:
        print 'Number of config files matches the number of lines:', len(config_files)

    # kill experiments running on same hosts.
    os.system('pssh -P -t 30 -h hosts_ns.txt  "killall -9 java"')
    os.system('pssh -P -t 30 -h hosts_lns.txt  "killall -9 java"')

    #os.system('./kill_mongodb.sh')
    #os.system('./run_mongodb.sh')
    #print 'Sleeping ...'
    #from time import sleep
    #sleep(mongo_sleep)

    #kill_experiments_at_given_hosts(get_hostnames(config_files[0]))
    #os.sytem('pssh -h hosts.txt "nohup ./mongod --dbpath /home/abhigyan/gnrs-db-mongodb/ & 1>/dev/null 2 > /dev/null
    # < /dev/null"')
    f = open(config_files[0])
    first_lns = True

    for i, line in enumerate(f):
        tokens = line.split()
        id = tokens[0]
        hostname = tokens[2]

        if tokens[1] == 'yes':
            if is_failed_node(id):
                print 'MESSAGE: Not running FAILED node: ', id
                continue
            run_name_server(id, hostname, working_dir, get_config_file_name(config_dir, id), normalizing_constant)
        else:
            if first_lns:
                first_lns = False
                from time import sleep

                print 'sleep sleep ...'
                if ns_sleep < 10:
                    sleep(ns_sleep)
                    continue
                count = int(ns_sleep / 10)
                for j in range(count):
                    sleep(10)
                    print 'Sleep ', str(j * 10), 'sec'
            run_local_name_server(id, hostname, working_dir, get_config_file_name(config_dir, id))

    print 'LOCAL NAME SERVERS running ....'


def restart_name_server(node_id, working_dir, normalizing_constant, config_dir='/home/abhigyan/gnrs/configLocal/'):
    config_files = get_config_files(config_dir)
    f = open(config_files[0])
    for i, line in enumerate(f):
        tokens = line.split()
        id = tokens[0]
        hostname = tokens[2]
        if str(id) == node_id:
            assert tokens[1] == 'yes'
            run_name_server(node_id, hostname, working_dir, get_config_file_name(config_dir, id), normalizing_constant)
            print "Restarted name server ", node_id


def pl_config_file_exists(id):
    return os.path.exists('configLocalPL/' + id)


def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def get_config_file_name(config_dir, node_id):
    return os.path.join(config_dir, 'config_' + str(node_id))


def get_config_files(config_dir):
    files = os.listdir(config_dir)
    f1 = []
    for f in files:
        f1.append(join(config_dir, f))
    return f1


def get_hostnames(config_file):
    from select_columns import extract_column_from_file

    host_column = extract_column_from_file(config_file, 2, n=False)

    hosts = {}
    for h in host_column:
        hosts[h] = 1
    return list(hosts.keys())


def kill_experiments_at_given_hosts(hostnames):
    for hostname in hostnames:
        print 'Killing java in ', hostname
        os.system('ssh ' + hostname + ' "killall -9 java 2> /dev/null > /dev/null"')


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
