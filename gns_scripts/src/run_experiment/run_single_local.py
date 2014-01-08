#!/usr/bin/env python
import os
import sys

def main():
    
    config_file = '/home/abhigyan/gnrs/local_config'
    working_dir = '/home/abhigyan/gnrs/log_local/'
    hostname = 'c0-13'
    if len(sys.argv) >= 2:
        #config_file = sys.argv[1]
        #if not config_file.startswith('/home/abhigyan'):
        #    config_file = os.path.join('/home/abhigyan/gnrs', config_file)
        working_dir = sys.argv[1]
        if not working_dir.startswith('/home/abhigyan'):
            working_dir = os.path.join('/home/abhigyan/gnrs', working_dir)
    if len(sys.argv) >= 3:
        hostname = sys.argv[2]
    if not os.path.exists(config_file):
        print 'Config file does not exist:', config_file
        return
    if not os.path.exists(working_dir):
        os.system('mkdir -p ' + working_dir)
        
    print 'Working dir is:',working_dir
    os.system('rm -rf ' + working_dir + '/*')
    #os.system('pssh -h hosts.txt "killall -9 java"')
    os.system('ssh ' + hostname + ' "killall -9 java"')
    f = open(config_file)

    for line in f:
        tokens = line.split()
        id = tokens[0]
        #hostname = tokens[2]
        
        if tokens[1] == 'yes':
            run_name_server(id, hostname, working_dir, config_file)
        else:
            run_local_name_server(id, hostname, working_dir, config_file)
        from time import sleep
        sleep(30)


def get_hostnames(config_file):
    from select_columns import extract_column_from_file
    host_column = extract_column_from_file(config_file, 3)
    hosts = {}
    for h in host_column:
        hosts[h] = 1
    return list(hosts.keys())

def kill_experiments_at_given_hosts(hostnames):
    for hostname in hostnames:
        print 'Killing java in ', hostname
        os.system('ssh ' + hostname + ' "killall -9 java"')


def run_name_server(id, hostname, working_dir, config_file, beta_inverse=None):
    work_dir = os.path.join(working_dir, 'log_ns_' + id)
    os.system('cd ' + working_dir + '; rm -rf ' + work_dir + '; mkdir ' + work_dir)
    if beta_inverse is None:
        cmd = 'ssh ' + hostname + ' "cd ' + work_dir + ';nohup /home/abhigyan/gnrs/name-server.py --id ' + id + \
              ' --jar /home/abhigyan/gnrs/GNS.jar --nsFile ' + config_file + ' > foo.out 2> foo.err < /dev/null "'
    else:
        cmd = 'ssh ' + hostname + ' "cd ' + work_dir + ';nohup /home/abhigyan/gnrs/name-server.py --id ' + id + \
              ' --jar /home/abhigyan/gnrs/GNS.jar --nsFile ' + config_file + ' --nConstant ' + str(beta_inverse) + \
              ' > foo.out 2> foo.err < /dev/null "'
    print cmd
    os.system(cmd)


def run_local_name_server(id, hostname, working_dir, config_file):
    work_dir = os.path.join(working_dir, 'log_lns_' + id)
    os.system('cd ' + working_dir + '; rm -rf ' + work_dir + '; mkdir ' + work_dir)
    host_int = int(hostname[len('compute-0-'):])
    if host_int >=13:
        cmd = 'ssh ' + hostname + ' "cd ' + work_dir + ';nohup /home/abhigyan/gnrs/local-name-server.py --id ' + id + '  --jar /home/abhigyan/gnrs/GNS.jar --nsFile ' + config_file 
        if os.path.exists('/home/abhigyan/gnrs/lookupLocal/' + id):
            cmd += ' --lookupTrace /home/abhigyan/gnrs/lookupLocal/' + id
        if os.path.exists('/home/abhigyan/gnrs/updateLocal/' + id):
            cmd += ' --updateTrace /home/abhigyan/gnrs/updateLocal/' + id 
        cmd +=  ' > foo.out 2> foo.err < /dev/null "'
# ' --lookupTrace /home/abhigyan/gnrs/lookupLocal/' + id + ' --updateTrace /home/abhigyan/gnrs/updateLocal/' + id + '  --jar /home/abhigyan/gnrs/GNS.jar --nsFile ' + config_file  + ' > foo.out 2> foo.err < /dev/null "'
    #cmd = 'ssh ' + hostname + ' "cd ' + work_dir + ';nohup /home/abhigyan/gnrs/local-name-server.py --id ' + id + ' --lookupTrace /home/abhigyan/gnrs/lookupLocal/' + id + ' --updateTrace /home/abhigyan/gnrs/updateLocal/' + id +  '  --workloadFile /home/abhigyan/gnrs/workloadLocal/' + id + '  --jar /home/abhigyan/gnrs/gnrs-lns.jar --nsFile ' + config_file  + ' > foo.out 2> foo.err < /dev/null "'
    else:
        cmd = 'ssh ' + hostname + ' "cd ' + work_dir + ';nohup /home/abhigyan/Python-2.7.3/bin/python /home/abhigyan/gnrs/local-name-server.py --id ' + id + ' --lookupTrace /home/abhigyan/gnrs/lookupLocal/' + id + ' --updateTrace /home/abhigyan/gnrs/updateLocal/' + id + '  --jar /home/abhigyan/gnrs/GNS.jar --nsFile ' + config_file  + ' > foo.out 2> foo.err < /dev/null "'
    print cmd
    os.system(cmd)


if __name__ == "__main__":
    main()
