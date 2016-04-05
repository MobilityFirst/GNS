#!/usr/bin/env python
import os
import sys
import time

import exp_config
from generate_multinode_config_file import generate_multinode_config_file
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
    if output_folder.endswith('/'):
        output_folder = output_folder[:-1]
    
    #run mongo db
    if exp_config.run_db:
        os.system('./kill_mongodb_pl.sh ' + exp_config.db_folder)
        os.system('./run_mongodb_pl.sh ' + exp_config.db_folder)    
        os.system('sleep ' + str(exp_config.mongo_sleep)) # ensures mongo is fully running 
    
    # copy config files to record experiment configuration
    os.system('mkdir -p ' + output_folder)
    
    #os.system(generate_config_script + ' ' +  str(exp_config.load))
    os.system('cp local-name-server.py name-server.py exp_config.py  ' + output_folder)
    
    #generate_ec2_config_file(exp_config.load)
    
    os.system('date')

    # ./cpPl.sh
    os.system('./cpPl.sh ' + exp_config.config_folder)
    
    # ./cpWorkload.sh
    #os.system('./cpWorkload.sh ' + lookupTrace + ' ' + updateTrace  + ' &')
    
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
    #downloadNameActives()
    
    

    if exp_config.download_jar:
        os.system('./getJarS3.sh')
    # ./rmLog.sh
    os.system('./killJava.sh')
    os.system('./rmLog.sh ' +  exp_config.paxos_log_folder + ' ' + exp_config.gns_output_logs)# +' 1>/dev/null 2>/dev/null')
    
    # start cpu use monitoring
    os.system('./run_mpstat.sh ' + exp_config.hosts_ns_file + ' ' + exp_config.remote_cpu_folder)

    # ./name-server.sh
    #os.system('./name-server.sh')
    run_all_ns(exp_config.gns_output_logs)
    
    try:
        #os.system('sleep ' + str(exp_config.ns_sleep))
        print 'ns sleep is ... ' + str(exp_config.ns_sleep) + 'sec ..'
        sleep_for_time(exp_config.ns_sleep)
    except:
        print 'NS sleep interrupted. Starting LNS ...'
        sys.exit(2)
    print 'Name servers running ...'
    
    # ./local-name-server.sh
    #os.system('./local-name-server.sh')
    run_all_lns(exp_config.gns_output_logs, exp_config.num_ns)
    
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
    
    print 'Ending script machine still running. Copy data yourself.'
    
    sys.exit(2)
    # ./endExperiment.sh output_folder
    print 'Ending experiment ..'
    os.system('./endExperiment.sh ' + output_folder + '  ' + exp_config.gns_output_logs)

    # stop cpu use monitoring and copy cpu use data
    os.system('./kill_mpstat.sh ' + exp_config.hosts_ns_file + ' ' + exp_config.remote_cpu_folder + ' ' + exp_config.local_cpu_folder)

    #os.system('./kill_mongodb_pl.sh')
    
    # ./parse_log.py output_folder
    os.system('logparse/parse_log.py ' + output_folder)
    os.system(log_parse_script + ' ' + output_folder  + ' ' + output_folder + '_stats local')
    
    time2 = time.time()
    
    diff = time2 - time1
    print 'TOTAL EXPERIMENT TIME:', int(diff/60), 'minutes'


def downloadNameActives():
    """ """
    nameActivesURL = exp_config.name_actives_url
    nameActivesRemote = exp_config.name_actives_remote
    
    nameActivesFile = nameActivesURL.split('/')[-1]
    output_folder = os.path.split(nameActivesRemote)[0]
    #nameActivesRemote = os.path.join(output_folder, nameActivesFile)
    
    os.system('./download_file.sh pl_ns ' + nameActivesURL + ' ' + nameActivesFile + ' ' + output_folder) 
    os.system('./cpNameActives_S3.sh pl_ns ' +  nameActivesRemote)

    os.system('./download_file.sh pl_lns ' + nameActivesURL + ' ' + nameActivesFile + ' ' + output_folder)
    os.system('./cpNameActives_S3.sh pl_lns ' +  nameActivesRemote)


def sleep_for_time(sleep_time):
    
    print 'sleep sleep ...'
    if sleep_time < 10:
        time.sleep(sleep_time)
        print 'here..'
        return
    count = int(sleep_time / 10)
    print 'sdf ...'
    for j in range(count):
        print 'jere2'
        time.sleep(10)
        print 'Sleep ', str(j * 10), ' sec / ' + str(sleep_time) + ' sec'


if __name__ == "__main__":
    #downloadNameActives()
    main()
