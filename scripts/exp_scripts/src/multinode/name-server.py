#!/usr/bin/env python
                                             
import getopt
import os
import sys
import subprocess
import socket

import exp_config
from time import sleep

#Constants: Do not edit
LOCAL_EXP = '-local'
#PLANETLAB_EXP = '-planetlab'
ID = '-id'
IP = '-ip'
NAMESERVER_FILE = '-nsfile'
PRIMARY_NAMESERVERS = '-primary'
AGGREGATE_INTERVAL = '-aInterval'
REPLICATION_INTERVAL = '-rInterval'
NORMALIZING_CONSTANT = '-nconstant'
MOVING_AVG_WINDOW_SIZE = '-mavg'
TTL_CONSTANT = '-ttlconstant'
DEFAULT_TTL_REGULAR_NAME = '-rttl'
DEFAULT_TTL_MOBILE_NAME = '-mttl'
REGULAR_WORLOAD = '-rworkload'
MOBILE_WORLOAD = '-mworkload'
STATIC_REPLICATION = '-static'
OPTIMAL_REPLICATION = '-optimal'
RANDOM_REPLICATION = '-random'
LOCATION_REPLICATION = '-location'
NAMESERVER_SELECTION_VOTE_SIZE = '-nsVoteSize'
MIN_REPLICA = '-minReplica'
MAX_REPLICA = '-maxReplica'
BEEHIVE_REPLICATION = '-beehive'
C = '-C'
ALPHA = '-alpha'
BASE = '-base'
DEBUG_MODE = '-debugMode'
EXPERIMENT_MODE = '-experimentMode'
HELP = '-help'
TINY_UPDATE = '-tinyUpdate'
EMULATE_PING_LATENCIES = '-emulatePingLatencies'
VARIATION = '-variation'
EVENTUAL_CONSISTENCY='-eventualConsistency'
NO_LOAD_DB='-noLoadDB'
#PRIMARY_PAXOS = '-primaryPaxos'
PAXOS_DISK_BACKUP = '-paxosDiskBackup'
PAXOS_LOG_FOLDER = '-paxosLogFolder'
FAILURE_DETECTION_MSG_INTERVAL = '-failureDetectionMsgInterval'
FAILURE_DETECTION_TIMEOUT_INTERVAL = '-failureDetectionTimeoutInterval'
PAXOS_START_MIN_DELAY = '-paxosStartMinDelaySec'
PAXOS_START_MAX_DELAY = '-paxosStartMaxDelaySec'
QUIT_AFTER_TIME = '-quitAfterTime'

KMEDOIDS_REPLICATION = '-kmedoids'
NUM_LNS = '-numLNS'
LNSNSPING_FILE = '-lnsnsping'
NSNSPING_FILE = '-nsnsping'

FILE_LOGGING_LEVEL =  '-fileLoggingLevel'
CONSOLE_OUTPUT_LEVEL = '-consoleOutputLevel'
STAT_FILE_LOGGING_LEVEL = '-statFileLoggingLevel'
STAT_CONSOLE_OUTPUT_LEVEL = '-statConsoleOutputLevel'
#SIGNATURE_CHECK = '-signatureCheck'

#PERSISTENT_DATA_STORE = '-persistentDataStore'
#SIMPLE_DISK_STORE = '-simpleDiskStore'

GEN_WORKLOAD_SLEEP_TIME = '-syntheticWorkloadSleepTimeBetweenAddingNames'

NAME_ACTIVES = '-nameActives'

WORKER_THREAD_COUNT = '-workerThreadCount'

MAX_LOG_NAME = '-maxLogName'

#Parameters: Update as required
name_server_jar = exp_config.jar_file_remote
is_local = False                          #Run the name server instance on the local host.
node_id = -1
ip = None
#name_server_file = 'pl_ns_ping'
name_server_file = 'pl_config'
primary_name_server = exp_config.primary_name_server
aggregate_interval = exp_config.replication_interval      #In seconds
replication_interval = exp_config.replication_interval    #In seconds
normalizing_constant = exp_config.normalizing_constant # exp_config.normalizing_constant   #Used as the denominator for calculating number of replicas
                            #NumReplicas = lookupRate / (updateRate * normalizing_constant)
moving_avg_window_size = 20 #Used for calculating inter-arrival update time and ttl value

ttl_constant = 0.0            #Multiplied by inter-arrival update time to calculate ttl value of a name

default_ttl_regular_name = 0   # TTL = 0 means no TTL, TTL = -1 means infinite TTL, else, TTL = TTL value in sec
default_ttl_mobile_name = 0    # TTL = 0 means no TTL, TTL = -1 means infinite TTL, else, TTL = TTL value in sec

regular_workload = exp_config.regular_workload
mobile_workload = exp_config.mobile_workload
gen_workload_sleep_time = 0

is_optimal_replication = False              # Optimal

is_static_replication = exp_config.is_static_replication                #Static3

is_random_replication = exp_config.is_random_replication               #Uniform

is_location_replication = exp_config.is_location_replication             #Locality
name_server_selection_vote_size = exp_config.name_server_selection_vote_size         #top-k size.
min_replica = exp_config.min_replica
max_replica = exp_config.max_replica

is_beehive_replication = exp_config.is_beehive_replication
c_hop = exp_config.c_hop
base = 16
alpha = exp_config.alpha

#primary_paxos = False                         # Whether primaries use paxos between themselves
paxos_disk_backup = False                     # Whether paxos stores its state to disk
paxos_log_folder = exp_config.paxos_log_folder      # folder does paxos store its state in

failure_detection_msg_interval = exp_config.failure_detection_msg_interval           # Interval (in sec) between two failure detection messages sent to a node
failure_detection_timeout_interval = exp_config.failure_detection_timeout_interval   # Interval (in sec) after which a node is declared failed is no response is recvd for failure detection messages

paxos_start_min_delay_sec = 0
paxos_start_max_delay_sec = 0

quit_after_time = exp_config.quit_after_time
#exp_config.experiment_run_time + exp_config.ns_sleep + exp_config.extra_wait + 60 # if value >= 0, name server will quit after that time
quit_node_id = exp_config.quit_node_id     # which node will quit

is_debug_mode = exp_config.is_debug_mode
is_experiment_mode = exp_config.is_experiment_mode                # Always set to True to run experiments

tiny_update = False
emulate_ping_latencies = exp_config.emulate_ping_latencies
variation = exp_config.variation

eventual_consistency=exp_config.eventual_consistency
no_load_db=exp_config.no_load_db

is_kmedoids_replication = False
num_local_name_server = 0 # not used
lnsnsping_file = ''
nsnsping_file = ''

# logging related parameters:
## values: ALL, OFF, INFO, FINE, FINER, FINEST,.. see java documentation.
file_logging_level = exp_config.nslog
console_output_level = exp_config.nslog
stat_file_logging_level = exp_config.nslogstat
stat_console_output_level = exp_config.nslogstat

#signature_check = exp_config.signature_check
#persistent_data_store = exp_config.persistent_data_store
#simple_disk_store = exp_config.simple_disk_store

name_actives = exp_config.name_actives_remote

worker_thread_count = exp_config.worker_thread_count

max_log_name = exp_config.max_log_name

remote_java_bin = exp_config.java_bin

""" Prints usage message """
def usage():
    print 'USAGE: name-server.py [options...]' 
    print 'Runs a name server instance with the specified options'
    print '\nOptions:'
    print '--jar <*.jar>\t\tName server jar file including path'
    print '--local\t\t\tRun Name Server instance on localhost'
    #print '--planetlab\t\t\tRun Name Server instance on PlanetLab node'
    print '--id <#>\t\tUnique name server id'
    print '--nsFile <file>\t\tName server file'
    print '\t\t\tFormat per line (local):' 
    print '\t\t\tID HOST_NAME DNSPort ReplicationPort'
    print '\t\t\tUpdatePort StatsPort ActiveNSPort' 
    print '\t\t\tPING_LATENCY LATITUDE LONGITUDE'
    print '\t\t\tFormat per line (cluster):' 
    print '\t\t\tID HOST_NAME PING_LATENCY LATITUDE LONGITUDE'
    print '--primary <#>\t\tNumber of primary name servers per name'
    print '--aggregateInterval\tInterval (in seconds) between aggregating name statistic'
    print '--replicationInterval\tInterval (in seconds) between replication'
    print '--nConstant <#>\t\tConstant used to calculate number of name servers for a name'
    print '\t\t\t#Replica = Lookup rate / (Update Rate * Normalizing_Constant)'
    print '--mavgSize <#>\t\tWIndow size for calculating moving average for lookup and update'
    print '--ttlConstant <#>\tTTL Constant. TTL_Address = Update_Rate * TTL_Constant'
    print '--rTTL <#>\t\tDefault TTL for regular name'
    print '--mTTL <#>\t\tDefault TTL for mobile names'
    print '--rWorkload <size>\tRegular workload size'
    print '--mWorkload <size>\tMobile workload size'
    print '--static\t\tFixed number of active name server per name'
    print '--random\t\tRandomly select new active name servers'
    print '--location\t\tLocation Based selection of active name servers.' 
    print '--nsSelectionVoteSize <size> Size of name server select vote'
    print '--debugMode\t\tRun in debug mode. Generates log in file log_lns_id'
    #    print '--experimentMode\t\tRun in experiment mode with GNRS-Westy'
    print '--help\t\t\tPrints usage messages'
    print '--kmedoids\t\tKmedoids clustering for replication'
    print '--numLNS\t\t\tnum of local name servers'
    print '--lnsnsping\t\t\tping latency file for lns-ns'
    print '--nsnsping\t\t\tping latency file for ns-ns'

    
""" Executes an instance of Name Server with the give parameters """
def run_name_server():
    command = 'nohup ' + remote_java_bin +  '/java -Xmx4000m -cp ' + name_server_jar + ' ' + exp_config.ns_main
    if is_local:
        command += ' ' + LOCAL_EXP
    #else:
    #    command += ' ' + PLANETLAB_EXP
    command += ' ' + ID + ' ' + str(node_id)
    command += ' ' + NAMESERVER_FILE + ' ' + name_server_file
    command += ' ' + PRIMARY_NAMESERVERS + ' ' + str(primary_name_server)
    command += ' ' + AGGREGATE_INTERVAL + ' ' + str(aggregate_interval)
    command += ' ' + REPLICATION_INTERVAL + ' ' + str(replication_interval)
    command += ' ' + NORMALIZING_CONSTANT + ' ' + str(normalizing_constant)
    command += ' ' + MOVING_AVG_WINDOW_SIZE + ' ' + str(moving_avg_window_size)
    command += ' ' + TTL_CONSTANT + ' ' + str(ttl_constant)
    command += ' ' + DEFAULT_TTL_REGULAR_NAME + ' ' + str(default_ttl_regular_name)
    command += ' ' + DEFAULT_TTL_MOBILE_NAME + ' ' + str(default_ttl_mobile_name)
    command += ' ' + REGULAR_WORLOAD + ' ' + str(regular_workload)
    command += ' ' + MOBILE_WORLOAD + ' ' + str(mobile_workload)
    #command += ' ' + GEN_WORKLOAD_SLEEP_TIME + ' ' + str(gen_workload_sleep_time)
    
    if is_static_replication:
        command += ' ' + STATIC_REPLICATION
    elif is_random_replication:
        command += ' ' + RANDOM_REPLICATION  
    elif is_location_replication:
        command += ' ' + LOCATION_REPLICATION
        command += ' ' + NAMESERVER_SELECTION_VOTE_SIZE + ' ' + str(name_server_selection_vote_size)
    elif is_beehive_replication:
        command += ' ' + BEEHIVE_REPLICATION
        command += ' ' + C + ' ' + str(c_hop)
        command += ' ' + BASE + ' ' + str(base)
        command += ' ' + ALPHA + ' ' + str(alpha)
    elif is_optimal_replication:
        command += ' ' + OPTIMAL_REPLICATION
    elif is_kmedoids_replication:
    	command += ' ' + NUM_LNS + ' ' + str(num_local_name_server)
    	command += ' ' + LNSNSPING_FILE + lnsnsping_file 
    	command += ' ' + NSNSPING_FILE + nsnsping_file
    else:
        print 'Error: No replication model selected'
        sys.exit(2)
    # min and max number of replica
    if (min_replica != 3):
        command += ' ' + MIN_REPLICA + ' ' + str(min_replica)
    if (max_replica != 100):
        command += ' ' + MAX_REPLICA + ' ' + str(max_replica)
        
    command += ' ' + FILE_LOGGING_LEVEL + ' ' + file_logging_level
    command += ' ' + CONSOLE_OUTPUT_LEVEL + ' ' + console_output_level
    command += ' ' + STAT_FILE_LOGGING_LEVEL + ' ' + stat_file_logging_level
    command += ' ' + STAT_CONSOLE_OUTPUT_LEVEL + ' ' + stat_console_output_level


    #if signature_check:
    #    command += ' ' + SIGNATURE_CHECK
    
    #if persistent_data_store:
    #    command += ' ' + PERSISTENT_DATA_STORE
    #elif simple_disk_store:
    #    command += ' ' + SIMPLE_DISK_STORE
    if tiny_update:
        command += ' ' + TINY_UPDATE
    if emulate_ping_latencies:
        command += ' ' + EMULATE_PING_LATENCIES
        command += ' ' + VARIATION + ' ' + str(variation)
    
    #if primary_paxos:
    #    command += ' ' + PRIMARY_PAXOS
    #    if paxos_disk_backup:
    #        command += ' ' + PAXOS_DISK_BACKUP
    if paxos_log_folder != '':
        command += ' ' + PAXOS_LOG_FOLDER + ' ' + os.path.join(paxos_log_folder,'log_' + str(node_id))
    
    command += ' ' + FAILURE_DETECTION_MSG_INTERVAL + ' ' + str(failure_detection_msg_interval)
    command += ' ' + FAILURE_DETECTION_TIMEOUT_INTERVAL + ' ' + str(failure_detection_timeout_interval)
    
    if  quit_node_id == node_id and quit_after_time >= 0: 
        command += ' ' + QUIT_AFTER_TIME + ' ' + str(quit_after_time)
    
    if paxos_start_min_delay_sec > 0:
        command += ' ' + PAXOS_START_MIN_DELAY + ' ' + str(paxos_start_min_delay_sec)
        command += ' ' + PAXOS_START_MAX_DELAY + ' ' + str(paxos_start_max_delay_sec)
    
    if not name_actives ==  '':
        command += ' ' + NAME_ACTIVES + ' ' + name_actives

    command += ' ' + WORKER_THREAD_COUNT + ' ' + str(worker_thread_count)

    if is_experiment_mode:
        command += ' ' + EXPERIMENT_MODE
        if eventual_consistency:
            command += ' ' + EVENTUAL_CONSISTENCY
        if no_load_db:
            command += ' ' + NO_LOAD_DB
        #command += ' ' + MAX_LOG_NAME + ' ' + str(max_log_name)
    
    if is_debug_mode:
        command += ' ' + DEBUG_MODE
    command += ' > log_ns_' + str(node_id)
    command += ' 2> log_ns_' + str(node_id)
    command += ' &'
    print command
    os.system(command)
    sys.exit(2)
    while True:
        sleep(2)
        if os.path.exists('log_ns_' + str(node_id)):
            f = open('log_ns_' + str(node_id))
            line = f.readline()
            if line.startswith('java: error while loading shared libraries'):
                os.system('killall -9 java')
                print 'java error RERUNNING COMMAND ON PL .... ' + str(node_id)
                os.system(command)
            else:
                print 'java running ... '
                break
    


def get_alpha_based_on_load(load):
    capacity = 40000.0
    r1 = 648.2133333/2
    w1 = 330.9416667/2
    
    r = load*r1
    w = load*w1
    beta = 3
    alpha = (capacity - beta*w) / r - 1
    return alpha

def get_node_id():
    """Name server ID from pl_config file."""
    if ip is None:
        host_name = socket.gethostbyname(socket.getfqdn())
        host_name2 = socket.gethostbyname(socket.gethostname())
    else:
        host_name = host_name2 = ip
    f = open(name_server_file)
    for line in f:
        tokens = line.split()
        if (tokens[2] == host_name or tokens[2] == host_name2) and tokens[1] == 'yes':
            return int(tokens[0])
    print 'Host:' + host_name + ' Node Id: -1'
    sys.exit(2)

""" Look up name server id from file. """
def get_name_server_id():
    
    file = open(name_server_file, 'r')
    cmd = "uname -n"
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    process.wait()
    host_name = process.stdout.read().strip()
    
    for line in file.readlines():
        token = line.rstrip('\n').split()
        if host_name == token[1]:
            return int(token[0])
    
    print 'Host:' + host_name + ' Node Id: -1'
    sys.exit(2)


""" Prints options during debug mode """
def print_options():
    if is_debug_mode:    
        print "Jar: " + name_server_jar
        print "Local: " + str(is_local) 
        print "Id: " + str(node_id)
        print "NameServer File: " + name_server_file
        print "Primary NameServer: " + str(primary_name_server)
        print "Aggregate Interval: " + str(aggregate_interval) + "sec"
        print "Replication Interval: " + str(replication_interval) + "sec"
        print "Normalizing Constant: " + str(normalizing_constant)
        print "Moving Avg Window Size: " + str(moving_avg_window_size)
        print "TTL Constant: " + str(ttl_constant)
        print "Default TTL Regular Names: " + str(default_ttl_regular_name) + "sec"
        print "Default TTL Mobile Names: " + str(default_ttl_mobile_name) + "sec"
        print "Regular Workload: " + str(regular_workload)
        print "Mobile Workload: " + str(mobile_workload)
        print "Static Replication: " + str(is_static_replication)
        print "Optimal Replication: " + str(is_optimal_replication)
        print "Random Replication: " + str(is_random_replication)
        print "Location Replication: " + str(is_location_replication)
        print "Name Server Selection Vote Size: " + str(name_server_selection_vote_size)
        print "Beehive Replication: " + str(is_beehive_replication)
        print "C: " + str(c_hop)
        print "Base: " + str(base)
        print "Alpha: " + str(alpha)
        print "Debug Mode: " + str(is_debug_mode)
        print "Experiment Mode: " + str(is_experiment_mode) 

def main(argv):
    
    try:                                
        opts, args = getopt.getopt(argv, 'a:bc:d:e:f:g:h:i:j:k:l:m:n:o:pqrw:stuvx:y:z:', 
                                   ['jar=', 'local', 'id=', 'nsFile=',
                                    'primary=', 'aggregateInterval=',
                                    'replicationInterval=', 'nConstant=',
                                    'mavgSize=', 'ttlConstant=', 'rTTL=',
                                    'mTTL=', 'nTTL=', 'rWorkload=', 
                                    'mWorkload=', 'static', 'random',
                                    'location', 'nsSelectionVoteSize=', 'debugMode', 'help', 'planetlab',
                                    'beehive', 'hop=', 'base=', 'alpha=', 'ip='])
    except getopt.GetoptError:    
        print 'Incorrect option'      
        usage()                         
        sys.exit(2)

    global name_server_jar
    global is_local
    global node_id 
    global ip
    global name_server_file 
    global primary_name_server 
    global aggregate_interval 
    global replication_interval 
    global normalizing_constant 
    global moving_avg_window_size 
    global ttl_constant 
    global default_ttl_regular_name 
    global default_ttl_mobile_name 
    global regular_workload 
    global mobile_workload 
    global is_static_replication 
    global is_optimal_replication 
    global is_random_replication 
    global is_location_replication 
    global name_server_selection_vote_size
    global is_beehive_replication
    global c_hop
    global base
    global alpha
    global is_debug_mode
    global is_kmedoids_replication
    global num_local_name_server 
    global lnsnsping_file
    global nsnsping_file 
    for opt, arg in opts:
        if opt == '--jar':
            name_server_jar = arg
        elif opt == '--local':
            is_local = True
        elif opt == '--planetlab':
            is_local = False
        elif opt == '--id':
            node_id = int(arg)
        elif opt == '--ip':
            ip = arg
        elif opt == '--nsFile':
            name_server_file = arg
        elif opt == '--primary':
            primary_name_server = int(arg)
        elif opt == '--aggregateInterval':
            aggregate_interval = int(arg)
        elif opt == '--replicationInterval':
            replication_interval = int(arg) 
        elif opt == '--nConstant':
            normalizing_constant = float(arg)
        elif opt == '--mavgSize':
            moving_avg_window_size = int(arg)
        elif opt == '--ttlConstant':
            ttl_constant = float(arg)
        elif opt == '--rTTL':
            default_ttl_regular_name = int(arg)
        elif opt == '--mTTL':
            default_ttl_mobile_name = int(arg)
        elif opt == '--rWorkload':
            regular_workload = int(arg)
        elif opt == '--mWorkload':
            mobile_workload = int(arg)
        elif opt == '--static':
            is_static_replication = True
            is_optimal_replication = False
            is_random_replication = False
            is_location_replication = False
            is_beehive_replication = False
            is_kmedoids_replication = False
        elif opt == '--optimal':
            is_static_replication = False
            is_optimal_replication = True
            is_random_replication = False
            is_location_replication = False
            is_beehive_replication = False
            is_kmedoids_replication = False
        elif opt == '--location':
            is_location_replication = True
            is_optimal_replication = False
            is_random_replication = False
            is_static_replication = False
            is_beehive_replication = False
            is_kmedoids_replication = False
        elif opt == '--random':
            is_random_replication= True
            is_optimal_replication = False
            is_location_replication = False
            is_static_replication = False
            is_beehive_replication = False
            is_kmedoids_replication = False
        elif opt == '--beehive':
            is_beehive_replication = True
            is_optimal_replication = False
            is_location_replication = False
            is_random_replication = False
            is_static_replication = False
            is_kmedoids_replication = False
        elif opt == '--nsSelectionVoteSize':
            name_server_selection_vote_size = int(arg)
        elif opt == '--hop':
            c_hop = float(arg)
        elif opt == '--base':
            base = float(arg)
        elif opt == '--alpha':
            alpha = float(arg)
        elif opt == '--debugMode':
            is_debug_mode = True
        elif opt == '--kmedoids':
            is_kmedoids_replication = True
            is_static_replication = False
            is_random_replication = False
            is_location_replication = False
            is_beehive_replication = False
        elif opt == '--numLNS': 
            num_local_name_server = int(arg)
        elif opt == '-lnsnsping':
            lnsnsping_file = arg
        elif opt == '-nsnsping':
            nsnsping_file = arg
        elif opt == '--help':
            usage()
            sys.exit(1)
        else:
            print 'Incorrect Argument: wrong option ' + opt
            usage()                         
            sys.exit(2)
    
    #normalizing_constant = 1.0/get_alpha_based_on_load(exp_config.load)
    print 'Load = ', exp_config.load, '\tNormalizing constant:', normalizing_constant
    check_file(name_server_jar)
    check_file(name_server_file)
    
    if node_id is None or node_id == -1:
        node_id = get_node_id();
        
    #print_options()
    run_name_server()


def check_file(filename):
    if not os.path.exists(filename):
        print '\n\tEXCEPTION\nEXCEPTION\nEXCEPTION\nEXCEPTION\n'
        print '************************************************'
        print 'File does not exist:', filename
        print '************************************************'
        print '\n\tEXCEPTION\nEXCEPTION\nEXCEPTION\nEXCEPTION\n'
        sys.exit(1)
                
if __name__ == "__main__":
    main(sys.argv[1:])
