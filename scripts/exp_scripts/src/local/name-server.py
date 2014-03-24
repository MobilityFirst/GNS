#!/usr/bin/env python2.7
                                             
import getopt
import os
import sys
import subprocess
import socket

import exp_config

#Constants: Do not edit
LOCAL_EXP = '-local'
#PLANETLAB_EXP = '-planetlab'
ID = '-id'
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
BEEHIVE_REPLICATION = '-beehive'
C = '-C'
ALPHA = '-alpha'
BASE = '-base'
DEBUG_MODE = '-debugMode'
EXPERIMENT_MODE = '-experimentMode'
HELP = '-help'
TINY_UPDATE = '-tinyUpdate'

KMEDOIDS_REPLICATION = '-kmedoids'
NUM_LNS = '-numLNS' 
LNSNSPING_FILE = '-lnsnsping'
NSNSPING_FILE = '-nsnsping'

FILE_LOGGING_LEVEL =  '-fileLoggingLevel'
CONSOLE_OUTPUT_LEVEL = '-consoleOutputLevel'
STAT_FILE_LOGGING_LEVEL = '-statFileLoggingLevel'
STAT_CONSOLE_OUTPUT_LEVEL = '-statConsoleOutputLevel'

#PERSISTENT_DATA_STORE = '-persistentDataStore'
MONGO_PORT = '-mongoPort'
PAXOS_LOG_FOLDER = '-paxosLogFolder'
QUIT_AFTER_TIME = '-quitAfterTime'
NAME_ACTIVES = '-nameActives'
FAILURE_DETECTION_MSG_INTERVAL = '-failureDetectionMsgInterval'
FAILURE_DETECTION_TIMEOUT_INTERVAL = '-failureDetectionTimeoutInterval'

#Parameters: Update as required
name_server_jar = 'GNS.jar'
is_local = False                          #Run the name server instance on the local host.
node_id = -1
#name_server_file = 'pl_ns_ping'
name_server_file = 'pl_config'
primary_name_server = exp_config.primary_name_server
aggregate_interval = exp_config.replication_interval      #In seconds
replication_interval = exp_config.replication_interval      #In seconds
#exp_config.replication_interval    #In seconds
normalizing_constant = 0.1   #Used as the denominator for calculating number of replicas
                            #NumReplicas = lookupRate / (updateRate * normalizing_constant)
moving_avg_window_size = 20 #Used for calculating inter-arrival update time and ttl value

ttl_constant = 0.0            #Multiplied by inter-arrival update time to calculate ttl value of a name

default_ttl_regular_name = 0   # TTL = 0 means no TTL, TTL = -1 means infinite TTL, else, TTL = TTL value in sec
default_ttl_mobile_name = 0    # TTL = 0 means no TTL, TTL = -1 means infinite TTL, else, TTL = TTL value in sec

regular_workload = exp_config.regular_workload
mobile_workload = exp_config.mobile_workload

is_optimal_replication = False              # Optimal

is_static_replication = exp_config.is_static_replication                #Static3

is_random_replication = exp_config.is_random_replication               #Uniform

is_location_replication = exp_config.is_location_replication             #Locality
name_server_selection_vote_size = 5         #top-k size. this parameter is not used anymore.

is_beehive_replication = exp_config.is_beehive_replication
c_hop = 0.3
base = 16
alpha = 0.91
is_debug_mode = True
is_experiment_mode = exp_config.is_experiment_mode                # Always set to True to run experiments

tiny_update = False

is_kmedoids_replication = False
num_local_name_server = exp_config.num_lns
lnsnsping_file = ''
nsnsping_file = ''

#persistent_data_store = False
mongo_port = 27017
paxos_log_folder = exp_config.paxos_log_folder
failure_detection_msg_interval = 3           # Interval (in sec) between two failure detection messages sent to a node
failure_detection_timeout_interval = 10       # Interval (in sec) after which a node is declared as failed if it does not responsd to failure messages

# logging related parameters:
# values: ALL, OFF, INFO, FINE, FINER, FINEST,.. see java documentation.
file_logging_level = exp_config.nslog
console_output_level = exp_config.nslog
stat_file_logging_level = exp_config.nslogstat
stat_console_output_level = exp_config.nslogstat

quit_after_time = -1 # if value >= 0, name server will quit after that time
quit_node_id = 0     # nodeID of the name server that will quit

name_actives = exp_config.name_actives

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
    #Xiaozheng
    print '--kmedoids\t\tKmedoids clustering for replication'
    print '--numLNS\t\t\tnum of local name servers'
    print '--lnsnsping\t\t\tping latency file for lns-ns'
    print '--nsnsping\t\t\tping latency file for ns-ns'
    
""" Executes an instance of Name Server with the give parameters """
def run_name_server():
    command = 'nohup java -cp ' + name_server_jar + ' edu.umass.cs.gns.main.StartNameServer'
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
    #Xiaozheng
    elif is_kmedoids_replication:   
    	command += ' ' + NUM_LNS + ' ' + str(num_local_name_server)
    	command += ' ' + LNSNSPING_FILE + lnsnsping_file 
    	command += ' ' + NSNSPING_FILE + nsnsping_file
    else:
        print 'Error: No replication model selected'
        sys.exit(2)

    command += ' ' + FILE_LOGGING_LEVEL + ' ' + file_logging_level
    command += ' ' + CONSOLE_OUTPUT_LEVEL + ' ' + console_output_level
    command += ' ' + STAT_FILE_LOGGING_LEVEL + ' ' + stat_file_logging_level
    command += ' ' + STAT_CONSOLE_OUTPUT_LEVEL + ' ' + stat_console_output_level
    
    command += ' ' + PAXOS_LOG_FOLDER + ' ' + paxos_log_folder + '/log_'  + str(node_id)
    command += ' ' + FAILURE_DETECTION_MSG_INTERVAL + ' ' + str(failure_detection_msg_interval)
    command += ' ' + FAILURE_DETECTION_TIMEOUT_INTERVAL + ' ' + str(failure_detection_timeout_interval)
    
    if not name_actives ==  '':
        command += ' ' + NAME_ACTIVES + ' ' + name_actives
        
    #if persistent_data_store:
    #    command += ' ' + PERSISTENT_DATA_STORE
    if mongo_port > 0:
        command += ' ' + MONGO_PORT + ' ' + str(mongo_port)
    if tiny_update:
        command += ' ' + TINY_UPDATE
    # only node ID 0 is quitting
    if node_id == quit_node_id and quit_after_time >= 0:
        command += ' ' + QUIT_AFTER_TIME + ' ' + str(quit_after_time)
        
    #if primary_paxos:
    #    command += ' ' + PRIMARY_PAXOS
    if is_experiment_mode:
        command += ' ' + EXPERIMENT_MODE

    if is_debug_mode:
        command += ' ' + DEBUG_MODE
        command += ' > log_ns_' + str(node_id)
        print command
    else:
        command += ' > log_ns'
    command += ' &'
    os.system(command)


def get_node_id():
    """Name server ID from pl_config file."""
    host_name = socket.getfqdn()
    host_name2 = socket.gethostname()
    
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
                                    'beehive', 'hop=', 'base=', 'alpha=', 'optimal'])
    except getopt.GetoptError:    
        print 'Incorrect option'      
        usage()                         
        sys.exit(2)

    global name_server_jar
    global is_local
    global node_id 
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
    #Xiaozheng
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
        elif opt == '--nsFile':
            name_server_file = arg
        elif opt == '--primary':
            primary_name_server = int(arg)
        elif opt == '--aggregateInterval':
            aggregate_interval = int(arg)
        elif opt == '--replicationInterval':
            replication_interval = int(arg) 
        elif opt == '--nConstant':
            normalizing_constant = int(arg)
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
        #Xiaozheng
        elif opt == '--kmedoids':
            is_kmedoids_replication = True
            is_static_replication = False
            is_random_replication = False
            is_location_replicaiotn = False
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
