#!/usr/bin/python   
                                                            
import getopt
import os
import sys
import subprocess

#Constants: Do not edit
LOCAL_EXP = '-local'
PLANETLAB_EXP = '-planetlab'
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
RANDOM_REPLICATION = '-random'
LOCATION_REPLICATION = '-location'
NAMESERVER_SELECTION_VOTE_SIZE = '-nsVoteSize'
BEEHIVE_REPLICATION = '-beehive'
C = '-C'
ALPHA = '-alpha'
BASE = '-base'
DEBUG_MODE = '-debugMode'
PERSISTENT_DATA_STORE = '-persistentDataStore'
HELP = '-help'

#Parameters: Update as required
name_server_jar = '../../build/jars/gnrs-ns.jar'
is_local = False          #Flag for indicating the execution environment       
node_id = -1
name_server_file = 'name-server-info'
primary_name_server = 3
aggregate_interval = 180      #In seconds
replication_interval = 600   #In seconds
normalizing_constant = 1   #Used as the denominator for calculating number of replicas
                            #NumReplicas = lookupRate / (updateRate * normalizing_constant)
moving_avg_window_size = 20 #Used for calculating inter-arrival update time and ttl value

ttl_constant = 0.0000001            #Multiplied by inter-arrival update time to calculate ttl value of a name
default_ttl_regular_name = 0
default_ttl_mobile_name = 0
regular_workload = 0
mobile_workload = 0
is_static_replication = False               #Radom3
is_random_replication = True               #Uniform
is_location_replication = False             
name_server_selection_vote_size = 10        #top-k size
is_beehive_replication = False
c_hop = 0.5
base = 16
alpha = 0.91
is_debug_mode = True
persistent_data_store = False

""" Prints usage message """
def usage():
    print 'USAGE: name-server.py [options...]' 
    print 'Runs a name server instance with the specified options'
    print '\nOptions:'
    print '--jar <*.jar>\t\tName server jar file including path'
    print '--local\t\t\tRun Name Server instance on localhost'
    print '--planetlab\t\t\tRun Name Server instance on PlanetLab node'
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
    print '--persistentDataStore\t\tUse a persistent data store for name records'
    print '--help\t\t\tPrints usage messages'
    
""" Executes an instance of Name Server with the give parameters """
def run_name_server():
    command = 'java -jar ' + name_server_jar
    if is_local:
        command += ' ' + LOCAL_EXP
    else:
        command += ' ' + PLANETLAB_EXP
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
    else:
        print 'Error: No replication model selected'
        sys.exit(2)

    if persistent_data_store:
        command += ' ' + PERSISTENT_DATA_STORE
  
    if is_debug_mode:
        command += ' ' + DEBUG_MODE
 #       command += ' > log_ns_' + str(node_id)
        print command
 #   else:
 #       command += ' > log_ns'
    
    os.system(command)
    
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
        print "Random Replication: " + str(is_random_replication)
        print "Location Replication: " + str(is_location_replication)
        print "Name Server Selection Vote Size: " + str(name_server_selection_vote_size)
        print "Beehive Replication: " + str(is_beehive_replication)
        print "Persistent Data Store: " + str(persistent_data_store) 
        print "C: " + str(c_hop)
        print "Base: " + str(base)
        print "Alpha: " + str(alpha)
        print "Debug Mode: " + str(is_debug_mode) 

def main(argv):
    try:                                
        opts, args = getopt.getopt(argv, 'a:bc:d:e:f:g:h:i:j:k:l:m:n:o:pqrw:stuvx:y:z:A', 
                                   ['jar=', 'local', 'id=', 'nsFile=',
                                    'primary=', 'aggregateInterval=',
                                    'replicationInterval=', 'nConstant=',
                                    'mavgSize=', 'ttlConstant=', 'rTTL=',
                                    'mTTL=', 'nTTL=', 'rWorkload=', 
                                    'mWorkload=', 'static', 'random',
                                    'location', 'nsSelectionVoteSize=', 'debugMode', 'help', 'planetlab',
                                    'beehive', 'hop=', 'base=', 'alpha=', 'persistentDataStore'])
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
    global is_random_replication 
    global is_location_replication 
    global name_server_selection_vote_size
    global is_beehive_replication
    global c_hop
    global base
    global alpha
    global is_debug_mode
    global persistent_data_store
    
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
            is_random_replication = False
            is_location_replication = False
            is_beehive_replication = False
        elif opt == '--location':
            is_location_replication = True
            is_random_replication = False
            is_static_replication = False
            is_beehive_replication = False
        elif opt == '--random':
            is_random_replication= True
            is_location_replication = False
            is_static_replication = False
            is_beehive_replication = False
        elif opt == '--beehive':
            is_beehive_replication = True
            is_location_replication = False
            is_random_replication = False
            is_static_replication = False
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
        elif opt == '--persistentDataStore':
            persistent_data_store = True
        elif opt == '--help':
            usage()
            sys.exit(1)
        else:
            print 'Incorrect Argument: wrong option ' + opt
            usage()                         
            sys.exit(2)
    
    if node_id is None or node_id == -1:
        node_id = get_name_server_id();
        
    print_options()
    run_name_server()

if __name__ == "__main__":
    main(sys.argv[1:])
