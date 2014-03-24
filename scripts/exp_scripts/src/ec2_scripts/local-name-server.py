#!/usr/bin/env python2.7

import getopt
import os
import sys
import subprocess
import socket

import exp_config

#Constants: Do not edit
ID = '-id'
NAMESERVER_FILE = '-nsfile'
LOCAL_EXP = '-local'
#PLANETLAB_EXP = '-planetlab'
REGULAR_WORLOAD = '-rworkload'
MOBILE_WORLOAD = '-mworkload'
WORKLOAD_FILE = '-wfile'
LOOKUP_TRACE_FILE = '-lookupTrace'
UPDATE_TRACE_FILE = '-updateTrace'
PRIMARY_NAMESERVERS = '-primary'
ALPHA = '-alpha'
CACHE_SIZE = '-cacheSize'
ZIPF_WORKLOAD = '-zipf'
LOCATION_REPLICATION = '-location'
OPTIMAL_REPLICATION = '-optimal'
REPLICATION_INTERVAL = '-rInterval'
OPTIMAL_TRACE = '-optimalTrace'
NAME = '-name'
NUM_LOOKUP = '-nlookup'
NUM_Update = '-nUpdate'
VOTE_INTERVAL = '-vInterval'
CHOOSE_FROM_CLOSEST_K = '-chooseFromClosestK'
LOOKUP_RATE = '-lookupRate'
UPDATE_RATE_MOBILE = '-updateRateMobile'
UPDATE_RATE_REGULAR = '-updateRateRegular'
DEBUG_MODE = '-debugMode'
EXPERIMENT_MODE = '-experimentMode'
HELP = '-help'
DELAY_SCHEDULING = '-delayScheduling'

BEEHIVE_REPLICATION = '-beehive'
BEEHIVEDHTBASE = '-beehiveBase'
BEEHIVELEAFSET = '-leafSet'

LOAD_BALANCING = '-loadDependentRedirection'
LOAD_MONITOR_INTERVAL = '-nsLoadMonitorIntervalSeconds'

NUMER_OF_TRANSMISSIONS = '-numberOfTransmissions'
MAX_QUERY_WAIT_TIME = '-maxQueryWaitTime'
QUERY_TIMEOUT = '-queryTimeout'
ADAPTIVE_TIMEOUT = '-adaptiveTimeout'
DELTA = '-delta'
MU = '-mu'
PHI = '-phi'

FILE_LOGGING_LEVEL =  '-fileLoggingLevel'
CONSOLE_OUTPUT_LEVEL = '-consoleOutputLevel'
STAT_FILE_LOGGING_LEVEL = '-statFileLoggingLevel'
STAT_CONSOLE_OUTPUT_LEVEL = '-statConsoleOutputLevel'



#Parameter: Update as required

local_name_server_jar = 'GNS.jar'              #Local name server jar
node_id = -1                                        #Node Id. Selected from name_server_file when running on planetlab
name_server_file = 'pl_config'                      #Name server information: ID Node_Name Ping_Latency Latitude Longitude
local_name_server_file = 'pl_config'                #List of local name server. Once planetlab node per line
is_local = False                                    #Flag for indicating the execution environment
                                                    #Setting this False implies PlanetLab as the execution environment
primary_name_server = exp_config.primary_name_server                             #Number of primary name servers
cache_size = 10000                                    #Cache Size
name = ''


lookup_trace_file = ''           ## lookups trace
update_trace_file = ''           ## updates trace

# calculated automatically
lookup_rate = 1000           #in ms                 #Inter-Arrival Time (in ms) between lookup queries, automatically determined by --expRunTime 
update_rate_regular = 10000    #in ms               #Inter-Arrival Time (in ms) between update request for regular names
update_rate_mobile = 0      #in ms (NOT USED)       #Inter-Arrival Time (in ms) between update request for mobile names   


## NOT USED. was used for zipf workload
is_zipf_workload = False                             #Use zipf distribution for generating lookup request
workload_file = ''                         #List of names queried at this local name server
regular_workload = 0                              #Size of regular workload, seems not used for local name server if update is not sent
mobile_workload = 0                                 #Size of mobile workload
alpha = 0.91                                        #Alpha for Zipf distribution
num_lookups = 0                                     #Number of lookup queries generated at the local name server, not used when lookuptrace is used
num_updates = 0                                     #Number of update queries generated at the local name server, not used when updatetrace is used


# Location Replication / Random Replication
is_location_replication = exp_config.is_location_replication    #Select location aware replication. If True, the local name server periodically
                                                    #(once every vote_interval) votes for its closest name server
                                                    #Set it to False for random replication                                                    
vote_interval = exp_config.replication_interval                                  #Time between votes (in seconds)
choose_from_closest_k = 1                           #Choose from K-closest to vote for a name

# Load balance
load_balancing = exp_config.load_balancing                 # local name servers start load balancing among name servers
load_monitor_interval = exp_config.replication_interval    # interval of monitoring load at every nameserver (seconds)

# Optimal Replication
is_optimal_replication = False
optimal_trace_file = ''
replication_interval = 300                          #In seconds. Should be the same as replication interval in name-server.py

# Beehive replication
is_beehive_replication = exp_config.is_beehive_replication                       # Beehive replication
beehive_base = 16                                   # Beehive DHT base, default 16
beehive_leaf_set = 4                                # Beehive Leaf set size
                                                    # must be less thant number of name servers, default 24
regular_workload = 1                                # Size of regular workload, seems not used for local name server if update is not sent

# Experiment duration
is_experiment_mode = exp_config.is_experiment_mode        # Always set to True to run experiments.
is_debug_mode = True                                #Prints logs if True. Used for testing.
experiment_run_time  = exp_config.experiment_run_time    # in seconds
delay_scheduling = False # True

# retransmission parameters
numberOfTransmissions = 3                         # maximum number of times a query is transmitted
maxQueryWaitTime = exp_config.maxQueryWaitTime    # maximum  Wait Time before query is  declared failed (milli-seconds)
queryTimeout = exp_config.queryTimeout            # timeout interval
adaptiveTimeout = False
delta = 0.05;                                   # Weight assigned to latest sample in calculating moving average.
mu = 1.0;                                       # Co-efficient of estimated RTT in calculating timeout.
phi = 6.0;                                      # Co-efficient of deviation in calculating timeout.


run_http_server = True

# logging related parameters:
## values: ALL, OFF, INFO, FINE, FINER, FINEST,.. see java documentation.
# logging related parameters:
file_logging_level = exp_config.lnslog
console_output_level = exp_config.lnslog
stat_file_logging_level = exp_config.lnslogstat
stat_console_output_level = exp_config.lnslogstat


""" Prints Usage Message """
def usage():
    print 'USAGE: local-name-server.py [options...]' 
    print 'Runs a local name server instance with the specified options'
    print '\nOptions:'
    print '--jar <*.jar>\t\tLocal name server jar file including path'
    print '--local\t\t\tRun Local Name Server instance on localhost'
    #print '--planetlab\t\t\tRun Local Name Server instance on PlanetLab node'
    print '--id <#>\t\tUnique local name server id'
    print '--nsFile <file>\t\tName server file'
    print '\t\t\tFormat per line (local):' 
    print '\t\t\tID HOST_NAME DNSPort ReplicationPort'
    print '\t\t\tUpdatePort StatsPort ActiveNSPort' 
    print '\t\t\tPING_LATENCY LATITUDE LONGITUDE'
    print '\t\t\tFormat per line (cluster):' 
    print '\t\t\tID HOST_NAME PING_LATENCY LATITUDE LONGITUDE'
    print '--lnsFile<file>\t\tFile containing list of local name servers'
    print '\t\t\tFormat: local name server (host name) per line'
    print '--cacheSize <size>\tSize of cache'
    print '--primary <#>\t\tNumber of primary name servers per name'
    print '--location\t\tLocation Based selection of active name servers.' 
    print '\t\t\tPeriodically send name server votes.'
    print '--voteInterval <#>\tInterval in seconds between name server votes'
    print '--name <name>\t\tName being queried'
    print '--zipfWorkload\t\tUse Zipf distribution to generate workload'
    print '--alpha <#>\t\tValue of alpha in the Zipf Distribution'
    print '--rWorkload <size>\tRegular workload size'
    print '--mWorkload <size>\tMobile workload size'
    print '--workloadFile <file>\tList of names queried'
    print '\t\t\tFormat: one name per line'
    print '--numLookup <#>\t\tNumber of lookup request'
    print '--numUpdate <#>\t\tNumber of update request'
    print '--lookupRate <ms>\t\tTime (in ms) between lookup transmission'
    print '--updateRateMobile <ms>\t\tInter-arrival time (in ms) between updates for mobile name'
    print '--updateRateRegular <ms>\t\tInter-arrival ti (in ms) me between updates for regular name'
    print '--expRunTime <sec>\t\tNumber of seconds the experiment is executed'
    print '--debugMode\t\tRun in debug mode. Generates log in file log_lns_id'
    print '--help\t\t\tPrints usage messages'


def get_event_rate(exp_time,trace_file):
    """Get average rate of events"""
    if exp_time <= 0:
        return 1000
    event_rate_f = float(exp_time) * 1000 / num_events(trace_file)
    event_rate = int(event_rate_f)
    return event_rate
    
def num_events(trace_file):
    """Count number of events in trace file"""
    if trace_file == '' or not os.path.isfile(trace_file):
        return 1
    f = open(trace_file, 'r')
    num_lookups = 0.0
    for line in f:
        name = line.split()
        if name:
            num_lookups += 1.0
    if num_lookups > 0:
    	return num_lookups
    else:
	#print 'num requests is zero'
        return 1

""" Executes an instance of the Local Name Server with the give parameters """
def run_local_name_server():
    #command = 'nohup java -Djava.util.logging.config.file=/Users/abhigyan/Documents/workspace/GNRS-westy/logging.properties -cp ' + local_name_server_jar + ' edu.umass.cs.gnrs.main.StartLocalNameServer '
    command = 'nohup java  -cp ' + local_name_server_jar + ' edu.umass.cs.gns.main.StartLocalNameServer '
    if is_local:
        command += ' ' + LOCAL_EXP
    #else:
    #    command += ' ' + PLANETLAB_EXP
    command += ' ' + ID + ' ' + str(node_id)
    command += ' ' + NAMESERVER_FILE + ' ' + name_server_file
    command += ' ' + CACHE_SIZE + ' ' + str(cache_size)
    command += ' ' + PRIMARY_NAMESERVERS + ' ' + str(primary_name_server)
    if is_location_replication:
        command += ' ' + LOCATION_REPLICATION
        command += ' ' + VOTE_INTERVAL + ' ' + str(vote_interval)
        command += ' ' + CHOOSE_FROM_CLOSEST_K + ' ' + str(choose_from_closest_k)
    if is_optimal_replication:
	command += ' ' + OPTIMAL_REPLICATION
	command += ' ' + REPLICATION_INTERVAL + ' ' + str(replication_interval)
	command += ' ' + OPTIMAL_TRACE + ' ' + optimal_trace_file

    if is_beehive_replication:
        command += ' ' + BEEHIVE_REPLICATION
        command += ' ' + BEEHIVEDHTBASE + ' ' + str(beehive_base)
        command += ' ' + BEEHIVELEAFSET + ' ' + str(beehive_leaf_set)

    if load_balancing:
        command += ' ' + LOAD_BALANCING
        command += ' ' + LOAD_MONITOR_INTERVAL + ' ' + str(load_monitor_interval)
    
    if is_zipf_workload:
        command += ' ' + ZIPF_WORKLOAD
        command += ' ' + ALPHA + ' ' + str(alpha)
        command += ' ' + REGULAR_WORLOAD + ' ' + str(regular_workload)
        command += ' ' + MOBILE_WORLOAD + ' ' + str(mobile_workload)
        command += ' ' + WORKLOAD_FILE + ' ' + workload_file

    if not lookup_trace_file  == '':
        command += ' ' + LOOKUP_TRACE_FILE + ' ' + lookup_trace_file
        
    if not update_trace_file  == '':
        command += ' ' + UPDATE_TRACE_FILE + ' ' + update_trace_file
    #else:
    #    command += ' ' + NAME + ' ' + name
    
    #command += ' ' + NUM_LOOKUP + ' ' + str(num_lookups)
    #command += ' ' + NUM_Update + ' ' + str(num_updates)
    
    command += ' ' + LOOKUP_RATE + ' ' + str(lookup_rate)
    command += ' ' + UPDATE_RATE_MOBILE + ' ' + str(update_rate_mobile)
    command += ' ' + UPDATE_RATE_REGULAR + ' ' + str(update_rate_regular)


    command += ' ' + NUMER_OF_TRANSMISSIONS  + ' ' + str(numberOfTransmissions)
    command += ' ' + MAX_QUERY_WAIT_TIME  + ' ' + str(maxQueryWaitTime)
    command += ' ' + QUERY_TIMEOUT + ' ' + str(queryTimeout)

    if adaptiveTimeout == True:
        command += ' ' + ADAPTIVE_TIMEOUT
        command += ' ' + DELTA + ' ' + str(delta)
        command += ' ' + MU + ' ' + str(mu)
        command += ' ' + PHI + ' ' + str(phi)
    if delay_scheduling:
        command += ' ' + DELAY_SCHEDULING

    command += ' ' + FILE_LOGGING_LEVEL + ' ' + file_logging_level
    command += ' ' + CONSOLE_OUTPUT_LEVEL + ' ' + console_output_level
    command += ' ' + STAT_FILE_LOGGING_LEVEL + ' ' + stat_file_logging_level
    command += ' ' + STAT_CONSOLE_OUTPUT_LEVEL + ' ' + stat_console_output_level

    if run_http_server:
        command += ' ' + RUN_HTTP_SERVER

    if is_experiment_mode:
        command += ' ' + EXPERIMENT_MODE
    if is_debug_mode:
        command += ' ' + DEBUG_MODE
        command += ' > log_lns_' + str(node_id)
        print command
    else:
        command += ' > log_lns'
    command += ' &'
    os.system(command)


def get_node_id():
    """This node's ID from pl_config file."""
    host_name = socket.getfqdn()
    host_name2 = socket.gethostname()
    f = open(name_server_file)
    for line in f:
        tokens = line.split()
        if (tokens[2] == host_name or tokens[2] == host_name2) and tokens[1] != 'yes':
            return int(tokens[0])
    print 'Host:' + host_name + ' Node Id: -1'
    sys.exit(2)

                                                
""" Look up local name server id from file. """
def get_local_name_server_id():      
    file = open(local_name_server_file, 'r')
    cmd = "uname -n"
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    process.wait()
    host_name = process.stdout.read().rstrip('\n')
    id = 0
    for line in file.readlines():
        lns_name = line.rstrip('\n')
        if host_name == lns_name:
            return id
        id += 1
    return id


""" Prints options during debug mode """
def print_options():
    if is_debug_mode:
        print "Jar: " + local_name_server_jar
        print "Local: " + str(is_local) 
        print "Id: " + str(node_id)
        print "NameServer File: " + name_server_file
        print "LocalNameServer File: " + local_name_server_file
        print "Cache Size: " + str(cache_size)
        print "Primary NameServer: " + str(primary_name_server)
        print "Location Replication: " + str(is_location_replication)
        print "Beehive Replication: " + str(is_beehive_replication)
        print "Vote Interval: " + str(vote_interval) + "sec"
        print "Name: " + name
        print "Zipf Workload: " + str(is_zipf_workload)
        print "Alpha:" + str(alpha)  
        print "Regular Workload Size: " + str(regular_workload) 
        print "Mobile Workload Size: " + str(mobile_workload) 
        print "Workload File: " + workload_file 
        print "Lookup Trace File:" + lookup_trace_file
        print "Update Trace File:" + update_trace_file
        print "Number of Lookup: " + str(num_lookups) 
        print "Number of Update: " + str(num_updates) 
        print "Lookup Rate: " + str(lookup_rate) + "ms"
        print "Update Rate Mobile: " + str(update_rate_mobile) + "ms"
        print "Update Rate Regular: " + str(update_rate_regular) + "ms"
        print "Experiment Run Time: " + str(experiment_run_time) + "sec"
        print "Optimal Replication: " + str(is_optimal_replication)
	print "Replication Interval: " + str(replication_interval) + "str"
	print "Optimal Trace File: " + optimal_trace_file
        print "Debug Mode: " + str(is_debug_mode) 

def main(argv):
    try:                                
        opts, args = getopt.getopt(argv, 'ab:c:d:e:fg:h:i:jk:l:m:n:o:p:qr:tuv:w:x:y:z:s:', 
                                   ['local', 'id=', 'nsFile=', 'lnsFile=',
                                    'cacheSize=', 
                                    'location', 'voteInterval=', 'primary=', 
                                    'name=', 'zipfWorkload', 'rWorkload=', 
                                    'mWorkload=', 'alpha=', 'numLookup=',  
                                    'numUpdate=', 'workloadFile=', 'debugMode',
                                    'jar=', 'help', 'planetlab', 'lookupRate=', 'updateRateMobile=', 
                                    'updateRateRegular=', 'lookupTrace=', 'updateTrace=',
                                    'expRunTime=' , 'rInterval=', 'optimalTrace=', 'optimal'])
    except getopt.GetoptError:   
        print 'Incorrect option'      
        usage()                         
        sys.exit(2)

    global local_name_server_jar
    global node_id
    global name_server_file
    global local_name_server_file
    global is_local 
    global regular_workload 
    global mobile_workload 
    global workload_file 
    global lookup_trace_file
    global update_trace_file
    global primary_name_server 
    global alpha 
    global num_lookups 
    global num_updates 
    global name 
    global is_zipf_workload 
    global is_location_replication 
    global vote_interval 
    global cache_size 
    global lookup_rate
    global update_rate_mobile
    global update_rate_regular
    global experiment_run_time
    global optimal_trace_file
    global is_optimal_replication
    global replication_interval
    global is_debug_mode
    
    for opt, arg in opts:
        if opt == '--local':
            is_local = True
        elif opt == '--id':
            node_id = int(arg)
        elif opt == '--nsFile':
            name_server_file = arg
        elif opt == '--lnsFile':
            local_name_server_file = arg
        elif opt == '--cacheSize':
            cache_size = int(arg)
        elif opt == '--location':
            is_location_replication = True
        elif opt == '--voteInterval':
            vote_interval = int(arg)
        elif opt == '--primary':
            primary_name_server = int(arg)
        elif opt == '--name':
            name = arg
        elif opt == '--zipfWorkload':
            is_zipf_workload = True
        elif opt == '--rWorkload':
            regular_workload = int(arg);
        elif opt == '--mWorkload':
            mobile_workload = int(arg)
        elif opt == '--alpha':
            alpha = float(arg)
        elif opt == '--numLookup':
            num_lookups = int(arg)
        elif opt == '--numUpdate':
            num_updates = int(arg)
        elif opt == '--workloadFile':
            workload_file = arg
        elif opt == '--lookupTrace':
            lookup_trace_file = arg
        elif opt == '--updateTrace':
            update_trace_file = arg
        elif opt == '--debugMode':
            is_debug_mode = True
        elif opt == '--jar':
            local_name_server_jar = arg
        elif opt == '--lookupRate':
            lookup_rate = int(arg)
        elif opt == '--updateRateMobile':
            update_rate_mobile = int(arg)
        elif opt == '--updateRateRegular':
            update_rate_regular = int(arg)
        elif opt == '--expRunTime':
            experiment_run_time = int(arg)
        elif opt == '--optimal':
	    is_optimal_replication = True
        elif opt == '--rInterval':
	    replication_interval = int(arg)
        elif opt == '--optimalTrace':
	    optimal_trace_file = arg
        elif opt == '--help':
            usage()
            sys.exit(1)
        else:
            print 'Incorrect Arguments: wrong option ' + opt
            usage()                         
            sys.exit(2)
    # check whether files are present or not.
    if lookup_trace_file != '':
        check_file(lookup_trace_file)
    if update_trace_file != '':
        check_file(update_trace_file)
    
    check_file(local_name_server_jar)
    check_file(name_server_file)
    
    if node_id == -1 and not name_server_file == '':
        node_id =  get_node_id()
        print 'Node ID', node_id
    
    lookup_rate = get_event_rate(experiment_run_time,lookup_trace_file)
    print 'Lookup Rate:', lookup_rate 
    update_rate_regular = get_event_rate(experiment_run_time,update_trace_file)
    print 'Update Rate Regular:', update_rate_regular 
    
    
    #print_options()
    run_local_name_server()


def check_file(filename):
    if not os.path.exists(filename):
        print '\n\n\n\n\n'
        os.system('hostname')
        print 'File does not exist:', filename
        print '\n\n\n\n\n'
        fw = open('fileError','w')
        fw.write('I QUIT! File does not exist:' + filename)
        fw.close()
        sys.exit(1)
                        

if __name__ == "__main__":
    main(sys.argv[1:])
