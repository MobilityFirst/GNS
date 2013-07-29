#!/usr/bin/python   
                                                            
import getopt
import os
import sys
import subprocess

#Constants: Do not edit
ID = '-id'
NAMESERVER_FILE = '-nsfile'
LOCAL_EXP = '-local'
PLANETLAB_EXP = '-planetlab'
REGULAR_WORLOAD = '-rworkload'
MOBILE_WORLOAD = '-mname-server-infoworkload'
WORKLOAD_FILE = '-wfile'
LOOKUP_TRACE_FILE = '-lookupTrace'
UPDATE_TRACE_FILE = '-updateTrace'
PRIMARY_NAMESERVERS = '-primary'
ALPHA = '-alpha'
CACHE_SIZE = '-cacheSize'
ZIPF_WORKLOAD = '-zipf'
LOCATION_REPLICATION = '-location'
NAME = '-name'
NUM_LOOKUP = '-nlookup'
NUM_Update = '-nUpdate'
VOTE_INTERVAL = '-vInterval'
LOOKUP_RATE = '-lookupRate'
UPDATE_RATE_MOBILE = '-updateRateMobile'
UPDATE_RATE_REGULAR = '-updateRateRegular'
DEBUG_MODE = '-debugMode'
HELP = '-help'

#Parameter: Update as required
local_name_server_jar = '../../build/jars/gnrs-lns.jar'              #Local name server jar
node_id = -1                                         #Node Id. Selected from name_server_file when running on planetlab
name_server_file = 'name-server-info'           #Name server information: ID Node_Name Ping_Latency Latitude Longitude
local_name_server_file = ''     #List of local name server. Once planetlab node per line
is_local = False                                    #Flag for indicating the execution environment
                                                    #Setting this False implies PlanetLab as the execution environment
primary_name_server = 3                             #Number of primary name servers
cache_size = 3000                                   #Cache Size
name = 'server 1'
is_zipf_workload = False                             #Do not Use zipf distribution for generating lookup request
workload_file = ''                           #List of names queried at this local name server
lookup_trace_file = ''
update_trace_file = ''
regular_workload = 0                             #Size of regular workload
mobile_workload = 0                            #Size of mobile workload
alpha = 0.91                                        #Alpha for Zipf distribution
num_lookups = 0                                #Number of lookup queries generated at the local name server
num_updates = 0                                #Number of update queries generated at the local name server 
lookup_rate = 1000           #in ms                  #Inter-Arrival Time (in ms) between lookup queries  
update_rate_mobile = 1000      #in ms             #Inter-Arrival Time (in seconds) between update request for mobile names   
update_rate_regular = 3000    #in ms             #Inter-Arrival Time (in seconds) between update request for regular names
is_location_replication = False                     #Select location aware replication. If True, the local name server periodically
                                                    #(once every vote_interval) votes for its closest name server
                                                    #Set it to False for random replication
vote_interval = 180                                 #Time between votes (in seconds)
is_debug_mode = True                               #Prints logs if True. Used for testing.

""" Prints Usage Message """
def usage():
    print 'USAGE: local-name-server.py [options...]' 
    print 'Runs a local name server instance with the specified options'
    print '\nOptions:'
    print '--jar <*.jar>\t\tLocal name server jar file including path'
    print '--local\t\t\tRun Local Name Server instance on localhost'
    print '--planetlab\t\t\tRun Local Name Server instance on PlanetLab node'
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
    print '--debugMode\t\tRun in debug mode. Generates log in file log_lns_id'
    print '--help\t\t\tPrints usage messages'
    
""" Executes an instance of the Local Name Server with the give parameters """
def run_local_name_server():
    command = 'java -jar ' + local_name_server_jar
    if is_local:
        command += ' ' + LOCAL_EXP
    else:
        command += ' ' + PLANETLAB_EXP
    command += ' ' + ID + ' ' + str(node_id)
    command += ' ' + NAMESERVER_FILE + ' ' + name_server_file
    command += ' ' + CACHE_SIZE + ' ' + str(cache_size)
    command += ' ' + PRIMARY_NAMESERVERS + ' ' + str(primary_name_server)
    if is_location_replication:
        command += ' ' + LOCATION_REPLICATION
        command += ' ' + VOTE_INTERVAL + ' ' + str(vote_interval)
    if is_zipf_workload:
        command += ' ' + ZIPF_WORKLOAD
        command += ' ' + ALPHA + ' ' + str(alpha)
        command += ' ' + REGULAR_WORLOAD + ' ' + str(regular_workload)
        command += ' ' + MOBILE_WORLOAD + ' ' + str(mobile_workload)
        command += ' ' + WORKLOAD_FILE + ' ' + workload_file
        command += ' ' + LOOKUP_TRACE_FILE + ' ' + lookup_trace_file
        command += ' ' + UPDATE_TRACE_FILE + ' ' + update_trace_file
    else:
        command += ' ' + NAME + ' ' + name 
    command += ' ' + NUM_LOOKUP + ' ' + str(num_lookups)
    command += ' ' + NUM_Update + ' ' + str(num_updates)
    command += ' ' + LOOKUP_RATE + ' ' + str(lookup_rate)
    command += ' ' + UPDATE_RATE_MOBILE + ' ' + str(update_rate_mobile)
    command += ' ' + UPDATE_RATE_REGULAR + ' ' + str(update_rate_regular)
    if is_debug_mode:
        command += ' ' + DEBUG_MODE
#        command += ' > log_lns_' + str(node_id)
        print command
#    else:
#        command += ' > log_lns'
        
    os.system(command)

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
        print "Debug Mode: " + str(is_debug_mode) 

def main(argv):
    try:                                
        opts, args = getopt.getopt(argv, 'ab:c:d:e:fg:h:i:jk:l:m:n:o:p:qr:tuv:w:x:y:z:', 
                                   ['local', 'id=', 'nsFile=', 'lnsFile=',
                                    'cacheSize=', 
                                    'location', 'voteInterval=', 'primary=', 
                                    'name=', 'zipfWorkload', 'rWorkload=', 
                                    'mWorkload=', 'alpha=', 'numLookup=',  
                                    'numUpdate=', 'workloadFile=', 'debugMode',
                                    'jar=', 'help', 'planetlab', 'lookupRate=', 'updateRateMobile=', 
                                    'updateRateRegular=', 'lookupTrace=', 'updateTrace=' ])
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
        elif opt == '--planetlab':
            is_local = False
        elif opt == '--lookupRate':
            lookup_rate = int(arg)
        elif opt == '--updateRateMobile':
            update_rate_mobile = int(arg)
        elif opt == '--updateRateRegular':
            update_rate_regular = int(arg)
        elif opt == '--help':
            usage()
            sys.exit(1)
        else:
            print 'Incorrect Arguments: wrong option ' + opt
            usage()                         
            sys.exit(2)
    
    if not local_name_server_file == '':
        node_id = get_local_name_server_id();
        
    print_options()
    run_local_name_server()

if __name__ == "__main__":
    main(sys.argv[1:])
