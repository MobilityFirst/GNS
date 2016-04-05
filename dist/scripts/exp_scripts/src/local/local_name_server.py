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
UPDATE_RATE_MOBILE = '-updateRateMobile'
UPDATE_RATE_REGULAR = '-updateRateRegular'
DEBUG_MODE = '-debugMode'
EXPERIMENT_MODE = '-experimentMode'
HELP = '-help'

EMULATE_PING_LATENCIES = '-emulatePingLatencies'
VARIATION = '-variation'

BEEHIVE_REPLICATION = '-beehive'
BEEHIVEDHTBASE = '-beehiveBase'
BEEHIVELEAFSET = '-leafSet'

LOAD_BALANCING = '-loadDependentRedirection'
LOAD_MONITOR_INTERVAL = '-nsLoadMonitorIntervalSeconds'

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
local_name_server_jar = exp_config.gnrs_jar              #Local name server jar
node_config = exp_config.node_config                      #Name server information: ID Node_Name Ping_Latency Latitude Longitude

primary_name_server = exp_config.primary_name_server                             #Number of primary name servers
cache_size = 10000                                    #Cache Size
name = ''

workload_file = exp_config.wfile


## NOT USED. was used for zipf workload
is_zipf_workload = False                             #Use zipf distribution for generating lookup request
                                                    #List of names queried at this local name server
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
#choose_from_closest_k = 1                           #Choose from K-closest to vote for a name

# Load balance
load_balancing = exp_config.load_balancing                 # local name servers start load balancing among name servers
load_monitor_interval = exp_config.replication_interval    # interval of monitoring load at every nameserver (seconds)

# Optimal Replication

# Beehive replication
is_beehive_replication = exp_config.is_beehive_replication                       # Beehive replication
beehive_base = 16                                   # Beehive DHT base, default 16
beehive_leaf_set = 4                                # Beehive Leaf set size
                                                    # must be less thant number of name servers, default 24

# Experiment duration
is_experiment_mode = exp_config.is_experiment_mode        # Always set to True to run experiments.
is_debug_mode = exp_config.is_debug_mode                                # Prints logs if True. Used for testing.
experiment_run_time  = exp_config.experiment_run_time    # in seconds

emulate_ping_latencies = exp_config.emulate_ping_latencies
variation = exp_config.variation


# retransmission parameters
numberOfTransmissions = 3                         # maximum number of times a query is transmitted
maxQueryWaitTime = exp_config.maxQueryWaitTime    # maximum  Wait Time before query is  declared failed (milli-seconds)
queryTimeout = exp_config.queryTimeout            # timeout interval
adaptiveTimeout = False
delta = 0.05                                   # Weight assigned to latest sample in calculating moving average.
mu = 1.0                                       # Co-efficient of estimated RTT in calculating timeout.
phi = 6.0                                      # Co-efficient of deviation in calculating timeout.

# logging related parameters:
## values: ALL, OFF, INFO, FINE, FINER, FINEST,.. see java documentation.
# logging related parameters:
file_logging_level = exp_config.lnslog
console_output_level = exp_config.lnslog
stat_file_logging_level = exp_config.lnslogstat
stat_console_output_level = exp_config.lnslogstat


def run_local_name_server(node_id, work_dir, update_trace_file):
    """ Executes an instance of the Local Name Server with the give parameters """

    check_file(local_name_server_jar)
    check_file(node_config)
    # print 'Lookup trace:', lookup_trace_file
    print 'Update trace:', update_trace_file
    if node_id == -1 and not node_config == '':
        node_id = get_node_id()
        print 'Node ID', node_id

    work_dir = os.path.join(work_dir, 'log_lns_' + node_id)
    os.system('rm -rf ' + work_dir + '; mkdir -p ' + work_dir)

    # check whether files are present or not.
    # if lookup_trace_file is not None and lookup_trace_file != '':
    #     check_file(lookup_trace_file)
    if update_trace_file is not None and update_trace_file != '':
        check_file(update_trace_file)

    # lookup_rate = get_event_rate(experiment_run_time, lookup_trace_file)
    # print 'Lookup Rate:', lookup_rate
    update_rate_regular = get_event_rate(experiment_run_time, update_trace_file)
    print 'Update Rate Regular:', update_rate_regular
    update_rate_mobile = 0      # in ms (NOT USED) Inter-Arrival Time (in ms) between update request for mobile names

    command = 'cd ' + work_dir + ';'
    command += 'nohup java -ea -cp ' + local_name_server_jar + \
               ' -Djava.rmi.server.useCodebaseOnly=false -Djava.rmi.server.codebase=file:' + local_name_server_jar +\
                ' edu.umass.cs.gns.main.StartLocalNameServer '

    command += ' ' + ID + ' ' + str(node_id)
    command += ' ' + NAMESERVER_FILE + ' ' + node_config
    command += ' ' + CACHE_SIZE + ' ' + str(cache_size)
    command += ' ' + PRIMARY_NAMESERVERS + ' ' + str(primary_name_server)
    if is_location_replication:
        command += ' ' + LOCATION_REPLICATION
        command += ' ' + VOTE_INTERVAL + ' ' + str(vote_interval)

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

    if update_trace_file is not None and update_trace_file != '':
        command += ' ' + UPDATE_TRACE_FILE + ' ' + update_trace_file
        command += ' ' + UPDATE_RATE_REGULAR + ' ' + str(update_rate_regular)

    if workload_file is not None and workload_file != '':
        command += ' ' + WORKLOAD_FILE + ' ' + workload_file

    command += ' ' + MAX_QUERY_WAIT_TIME + ' ' + str(maxQueryWaitTime)
    command += ' ' + QUERY_TIMEOUT + ' ' + str(queryTimeout)

    if emulate_ping_latencies:
        command += ' ' + EMULATE_PING_LATENCIES
        command += ' ' + VARIATION + ' ' + str(variation)

    if adaptiveTimeout:
        command += ' ' + ADAPTIVE_TIMEOUT
        command += ' ' + DELTA + ' ' + str(delta)
        command += ' ' + MU + ' ' + str(mu)
        command += ' ' + PHI + ' ' + str(phi)

    command += ' ' + FILE_LOGGING_LEVEL + ' ' + file_logging_level
    command += ' ' + CONSOLE_OUTPUT_LEVEL + ' ' + console_output_level
    command += ' ' + STAT_FILE_LOGGING_LEVEL + ' ' + stat_file_logging_level
    command += ' ' + STAT_CONSOLE_OUTPUT_LEVEL + ' ' + stat_console_output_level

    if is_experiment_mode:
        command += ' ' + EXPERIMENT_MODE
    if is_debug_mode:
        command += ' ' + DEBUG_MODE
    out_file = os.path.join(work_dir, 'log.out')
    err_file = os.path.join(work_dir, 'log.err')
    command += ' > ' + out_file
    command += ' 2> ' + err_file
    command += ' &'
    print command
    os.system(command)


# BELOW: Various utility methods to generate the big command above

def get_node_id():
    """This node's ID from pl_config file."""
    host_name = socket.getfqdn()
    host_name2 = socket.gethostname()
    f = open(node_config)
    for line in f:
        tokens = line.split()
        if (tokens[2] == host_name or tokens[2] == host_name2) and tokens[1] != 'yes':
            return int(tokens[0])
    print 'Host:' + host_name + ' Node Id: -1'
    sys.exit(2)


def check_file(filename):
    """
    Checks if the given file exists. Prints error message otherwise
    """
    if not os.path.exists(filename):
        print '\n\n\n\n\n'
        os.system('hostname')
        print 'File does not exist:', filename
        print '\n\n\n\n\n'
        fw = open('fileError', 'w')
        fw.write('I QUIT! File does not exist:' + filename)
        fw.close()
        sys.exit(1)


def get_event_rate(exp_time, trace_file):
    """Get average rate of events"""
    if exp_time <= 0:
        return 1000
    event_rate_f = float(exp_time) * 1000 / num_events(trace_file)
    event_rate = int(event_rate_f)
    return event_rate


def num_events(trace_file):
    """Count number of events in trace file"""
    if trace_file is None or trace_file == '' or not os.path.isfile(trace_file):
        return 1
    f = open(trace_file, 'r')
    event_count = 0.0
    for line in f:
        name1 = line.split()
        if name1:
            event_count += 1.0
    if event_count > 0:
        return event_count
    else:
    #print 'num requests is zero'
        return 1
