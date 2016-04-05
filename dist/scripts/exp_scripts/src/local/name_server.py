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
PRIMARY_NAMESERVERS = '-primary'
AGGREGATE_INTERVAL = '-aInterval'
REPLICATION_INTERVAL = '-rInterval'
NORMALIZING_CONSTANT = '-nconstant'
MOVING_AVG_WINDOW_SIZE = '-mavg'
STATIC_REPLICATION = '-static'
RANDOM_REPLICATION = '-random'
LOCATION_REPLICATION = '-location'
NAMESERVER_SELECTION_VOTE_SIZE = '-nsVoteSize'
BEEHIVE_REPLICATION = '-beehive'
C = '-C'
ALPHA = '-alpha'
BASE = '-base'

SINGLE_NS = '-singleNS'

DEBUG_MODE = '-debugMode'
EXPERIMENT_MODE = '-experimentMode'
HELP = '-help'
EMULATE_PING_LATENCIES = '-emulatePingLatencies'
VARIATION = '-variation'
DUMMY_GNS = '-dummyGNS'

FILE_LOGGING_LEVEL =  '-fileLoggingLevel'
CONSOLE_OUTPUT_LEVEL = '-consoleOutputLevel'
STAT_FILE_LOGGING_LEVEL = '-statFileLoggingLevel'
STAT_CONSOLE_OUTPUT_LEVEL = '-statConsoleOutputLevel'

MONGO_PORT = '-mongoPort'

MULTIPAXOS = '-multipaxos'

PAXOS_LOG_FOLDER = '-paxosLogFolder'
FAILURE_DETECTION_MSG_INTERVAL = '-failureDetectionMsgInterval'
FAILURE_DETECTION_TIMEOUT_INTERVAL = '-failureDetectionTimeoutInterval'
NO_PAXOS_LOG = '-noPaxosLog'


#Parameters: Update as required
name_server_jar = exp_config.gnrs_jar
is_local = False                          # Run the name server instance on the local host.
node_config = exp_config.node_config
primary_name_server = exp_config.primary_name_server
aggregate_interval = exp_config.replication_interval      # In seconds
replication_interval = exp_config.replication_interval      # In seconds
#exp_config.replication_interval    #In seconds
normalizing_constant = 0.1   # Used as the denominator for calculating number of replicas
                            #NumReplicas = lookupRate / (updateRate * normalizing_constant)
moving_avg_window_size = 20  # Used for calculating inter-arrival update time and ttl value


is_static_replication = exp_config.is_static_replication                #Static3

is_random_replication = exp_config.is_random_replication               #Uniform

is_location_replication = exp_config.is_location_replication             #Locality
name_server_selection_vote_size = 5         # top-k size. this parameter is not used anymore.

is_beehive_replication = exp_config.is_beehive_replication
c_hop = 0.3
base = 16
alpha = 0.91
is_debug_mode = exp_config.is_debug_mode
is_experiment_mode = exp_config.is_experiment_mode                # Always set to True to run experiments
emulate_ping_latencies = exp_config.emulate_ping_latencies
variation = exp_config.variation


#persistent_data_store = False
mongo_port = exp_config.mongo_port
paxos_log_folder = exp_config.paxos_log_folder
failure_detection_msg_interval = exp_config.failure_detection_msg_interval           # Interval (in sec) between
                                                                # two failure detection messages sent to a node
failure_detection_timeout_interval = exp_config.failure_detection_timeout_interval       # Interval (in sec) after
                                    # which a node is declared as failed if it does not responsd to failure messages

# logging related parameters:
# values: ALL, OFF, INFO, FINE, FINER, FINEST,.. see java documentation.
file_logging_level = exp_config.nslog
console_output_level = exp_config.nslog
stat_file_logging_level = exp_config.nslogstat
stat_console_output_level = exp_config.nslogstat


def run_name_server(node_id, work_dir):
    """ Executes an instance of Name Server with the give parameters """
    check_file(name_server_jar)
    check_file(node_config)

    if node_id is None or node_id == -1:
        node_id = get_node_id()
    work_dir = os.path.join(work_dir, 'log_ns_' + node_id)
    os.system('rm -rf ' + work_dir + '; mkdir -p ' + work_dir)

    command = 'cd ' + work_dir + ';'
    command += 'nohup java -ea -cp ' + name_server_jar + ' edu.umass.cs.gns.main.StartNameServer'
    command += ' ' + ID + ' ' + str(node_id)
    command += ' ' + NAMESERVER_FILE + ' ' + node_config
    command += ' ' + PRIMARY_NAMESERVERS + ' ' + str(primary_name_server)
    command += ' ' + AGGREGATE_INTERVAL + ' ' + str(aggregate_interval)
    command += ' ' + REPLICATION_INTERVAL + ' ' + str(replication_interval)
    command += ' ' + NORMALIZING_CONSTANT + ' ' + str(normalizing_constant)
    command += ' ' + MOVING_AVG_WINDOW_SIZE + ' ' + str(moving_avg_window_size)
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

    if primary_name_server == 1:
        command += ' ' + SINGLE_NS

    command += ' ' + FILE_LOGGING_LEVEL + ' ' + file_logging_level
    command += ' ' + CONSOLE_OUTPUT_LEVEL + ' ' + console_output_level
    command += ' ' + STAT_FILE_LOGGING_LEVEL + ' ' + stat_file_logging_level
    command += ' ' + STAT_CONSOLE_OUTPUT_LEVEL + ' ' + stat_console_output_level
    
    command += ' ' + PAXOS_LOG_FOLDER + ' ' + paxos_log_folder + '/log_'  + str(node_id)
    command += ' ' + FAILURE_DETECTION_MSG_INTERVAL + ' ' + str(failure_detection_msg_interval)
    command += ' ' + FAILURE_DETECTION_TIMEOUT_INTERVAL + ' ' + str(failure_detection_timeout_interval)
    if exp_config.no_paxos_log:
        command += ' ' + NO_PAXOS_LOG

    if emulate_ping_latencies:
        command += ' ' + EMULATE_PING_LATENCIES
        command += ' ' + VARIATION + ' ' + str(variation)
    if exp_config.multipaxos:
        command += ' ' + MULTIPAXOS
    if exp_config.dummy_gns:
        command += ' ' + DUMMY_GNS

    #if persistent_data_store:
    #    command += ' ' + PERSISTENT_DATA_STORE
    if mongo_port > 0:
        command += ' ' + MONGO_PORT + ' ' + str(mongo_port)
    #if primary_paxos:
    #    command += ' ' + PRIMARY_PAXOS
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


def get_node_id():
    """Name server ID from pl_config file."""
    host_name = socket.getfqdn()
    host_name2 = socket.gethostname()
    
    f = open(node_config)
    for line in f:
        tokens = line.split()
        if (tokens[2] == host_name or tokens[2] == host_name2) and tokens[1] == 'yes':
            return int(tokens[0])
    print 'Host:' + host_name + ' Node Id: -1'
    sys.exit(2)


def get_name_server_id():
    """ Look up name server id from file. """
    file = open(node_config, 'r')
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


def check_file(filename):
    if not os.path.exists(filename):
        print '\n\tEXCEPTION\nEXCEPTION\nEXCEPTION\nEXCEPTION\n'
        print '************************************************'
        print 'File does not exist:', filename
        print '************************************************'
        print '\n\tEXCEPTION\nEXCEPTION\nEXCEPTION\nEXCEPTION\n'
        sys.exit(1)


