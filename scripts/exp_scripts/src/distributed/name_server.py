#!/usr/bin/env python

import os
import sys

import exp_config

#Constants: Do not edit
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
MIN_REPLICA = '-minReplica'
MAX_REPLICA = '-maxReplica'
MAX_REQ_RATE = '-maxReqRate'

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
SINGLE_NS = '-singleNS'
USE_GNS_NIO_TRANSPORT = '-useGNSNIOTransport'
DUMMY_GNS = '-dummyGNS'

READ_COORDINATION = '-readCoordination'
EVENTUAL_CONSISTENCY = '-eventualConsistency'
NO_PAXOS_LOG = '-noPaxosLog'
NO_LOAD_DB = '-noLoadDB'
#PRIMARY_PAXOS = '-primaryPaxos'
PAXOS_DISK_BACKUP = '-paxosDiskBackup'
PAXOS_LOG_FOLDER = '-paxosLogFolder'
FAILURE_DETECTION_MSG_INTERVAL = '-failureDetectionMsgInterval'
FAILURE_DETECTION_TIMEOUT_INTERVAL = '-failureDetectionTimeoutInterval'
PAXOS_START_MIN_DELAY = '-paxosStartMinDelaySec'
PAXOS_START_MAX_DELAY = '-paxosStartMaxDelaySec'
MONGO_PORT = '-mongoPort'
QUIT_AFTER_TIME = '-quitAfterTime'

KMEDOIDS_REPLICATION = '-kmedoids'
NUM_LNS = '-numLNS'
LNSNSPING_FILE = '-lnsnsping'
NSNSPING_FILE = '-nsnsping'

FILE_LOGGING_LEVEL = '-fileLoggingLevel'
CONSOLE_OUTPUT_LEVEL = '-consoleOutputLevel'
STAT_FILE_LOGGING_LEVEL = '-statFileLoggingLevel'
STAT_CONSOLE_OUTPUT_LEVEL = '-statConsoleOutputLevel'

GEN_WORKLOAD_SLEEP_TIME = '-syntheticWorkloadSleepTimeBetweenAddingNames'

WORKER_THREAD_COUNT = '-workerThreadCount'

MAX_LOG_NAME = '-maxLogName'


def run_name_server(node_id, config_file, node_config_file):
    """ Executes an instance of Name Server with the give parameters """

    # print 'Node id', node_id
    # print 'Config file', config_file
    # print 'Node config file', node_config_file

    if config_file is not None and config_file != '':
        exp_config.initialize(config_file)

    #Parameters: Update as required
    name_server_jar = exp_config.remote_jar_file
    primary_name_server = exp_config.primary_name_server
    aggregate_interval = exp_config.replication_interval  # In seconds
    replication_interval = exp_config.replication_interval  # In seconds
    # Used for calculating number of replicas as per this formula:
    # NumReplicas = lookupRate / (updateRate * normalizing_constant)
    normalizing_constant = exp_config.normalizing_constant
    moving_avg_window_size = 1  # Used for calculating inter-arrival update time and ttl value

    ttl_constant = 0.0  # Multiplied by inter-arrival update time to calculate ttl value of a name

    default_ttl_regular_name = 0  # TTL = 0 means no TTL, TTL = -1 means infinite TTL, else, TTL = TTL value in sec
    default_ttl_mobile_name = 0  # TTL = 0 means no TTL, TTL = -1 means infinite TTL, else, TTL = TTL value in sec

    regular_workload = exp_config.regular_workload
    mobile_workload = exp_config.mobile_workload

    is_optimal_replication = False  # Optimal

    is_static_replication = exp_config.is_static_replication  # Static3

    is_random_replication = exp_config.is_random_replication  # Uniform

    is_location_replication = exp_config.is_location_replication  # Locality
    name_server_selection_vote_size = exp_config.name_server_selection_vote_size  # top-k size.
    min_replica = exp_config.min_replica
    max_replica = exp_config.max_replica

    is_beehive_replication = exp_config.is_beehive_replication
    c_hop = exp_config.c_hop
    base = 16
    alpha = exp_config.alpha

    read_coordination = exp_config.read_coordination
    paxos_log_folder = exp_config.paxos_log_folder  # folder does paxos store its state in

    # Interval (in sec) between two failure detection messages sent to a node
    failure_detection_msg_interval = exp_config.failure_detection_msg_interval
    # Interval (in sec) after which a node is declared failed is no response is recvd for failure detection messages
    failure_detection_timeout_interval = exp_config.failure_detection_timeout_interval

    paxos_start_min_delay_sec = 0
    paxos_start_max_delay_sec = 0

    mongo_port = exp_config.mongo_port

    quit_after_time = exp_config.quit_after_time
    quit_node_id = exp_config.quit_node_id  # which node will quit

    is_debug_mode = exp_config.is_debug_mode
    is_experiment_mode = exp_config.is_experiment_mode  # Always set to True to run experiments

    emulate_ping_latencies = exp_config.emulate_ping_latencies
    variation = exp_config.variation

    # logging related parameters:
    ## values: ALL, OFF, INFO, FINE, FINER, FINEST,.. see java documentation.
    file_logging_level = exp_config.nslog
    console_output_level = exp_config.nslog
    stat_file_logging_level = exp_config.nslogstat
    stat_console_output_level = exp_config.nslogstat

    worker_thread_count = exp_config.worker_thread_count

    java_bin = exp_config.remote_java_bin

    check_file(name_server_jar)
    check_file(node_config_file)

    command = 'nohup ' + java_bin + '/java -cp ' + name_server_jar + ' ' + exp_config.ns_main

    command += ' ' + ID + ' ' + str(node_id)
    command += ' ' + NAMESERVER_FILE + ' ' + node_config_file
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
    else:
        print 'Error: No replication model selected'
        sys.exit(2)
    # min and max number of replica
    # if min_replica != primary_name_server:
    command += ' ' + MIN_REPLICA + ' ' + str(min_replica)
    # if max_replica != 100:
    command += ' ' + MAX_REPLICA + ' ' + str(max_replica)
    command += ' ' + MAX_REQ_RATE + ' ' + str(exp_config.max_req_rate)

    command += ' ' + FILE_LOGGING_LEVEL + ' ' + file_logging_level
    command += ' ' + CONSOLE_OUTPUT_LEVEL + ' ' + console_output_level
    command += ' ' + STAT_FILE_LOGGING_LEVEL + ' ' + stat_file_logging_level
    command += ' ' + STAT_CONSOLE_OUTPUT_LEVEL + ' ' + stat_console_output_level

    if primary_name_server == 1:
        command += ' ' + SINGLE_NS

    if emulate_ping_latencies:
        command += ' ' + EMULATE_PING_LATENCIES
        command += ' ' + VARIATION + ' ' + str(variation)

    if read_coordination is not False:
        command += ' ' + READ_COORDINATION

    if paxos_log_folder != '':
        command += ' ' + PAXOS_LOG_FOLDER + ' ' + os.path.join(paxos_log_folder, 'log_' + str(node_id))

    command += ' ' + FAILURE_DETECTION_MSG_INTERVAL + ' ' + str(failure_detection_msg_interval)
    command += ' ' + FAILURE_DETECTION_TIMEOUT_INTERVAL + ' ' + str(failure_detection_timeout_interval)

    command += ' ' + MONGO_PORT + ' ' + str(mongo_port)

    if quit_node_id == node_id and quit_after_time >= 0:
        command += ' ' + QUIT_AFTER_TIME + ' ' + str(quit_after_time)

    if paxos_start_min_delay_sec > 0:
        command += ' ' + PAXOS_START_MIN_DELAY + ' ' + str(paxos_start_min_delay_sec)
        command += ' ' + PAXOS_START_MAX_DELAY + ' ' + str(paxos_start_max_delay_sec)
    if exp_config.no_paxos_log:
        command += ' ' + NO_PAXOS_LOG
    if exp_config.use_gns_nio_transport:
        command += ' ' + USE_GNS_NIO_TRANSPORT
    if exp_config.dummy_gns:
        command += ' ' + DUMMY_GNS

    command += ' ' + WORKER_THREAD_COUNT + ' ' + str(worker_thread_count)

    if is_experiment_mode:
        command += ' ' + EXPERIMENT_MODE

    if is_debug_mode:
        command += ' ' + DEBUG_MODE
    command += ' > log_ns_' + str(node_id)
    command += ' 2> log_ns_' + str(node_id)
    command += ' &'
    print command
    os.system(command)


def check_file(filename):
    if not os.path.exists(filename):
        print '\n\tEXCEPTION\nEXCEPTION\nEXCEPTION\nEXCEPTION\n'
        print '************************************************'
        print 'File does not exist:', filename
        print '************************************************'
        print '\n\tEXCEPTION\nEXCEPTION\nEXCEPTION\nEXCEPTION\n'
        sys.exit(1)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser("local_name_server", "run local name server with options given in config file")
    parser.add_argument("--id", help="node ID of local name server")
    parser.add_argument("--configFile", help="config file to initialize exp_config parameters")
    parser.add_argument("--nsfile", help="file with node config")
    args = parser.parse_args()
    run_name_server(args.id, args.configFile, args.nsfile)
