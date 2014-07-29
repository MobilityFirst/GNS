#!/usr/bin/env python

import os
import sys

import exp_config


#Constants: Do not edit
ID = '-id'
NAMESERVER_FILE = '-nsfile'
REGULAR_WORLOAD = '-rworkload'
MOBILE_WORLOAD = '-mworkload'
WORKLOAD_FILE = '-wfile'
UPDATE_TRACE_FILE = '-updateTrace'
PRIMARY_NAMESERVERS = '-primary'
ALPHA = '-alpha'
CACHE_SIZE = '-cacheSize'
ZIPF_WORKLOAD = '-zipf'
LOCATION_REPLICATION = '-location'
REPLICATION_INTERVAL = '-rInterval'
NAME = '-name'
VOTE_INTERVAL = '-vInterval'
LOOKUP_RATE = '-lookupRate'
UPDATE_RATE_REGULAR = '-updateRateRegular'
OUTPUT_SAMPLE_RATE = '-outputSampleRate'
DEBUG_MODE = '-debugMode'
EXPERIMENT_MODE = '-experimentMode'
HELP = '-help'

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

EMULATE_PING_LATENCIES = '-emulatePingLatencies'
VARIATION = '-variation'

FILE_LOGGING_LEVEL = '-fileLoggingLevel'
CONSOLE_OUTPUT_LEVEL = '-consoleOutputLevel'
STAT_FILE_LOGGING_LEVEL = '-statFileLoggingLevel'
STAT_CONSOLE_OUTPUT_LEVEL = '-statConsoleOutputLevel'


def get_event_rate(exp_time, trace_file):
    """Get average rate of events"""
    event_rate_f = float(exp_time) * 1000 / num_events(trace_file)
    #    event_rate = int(event_rate_f)
    #    if event_rate == 0:
    #        event_rate = 1
    return event_rate_f


def num_events(trace_file):
    """Count number of events in trace file"""
    if trace_file == '' or not os.path.isfile(trace_file):
        print 'Trace file does not exist: ', trace_file
        return 1
    f = open(trace_file, 'r')
    events = 0.0
    for line in f:
        name1 = line.split()
        if name1:
            #if int(line) >= 10000000:
            events += 1.0
    if events > 0:
        return events
    else:
        print 'num EVENTS is zero'
        return 1


def run_local_name_server(node_id, config_file, node_config_file, update_trace, workload_config_file):
    """ Executes an instance of the Local Name Server with the give parameters """
    # print 'node id ', node_id
    # print 'config file', config_file
    # print 'node config file', node_config_file
    # print 'update trace', update_trace
    # print 'workload config file', workload_config_file

    if config_file is not None and config_file != '':
        exp_config.initialize(config_file)

    local_name_server_jar = exp_config.remote_jar_file  # Local name server jar
    # node_config_file = 'pl_config'  # Name server information: ID Node_Name Ping_Latency Latitude Longitude
    primary_name_server = exp_config.primary_name_server  # Number of primary name servers
    cache_size = exp_config.cache_size  # Cache Size

    output_sample_rate = exp_config.output_sample_rate

    # Location Replication / Random Replication
    # Select location aware replication. If True, the local name server periodically
    is_location_replication = exp_config.is_location_replication
    #(once every vote_interval) votes for its closest name server
    #Set it to False for random replication
    vote_interval = exp_config.replication_interval  # Time between votes (in seconds)

    # Load balance
    load_balancing = exp_config.load_balancing  # local name servers start load balancing among name servers
    load_monitor_interval = exp_config.replication_interval  # interval of monitoring load at every nameserver (seconds)

    # Beehive replication
    is_beehive_replication = exp_config.is_beehive_replication  # Beehive replication
    beehive_base = 16  # Beehive DHT base, default 16
    beehive_leaf_set = 4  # Beehive Leaf set size

    # Experiment duration
    is_experiment_mode = exp_config.is_experiment_mode  # Always set to True to run experiments.
    print "LNS: is_experiment_mode = ", is_experiment_mode
    #restart=False
    is_debug_mode = exp_config.is_debug_mode  # Prints logs if True. Used for testing.

    # retransmission parameters
    max_query_wait_time = exp_config.maxQueryWaitTime  # max wait time before query is declared failed (milli-seconds)
    query_timeout = exp_config.queryTimeout  # query timeout interval
    adaptive_timeout = exp_config.adaptiveTimeout
    delta = exp_config.delta  # Weight assigned to latest sample in calculating moving average.
    mu = exp_config.mu  # Co-efficient of estimated RTT in calculating timeout.
    phi = exp_config.phi  # Co-efficient of deviation in calculating timeout.

    emulate_ping_latencies = exp_config.emulate_ping_latencies
    variation = exp_config.variation

    # logging related parameters:
    ## values: ALL, OFF, INFO, FINE, FINER, FINEST,.. see java documentation.
    file_logging_level = exp_config.lnslog
    console_output_level = exp_config.lnslog
    stat_file_logging_level = exp_config.lnslogstat
    stat_console_output_level = exp_config.lnslogstat

    java_bin = exp_config.remote_java_bin

    check_file(local_name_server_jar)
    check_file(node_config_file)

    command = 'nohup ' + java_bin + '/java -cp ' + local_name_server_jar + ' ' \
              ' -Djava.rmi.server.useCodebaseOnly=false -Djava.rmi.server.codebase=file:' + local_name_server_jar \
              + ' ' + exp_config.lns_main

    command += ' ' + ID + ' ' + str(node_id)
    command += ' ' + NAMESERVER_FILE + ' ' + node_config_file
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

    if workload_config_file is not None and workload_config_file != '':
        command += ' ' + WORKLOAD_FILE + ' ' + workload_config_file

    if update_trace is not None and update_trace != '' and os.path.exists(update_trace):
        command += ' ' + UPDATE_TRACE_FILE + ' ' + update_trace

    command += ' ' + OUTPUT_SAMPLE_RATE + ' ' + str(output_sample_rate)

    command += ' ' + MAX_QUERY_WAIT_TIME + ' ' + str(max_query_wait_time)
    command += ' ' + QUERY_TIMEOUT + ' ' + str(query_timeout)

    if adaptive_timeout:
        command += ' ' + ADAPTIVE_TIMEOUT
        command += ' ' + DELTA + ' ' + str(delta)
        command += ' ' + MU + ' ' + str(mu)
        command += ' ' + PHI + ' ' + str(phi)

    if emulate_ping_latencies:
        command += ' ' + EMULATE_PING_LATENCIES
        command += ' ' + VARIATION + ' ' + str(variation)

    command += ' ' + FILE_LOGGING_LEVEL + ' ' + file_logging_level
    command += ' ' + CONSOLE_OUTPUT_LEVEL + ' ' + console_output_level
    command += ' ' + STAT_FILE_LOGGING_LEVEL + ' ' + stat_file_logging_level
    command += ' ' + STAT_CONSOLE_OUTPUT_LEVEL + ' ' + stat_console_output_level
    if is_experiment_mode:
        command += ' ' + EXPERIMENT_MODE
    if is_debug_mode:
        command += ' ' + DEBUG_MODE
    command += ' 2> log_lns_' + str(node_id)
    command += ' > log_lns_' + str(node_id)
    command += ' &'
    print command
    os.system(command)


def check_file(filename):
    if not os.path.exists(filename):
        print '\n\n\n\n\n'
        os.system('hostname')
        print 'File does not exist:', filename
        print '\n\n\n\n\n'
        fw = open('fileError', 'w')
        fw.write('I QUIT! File does not exist:' + filename)
        fw.close()
        sys.exit(1)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser("local_name_server", "run local name server with options given in config file")
    parser.add_argument("--id", help="node ID of local name server")
    parser.add_argument("--configFile", help="config file to initialize exp_config parameters")
    parser.add_argument("--nsfile", help="file with node config")
    parser.add_argument("--updateTrace", help="Update trace for local name server")
    parser.add_argument("--wfile", help="file with workload config parameters")

    args = parser.parse_args()
    run_local_name_server(args.id, args.configFile, args.nsfile, args.updateTrace, args.wfile)
