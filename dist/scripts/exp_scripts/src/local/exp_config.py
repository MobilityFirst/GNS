import ConfigParser
import sys

# *** MUST set value of gns_folder and mongo_bin_folder correctly.***

#
# Constant values
#
DEFAULT_WORKING_DIR = 'test_output'
DEFAULT_GNS_OUTPUT_FOLDER = 'log'
DEFAULT_STATS_FOLDER = 'log_stats'

#types of latency emulation
CONSTANT_DELAY = 'constant_delay'
GEOGRAPHIC_DELAY = 'geo_delay'

DEFAULT_CONST_DELAY = 100


#
#
# config parameters


gns_folder = '/Users/abhigyan/Documents/workspace/GNS/'

mongo_bin_folder = '/opt/local/bin'

mongo_port = 31243

# gns jar
gnrs_jar = '/Users/abhigyan/Documents/workspace/GNS/dist/GNS.jar'

# top level folder
#working_dir = gns_folder + '/' + DEFAULT_WORKING_DIR  # location of top-level folder checked out from SVN.

# output folder: GNS logs for name servers, and local name servers are stored in this folder 
output_folder = None

# paxos log folder
paxos_log_folder = None  # folder where paxos logs are stored

# config file storing list of nodes, port numbers, etc.
node_config = None

# trace folder: lookup and update requests sent by LNS are stored in this folder
trace_folder = None

# mongodb data folder
mongodb_data_folder = None


# parameters used only by scripts for running experiments
experiment_run_time = -1    # duration of experiment (seconds)

clean_start = True   # if True, we delete all previous state and start a fresh GNS instance

ns_sleep = 10      # after starting name servers, wait for ns_sleep seconds before starting local name servers.
extra_wait = 10   # extra wait time after LNS sends all requests

random_node_ids = None  # option to select nodeIDs randomly. If random_node_ids is not None, it takes an int value.
                        # we select node IDs taking the given int as seed of random number generator

failed_nodes = None   # NOT used

num_ns = 3    # must be more than 3
num_lns = 1  # must be set to 1

#
#
# parameters for workload generator
wfile = None

gen_workload = False   # if True, generate new workload,

#
#
#
# logging options
is_debug_mode = True   #

nslog = 'FINE'       # Set to  FINER for more verbose output; INFO or SEVERE for less verbose output
nslogstat = 'FINE'  # Set to  FINER for more verbose output; INFO or SEVERE for less verbose output
lnslog = 'FINE'    # Set to  FINER for more verbose output; INFO or SEVERE for less verbose output
lnslogstat = 'FINE'  # Always set to 'FINE'


#
#
# GNS parameters common to name server / local name servers
is_experiment_mode = False  # set to True to run experiments, false otherwise.

primary_name_server = 3  # number of primary name servers

no_paxos_log = False
dummy_gns = False

scheme = 'locality'         # 'locality' is for auspice
schemes = {'beehive': 0, 'locality': 1, 'uniform': 2, 'static3': 3, 'replicate_all': 4}

#
#
# name server parameters
replication_interval = 10000   # interval (in sec) at which group changes are done.
failure_detection_msg_interval = 10
failure_detection_timeout_interval = 30
multipaxos = True

#
#
# local name server parameters
queryTimeout = 2000   # ms    # timeout value for a query (lookup/update)
maxQueryWaitTime = 10000  # ms  #  maximum wait time after which a query is declared failed

#
#
# local name server parameters
emulate_ping_latencies = False
variation = 0.1
emulation_type = CONSTANT_DELAY
const_latency_value = DEFAULT_CONST_DELAY

load_balancing = False  # Redirect to closest name server based on (RTT + server-load)

is_beehive_replication = False  # do not change
is_location_replication = False  # do not change
is_random_replication = False  # do not change
is_static_replication = False  # do not change

if scheme not in schemes:
    print 'ERROR: Scheme name not valid:', scheme, 'Valid scheme names:', schemes.keys()
    sys.exit(2)

if scheme == 'beehive':
    is_beehive_replication = True
    load_balancing = False
elif scheme == 'locality':
    is_location_replication = True
elif scheme == 'uniform':
    is_random_replication = True
elif scheme == 'static3':
    is_static_replication = True
elif scheme == 'replicate_all':
    is_static_replication = True
    primary_name_server = num_ns


def initialize(filename):
    """Initializes the parameters above based on given config file"""

    parser = ConfigParser.ConfigParser()
    parser.read(filename)
    initialize_path_locations(parser)
    initialize_emulation_parameters(parser)

    import os
    os.system('cat ' + filename)
    if parser.has_option(ConfigParser.DEFAULTSECT, 'clean_start'):
        global clean_start
        clean_start = parser.getboolean(ConfigParser.DEFAULTSECT, 'clean_start')

    #
    # Parameters related to experiment setup
    #
    if parser.has_option(ConfigParser.DEFAULTSECT, 'experiment_run_time'):
        global experiment_run_time
        experiment_run_time = float(parser.get(ConfigParser.DEFAULTSECT, 'experiment_run_time'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'num_ns'):
        global num_ns
        num_ns = int(parser.get(ConfigParser.DEFAULTSECT, 'num_ns'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'num_lns'):
        global num_lns
        num_lns = int(parser.get(ConfigParser.DEFAULTSECT, 'num_lns'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'failed_nodes'):  # multiple failed nodes are concatenated by ':'
        global failed_nodes
        failed_nodes = parser.get(ConfigParser.DEFAULTSECT, 'failed_nodes').split(':')
        failed_nodes = [int(x) for x in failed_nodes]

    if parser.has_option(ConfigParser.DEFAULTSECT, 'ns_sleep'):
        global ns_sleep
        ns_sleep = int(parser.get(ConfigParser.DEFAULTSECT, 'ns_sleep'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'random_node_ids'):
        global random_node_ids
        random_node_ids = int(parser.get(ConfigParser.DEFAULTSECT, 'random_node_ids'))

    #
    # GNS-specific parameters
    #
    if parser.has_option(ConfigParser.DEFAULTSECT, 'primary_name_server'):
        global primary_name_server
        primary_name_server = int(parser.get(ConfigParser.DEFAULTSECT, 'primary_name_server'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'is_experiment_mode'):
        global is_experiment_mode
        is_experiment_mode = bool(parser.get(ConfigParser.DEFAULTSECT, 'is_experiment_mode'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'is_debug_mode'):
        global is_debug_mode
        is_debug_mode = bool(parser.get(ConfigParser.DEFAULTSECT, 'is_debug_mode'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'wfile'):
        global wfile
        wfile = parser.get(ConfigParser.DEFAULTSECT, 'wfile')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'replication_interval'):
        global replication_interval
        replication_interval = parser.getint(ConfigParser.DEFAULTSECT, 'replication_interval')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'failure_detection_msg_interval'):
        global failure_detection_msg_interval
        failure_detection_msg_interval = int(parser.get(ConfigParser.DEFAULTSECT, 'failure_detection_msg_interval'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'failure_detection_timeout_interval'):
        global failure_detection_timeout_interval
        failure_detection_timeout_interval = int(parser.get(ConfigParser.DEFAULTSECT,
                                                            'failure_detection_timeout_interval'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'use_gns_nio_transport'):
        global use_gns_nio_transport
        use_gns_nio_transport = bool(parser.get(ConfigParser.DEFAULTSECT, 'use_gns_nio_transport'))


def initialize_path_locations(parser):
    """Initializes locations of various folders:
    1. GNS jar file
    2. GNS debug logs
    3. Node config file
    4. Request trace folder
    5. Paxos logs
    6. mongoDB data folder
    7. mongoDB bin folder
    """
    global gns_folder, gnrs_jar, mongo_bin_folder, output_folder, paxos_log_folder, trace_folder,\
        mongodb_data_folder  # working_dir,

    if parser.has_option(ConfigParser.DEFAULTSECT, 'mongo_bin_folder'):
        mongo_bin_folder = parser.get(ConfigParser.DEFAULTSECT, 'mongo_bin_folder')

    # if gns folder is given, we initialize gns_jar location and other variables with respect to gns folder.
    if parser.has_option(ConfigParser.DEFAULTSECT, 'gns_folder'):
        gns_folder = parser.get(ConfigParser.DEFAULTSECT, 'gns_folder')
        gnrs_jar = gns_folder + '/dist/GNS.jar'
        working_dir = gns_folder + '/' + DEFAULT_WORKING_DIR
        update_path_locations(working_dir)

    # if working dir is given, we initialize all variables with respect to working dir.
    if parser.has_option(ConfigParser.DEFAULTSECT, 'working_dir'):
        working_dir = parser.get(ConfigParser.DEFAULTSECT, 'working_dir')
        update_path_locations(working_dir)

    # if locations for one of the remaining variables is specified, it overrides the earlier initialization.

    if parser.has_option(ConfigParser.DEFAULTSECT, 'output_folder'):
        output_folder = parser.get(ConfigParser.DEFAULTSECT, 'output_folder')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'paxos_log_folder'):
        paxos_log_folder = parser.get(ConfigParser.DEFAULTSECT, 'paxos_log_folder')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'trace_folder'):
        trace_folder = parser.get(ConfigParser.DEFAULTSECT, 'trace_folder')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'mongodb_data_folder'):
        mongodb_data_folder = parser.get(ConfigParser.DEFAULTSECT, 'mongodb_data_folder')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'jar_file'):
        print 'GNRS jar file is at this location:', gnrs_jar
        gnrs_jar = parser.get(ConfigParser.DEFAULTSECT, 'jar_file')


def update_path_locations(working_dir1):
    """Updates locations of all files relative to working dir."""

    # output folder: GNS logs for name servers, and local name servers are stored in this folder
    global output_folder
    output_folder = working_dir1

    # paxos log folder
    global paxos_log_folder
    paxos_log_folder = working_dir1 + '/paxoslog/'  # folder where paxos logs are stored

    # config file storing list of nodes, port numbers, etc.
    global node_config
    node_config = working_dir1 + '/local_config'

    # trace folder: lookup and update requests sent by LNS are stored in this folder
    global trace_folder
    trace_folder = working_dir1 + '/trace/'

    # trace folder: lookup and update requests sent by LNS are stored in this folder
    global mongodb_data_folder
    mongodb_data_folder = working_dir1 + '/mongodb/'


def initialize_emulation_parameters(parser):
    if parser.has_option(ConfigParser.DEFAULTSECT, 'emulate_ping_latencies'):
        global emulate_ping_latencies
        emulate_ping_latencies = bool(parser.get(ConfigParser.DEFAULTSECT, 'emulate_ping_latencies'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'emulation_type'):
        global emulation_type
        emulation_type = parser.get(ConfigParser.DEFAULTSECT, 'emulation_type')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'const_latency_value'):
        global const_latency_value
        const_latency_value = parser.getint(ConfigParser.DEFAULTSECT, 'const_latency_value')
