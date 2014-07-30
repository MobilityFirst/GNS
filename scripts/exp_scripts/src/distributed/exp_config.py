import ConfigParser
import os

DEFAULT_STATS_FOLDER = 'log_stats'

# values of these variables are pathnames on the machine where the script is running
local_output_folder = '/home/abhigyan/gnrs/results/jan30/gns_output/'  # local folder where output is stored
local_ns_file = '/home/abhigyan/gnrs/planetlab/nodes/pl_ns'  # file with list of name servers (one per line)
local_lns_file = '/home/abhigyan/gnrs/planetlab/nodes/pl_lns'  # file with list of local name servers (one per line)
local_jar_file = '/home/abhigyan/gnrs/GNS.jar'  # path of the GNS jar on local machine,

user = 'umass_nameservice'  # user name to log in to every machine
ssh_key = '/home/abhigyan/.ssh/id_rsa'  # ssh key used for logging into remote machine

# values of these variables are pathnames on the remote machine
remote_java_bin = '/home/ec2-user/jdk/bin/'   # java binaries are located at this folder on remote machine
remote_mongo_bin = '/home/ec2-user/mongodb/bin/'  # mongodb binaries are located at this folder on remote machine

remote_folder = '/home/ec2-user/'   # top level remote folder where all GNS related data is stored
remote_jar_file = '/home/ec2-user/GNS.jar'  # path name on remote machine where jar will be stored
remote_gns_logs = '/home/ec2-user/gnslogs/'  # remote folder where gns output will be stored
paxos_log_folder = '/home/ec2-user/paxos_log/'  # remote folder where paxos logs will be stored

db_folder = '/home/ec2-user/gnsdb/'  # remote folder where mongodb will store its logs

mongo_sleep = 20
ns_sleep = 20
experiment_run_time = 10  # duration for which requests are sent
extra_wait = 11

failed_nodes = None

# if True, all database and paxos state is cleared before running an experiment
clean_start = True

copy_jar = True  # if true, copy jar to remote machines from given location

download_jar = False   # if true, download jar from S3

hosts_ns_file = 'pl_ns'

############## Workload generator parameters #################

# folder containing node config file for each node.
node_config_folder = '/home/abhigyan/gnrs/ec2_data/ec2_config/'
gen_node_config = False

# Folder where workload is generated
update_trace = ''

# Options below are not supported currently.

ns_geo_file = 'abcd'
lns_geo_file = 'abcd'    # needed for generating workload

other_data = os.path.join(local_output_folder, 'workload/otherData')   # data to generate placement, e.g., read rate,
                                                                 # write rate, etc. are output in this folder.

gen_workload = 'test'   # write 'test' to generate test workload with random values at each node, write 'locality' to
                        # generate locality-based workload
# workload parameters
regular_workload = 0  # number of regular/service names in the workload
mobile_workload = 1000  # number of mobile device names in the workload

lookup_count = 10000  # approx mobile lookups (for mobile workload generation)
update_count = 10000  # approx mobile updates (for mobile workload generation)
lookup_count_regular = 0  # fixed (for regular workload, number of lookups in lookup_regular_folder)

update_trace_url = ''  # 'https://s3.amazonaws.com/update100m/lookup_'
lookup_trace_url = ''  # 'https://s3.amazonaws.com/lookup100m/update_'

load = 1  # used for cluster to generate workload

#  Data collected from planetlab used for generating workload    
pl_latency_folder = '/home/abhigyan/gnrs/ec2_data/pl_data_new/ping_new_local/'
lookup_regular_folder = '/home/abhigyan/gnrs/ec2_data/pl_data_new/lookupTraceRegular_1M/'
pl_lns_workload = '/home/abhigyan/gnrs/ec2_data/pl_data_new/nodes/pl_lns'
pl_lns_geo_workload = '/home/abhigyan/gnrs/ec2_data/pl_data_new/nodes/pl_lns_geo'

# other options

output_sample_rate = 1.0    # fraction of requests that are logged


##################### LOGGING #########################

# if True, log output is more verbose.
is_debug_mode = True

nslog = 'FINE'
nslogstat = 'FINE'  # records write propagation times
lnslog = 'FINE'
lnslogstat = 'FINE'


################ Common NS/LNS parameters #############

primary_name_server = 3  # Number of primary name servers. Must be less than number of name servers.
                         # To run with static placement, set primary_name_server to the number of replicas per name.
                         # and set replication_interval to a value more than the duration of the experiment.

replication_interval = 100000   # (in seconds). Intervals at which name servers compute new set of active replicas.
                                # and local name servers sends votes to name servers

# if True, local name servers starts sending requests as per given workload
is_experiment_mode = False

# if true, we emulate wide-area latency between packets sent between nodes
emulate_ping_latencies = False

# variation in latency emulation
variation = 0.8

event_file = None  # file with list of events at nodes, e.g., failure, restart, node addition, node removal

scheme = 'locality'

## do not edit these variables ## choose scheme using variable 'scheme'
is_beehive_replication = False
is_location_replication = False
is_random_replication = False
is_static_replication = False

################# LNS parameters ######################

wfile = None   # contains workload parameters (simulated by local name servers)

load_balancing = False

maxQueryWaitTime = 10100
queryTimeout = 2000           # query timeout interval
adaptiveTimeout = False
delta = 0.05
mu = 1.5
phi = 4.0

lns_main = 'edu.umass.cs.gns.main.StartLocalNameServer'

cache_size = 10000

#
################## NS parameters ######################

mongo_port = 28123

normalizing_constant = 1.0

name_server_selection_vote_size = 5

min_replica = 3
max_replica = 100

c_hop = 0.18
alpha = 0.63

worker_thread_count = 10

ns_main = 'edu.umass.cs.gns.main.StartNameServer'

failure_detection_msg_interval = 10
failure_detection_timeout_interval = 28

read_coordination = False

no_paxos_log = False

dummy_gns = False

max_req_rate = 300

multipaxos = True

#### methods for parsing options in config file

def initialize(filename):
    """ Initializes the parameters above based on given config file"""
    os.system('cat ' + filename)

    parser = ConfigParser.ConfigParser()
    parser.read(filename)
    initialize_env_variables(parser)
    initialize_gns_parameters(parser)

    if parser.has_option(ConfigParser.DEFAULTSECT, 'experiment_run_time'):
        global experiment_run_time
        experiment_run_time = parser.getfloat(ConfigParser.DEFAULTSECT, 'experiment_run_time')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'ns_sleep'):
        global ns_sleep
        ns_sleep = int(parser.get(ConfigParser.DEFAULTSECT, 'ns_sleep'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'extra_wait'):
        global extra_wait
        extra_wait = int(parser.get(ConfigParser.DEFAULTSECT, 'extra_wait'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'clean_start'):
        global clean_start
        clean_start = parser.getboolean(ConfigParser.DEFAULTSECT, 'clean_start')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'lookup_trace'):
        global lookup_trace
        lookup_trace = parser.get(ConfigParser.DEFAULTSECT, 'lookup_trace')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'update_trace'):
        global update_trace
        update_trace = parser.get(ConfigParser.DEFAULTSECT, 'update_trace')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'config_folder'):
        global node_config_folder
        node_config_folder = parser.get(ConfigParser.DEFAULTSECT, 'config_folder')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'event_file'):
        global event_file
        event_file = parser.get(ConfigParser.DEFAULTSECT, 'event_file')

    # this must be after 'primary_name_server' parameter is parsed
    initialize_replication()


def initialize_gns_parameters(parser):

    if parser.has_option(ConfigParser.DEFAULTSECT, 'primary_name_server'):
        global primary_name_server
        primary_name_server = parser.getint(ConfigParser.DEFAULTSECT, 'primary_name_server')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'scheme'):
        global scheme
        scheme = parser.get(ConfigParser.DEFAULTSECT, 'scheme')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'is_experiment_mode'):
        global is_experiment_mode
        print '\nExperiment mode:', is_experiment_mode
        is_experiment_mode = bool(parser.get(ConfigParser.DEFAULTSECT, 'is_experiment_mode'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'is_debug_mode'):
        global is_debug_mode
        is_debug_mode = bool(parser.get(ConfigParser.DEFAULTSECT, 'is_debug_mode'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'wfile'):
        global wfile
        wfile = parser.get(ConfigParser.DEFAULTSECT, 'wfile')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'read_coordination'):
        global read_coordination
        read_coordination = parser.getboolean(ConfigParser.DEFAULTSECT, 'read_coordination')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'no_paxos_log'):
        global no_paxos_log
        no_paxos_log = parser.getboolean(ConfigParser.DEFAULTSECT, 'no_paxos_log')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'replication_interval'):
        global replication_interval
        replication_interval = parser.getint(ConfigParser.DEFAULTSECT, 'replication_interval')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'emulate_ping_latencies'):
        global emulate_ping_latencies
        emulate_ping_latencies = bool(parser.get(ConfigParser.DEFAULTSECT, 'emulate_ping_latencies'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'min_replica'):
        global min_replica
        min_replica = int(parser.get(ConfigParser.DEFAULTSECT, 'min_replica'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'max_replica'):
        global max_replica
        max_replica = int(parser.get(ConfigParser.DEFAULTSECT, 'max_replica'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'max_req_rate'):
        global max_req_rate
        max_req_rate = float(parser.get(ConfigParser.DEFAULTSECT, 'max_req_rate'))


def initialize_replication():
    """ Initializes type of replication used by GNS"""

    schemes = ['beehive', 'locality', 'uniform', 'static', 'replicate_all']

    assert scheme in schemes

    global is_location_replication, is_random_replication, is_static_replication, is_beehive_replication
    if scheme == 'locality':
        is_location_replication = True
    elif scheme == 'uniform':
        is_random_replication = True
    elif scheme == 'static':
        is_static_replication = True
    elif scheme == 'replicate_all':
        assert False
        is_static_replication = True
        primary_name_server = num_ns
    elif scheme == 'beehive':
        assert False
        is_beehive_replication = True
        load_balancing = False


def initialize_env_variables(parser):
    """ Initializes variables related to the environment in which test is run"""

    if parser.has_option(ConfigParser.DEFAULTSECT, 'local_output_folder'):
        global local_output_folder
        local_output_folder = parser.get(ConfigParser.DEFAULTSECT, 'local_output_folder')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'ns_file'):
        global local_ns_file
        local_ns_file = parser.get(ConfigParser.DEFAULTSECT, 'ns_file')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'lns_file'):
        global local_lns_file
        local_lns_file = parser.get(ConfigParser.DEFAULTSECT, 'lns_file')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'jar_file'):
        global local_jar_file
        local_jar_file = parser.get(ConfigParser.DEFAULTSECT, 'jar_file')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'user'):
        global user
        user = parser.get(ConfigParser.DEFAULTSECT, 'user')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'ssh_key'):
        global ssh_key
        ssh_key = parser.get(ConfigParser.DEFAULTSECT, 'ssh_key')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'remote_folder'):
        global remote_folder, remote_jar_file, remote_gns_logs, paxos_log_folder, db_folder
        remote_folder = parser.get(ConfigParser.DEFAULTSECT, 'remote_folder')
        remote_jar_file = os.path.join(remote_folder, 'GNS.jar')  # path name on remote machine where jar will be stored
        remote_gns_logs = os.path.join(remote_folder, 'gnslogs')  # remote folder where gns output will be stored
        paxos_log_folder = os.path.join(remote_folder, 'paxos_log')  # remote folder where paxos logs will be stored
        db_folder = os.path.join(remote_folder, 'gnsdb')  # remote folder where mongodb will store its logs

    if parser.has_option(ConfigParser.DEFAULTSECT, 'java_bin'):
        global remote_java_bin
        remote_java_bin = parser.get(ConfigParser.DEFAULTSECT, 'java_bin')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'mongo_bin'):
        global remote_mongo_bin
        remote_mongo_bin = parser.get(ConfigParser.DEFAULTSECT, 'mongo_bin')


