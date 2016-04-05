import ConfigParser
import os, sys


DEFAULT_STATS_FOLDER = 'log_stats'


# values of these variables are pathnames on the machine where the script is running
local_output_folder = '/home/abhigyan/gnrs/results/jan30/gns_output/'  # path where output from experiment will be stored
local_ns_file = '/home/abhigyan/gnrs/planetlab/nodes/pl_ns'  # file with list of name servers (one per line)
local_lns_file = '/home/abhigyan/gnrs/planetlab/nodes/pl_lns'  # file with list of local name servers (one per line)

user = 'umass_nameservice'  # user name to log in to every machine
ssh_key = '/home/abhigyan/.ssh/id_rsa'  # ssh key used for logging into remote machine
local_jar_file = '/home/abhigyan/gnrs/GNS.jar'  # path of the GNS jar on local machine,

# values of these variables are pathnames on the remote machine
remote_java_bin = '/home/ec2-user/jdk/bin/'   # java binaries are located at this folder on remote machine
remote_mongo_bin = '/home/ec2-user/mongodb/bin/'  # mongodb binaries are located at this folder on remote machine

remote_jar_file = '/home/ec2-user/GNS.jar'  # path name on remote machine where jar will be stored
remote_gns_logs = '/home/ec2-user/gnslogs/'  # remote folder where gns output will be stored
paxos_log_folder = '/home/ec2-user/paxos_log/' # remote folder where paxos logs will be stored

db_folder = '/home/ec2-user/gnsdb/'  #  remote folder where mongodb will store its logs







primary_name_server = 3  # Number of primary name servers. Must be less than number of name servers.
                         # To run with static placement, set primary_name_server to the number of replicas per name.
                         # and set replication_interval to a value more than the duration of the experiment.

replication_interval = 100000   # (in seconds). Intervals at which auspice compute new set of active replicas.


wfile = None

ns_geo_file = 'abcd'
lns_geo_file = 'abcd'    # needed for generating workload


mongo_sleep = 60
ns_sleep = 20
experiment_run_time = 60  # duration for which requests are sent
extra_wait = 30

failed_nodes = None

scheme = 'locality'
schemes = {'beehive': 0, 'locality': 1, 'uniform': 2, 'static3': 3, 'replicate_all': 4}

# if True, all database and paxos state is cleared before running an experiment
clean_start = True

# if True, more detailed log messages are printed
is_debug_mode = True

# if True, local name servers starts sending requests as per given workload
is_experiment_mode = True

# if true, we emulate wide-area latency between packets sent between nodes
emulate_ping_latencies = True

# variation in latency emulation
variation = 0.10


copy_jar = True  # if true, copy jar to remote machines from given location
download_jar = False   # if true, download jar from S3


hosts_ns_file = 'pl_ns'

############## Workload generator parameters #################


def get_lookup_folder(output_folder):
    return os.path.join(output_folder, 'workload/lookupTrace')


def get_update_folder(output_folder):
    return os.path.join(output_folder, 'workload/updateTrace')

# folder where workload is generated
lookup_trace = get_lookup_folder(local_output_folder) # '/home/abhigyan/gnrs/ec2_data/workload/lookupTrace/'
update_trace = get_update_folder(local_output_folder) #'/home/abhigyan/gnrs/ec2_data/workload/updateTrace/'
other_data = os.path.join(local_output_folder, 'workload/otherData') #  data to generate placement, e.g., read rate, write rate, etc. are output in this folder.
#'/home/abhigyan/gnrs/ec2_data/workload/otherData/'

gen_workload = 'test'   # write 'test' to generate test workload with random values at each node, write 'locality' to
                        # generate locality-based workload

# workload parameters
regular_workload = 0  # number of regular/service names in the workload
mobile_workload = 1000  # number of mobile device names in the workload

lookup_count = 10000 # approx mobile lookups (for mobile workload generation)
update_count = 10000 # approx mobile updates (for mobile workload generation)
lookup_count_regular = 0 # fixed (for regular workload, number of lookups in lookup_regular_folder)

# folder where config files for each node are generated: this is not necessary anymore
#config_folder = os.path.join(output_folder, 'configFolder') #'
gen_config = True
node_config_folder = '/home/abhigyan/gnrs/ec2_data/ec2_config/'


update_trace_url = ''  # 'https://s3.amazonaws.com/update100m/lookup_'
lookup_trace_url = ''  # 'https://s3.amazonaws.com/lookup100m/update_'

load = 1  # used for cluster to generate workload
loads = [1]

# Data collected from planetlab used for generating workload
#pl_latency_folder = '/home/abhigyan/gnrs/ec2_data/pl_data/pl_latency/'
#lookup_regular_folder = '/home/abhigyan/gnrs/ec2_data/pl_data/lookupTraceRegular/'
#pl_lns_workload = '/home/abhigyan/gnrs/ec2_data/pl_data/pl_lns_geo'
#pl_lns_geo_workload = '/home/abhigyan/gnrs/ec2_data/pl_data/pl_lns_geo'

#  Data collected from planetlab used for generating workload    
pl_latency_folder = '/home/abhigyan/gnrs/ec2_data/pl_data_new/ping_new_local/'
lookup_regular_folder = '/home/abhigyan/gnrs/ec2_data/pl_data_new/lookupTraceRegular_1M/'
pl_lns_workload = '/home/abhigyan/gnrs/ec2_data/pl_data_new/nodes/pl_lns'
pl_lns_geo_workload = '/home/abhigyan/gnrs/ec2_data/pl_data_new/nodes/pl_lns_geo'

# other options
reducequeryratefactor = 1.0

output_sample_rate = 1.0    # fraction of requests that are logged

download_name_actives = False
name_actives_remote = '' # location of remote name actives file
name_actives_url = '' ##** match with name of name_actives_remote
name_actives_local = ''

max_log_name = int(output_sample_rate * (regular_workload + mobile_workload))

################# LNS parameters ######################

run_http_server = False
load_balancing = False

numberOfTransmissions = 3
maxQueryWaitTime = 12100
queryTimeout = 2000           # query timeout interval
adaptiveTimeout = False
delta = 0.05
mu = 1.5
phi = 4.0

lns_main = 'edu.umass.cs.gns.main.StartLocalNameServer'

cache_size = 1000000

################## NS parameters ######################

normalizing_constant = 1  # this value is used. set in name-server.py

name_server_selection_vote_size = 5

eventual_consistency = False

#paxos_log_folder = 'paxos_log/'

min_replica = 3
max_replica = 100

c_hop = 0.18
alpha = 0.63

worker_thread_count = 10

#persistent_data_store = True
#signature_check = False
#simple_disk_store = False

ns_main = 'edu.umass.cs.gns.main.StartNameServer'

failure_detection_msg_interval = 10
failure_detection_timeout_interval = 28

quit_after_time = -1
quit_node_id = -1

##################### LOGGING #########################

nslog = 'WARNING'
nslogstat = 'FINE'  # records write propagation times
lnslog = 'FINE'
lnslogstat = 'FINE'

##################### SCHEMES #########################

if scheme not in schemes:
    print 'ERROR: Scheme name not valid:', scheme, 'Valid scheme names:', schemes.keys()
    sys.exit(2)

## do not edit these variables ## choose scheme using variable 'scheme'
is_beehive_replication = False
is_location_replication = False
is_random_replication = False
is_static_replication = False


if scheme == 'locality':
    is_location_replication = True
elif scheme == 'uniform':
    is_random_replication = True
elif scheme == 'static3':
    is_static_replication = True
elif scheme == 'replicate_all':
    assert False
    is_static_replication = True
    primary_name_server = num_ns
elif scheme == 'beehive':
    assert False
    is_beehive_replication = True
    load_balancing = False


def initialize(filename):
    """ Initializes the parameters above based on given config file"""
    parser = ConfigParser.ConfigParser()
    parser.read(filename)

    os.system('cat ' + filename)

    initialize_env_variables(parser)

    initialize_gns_parameters(parser)

    if parser.has_option(ConfigParser.DEFAULTSECT, 'clean_start'):
        global clean_start
        clean_start = parser.getboolean(ConfigParser.DEFAULTSECT, 'clean_start')


def initialize_gns_parameters(parser):

    if parser.has_option(ConfigParser.DEFAULTSECT, 'is_experiment_mode'):
        global is_experiment_mode
        is_experiment_mode = bool(parser.get(ConfigParser.DEFAULTSECT, 'is_experiment_mode'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'is_debug_mode'):
        global is_debug_mode
        is_debug_mode = bool(parser.get(ConfigParser.DEFAULTSECT, 'is_debug_mode'))

    if parser.has_option(ConfigParser.DEFAULTSECT, 'wfile'):
        global wfile
        wfile = parser.get(ConfigParser.DEFAULTSECT, 'wfile')


def initialize_env_variables(parser):
    if parser.has_option(ConfigParser.DEFAULTSECT, 'local_output_folder'):
        global local_output_folder
        output_folder = parser.get(ConfigParser.DEFAULTSECT, 'local_output_folder')
        print 'Local output folder '

    if parser.has_option(ConfigParser.DEFAULTSECT, 'ns_file'):
        global local_ns_file
        ns_file = parser.get(ConfigParser.DEFAULTSECT, 'ns_file')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'lns_file'):
        global local_lns_file
        lns_file = parser.get(ConfigParser.DEFAULTSECT, 'lns_file')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'jar_file'):
        global local_jar_file
        jar_file = parser.get(ConfigParser.DEFAULTSECT, 'jar_file')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'user'):
        global user
        user = parser.get(ConfigParser.DEFAULTSECT, 'user')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'ssh_key'):
        global ssh_key
        ssh_key = parser.get(ConfigParser.DEFAULTSECT, 'ssh_key')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'remote_folder'):
        global remote_folder, remote_jar_file, remote_gns_logs, paxos_log_folder, db_folder
        remote_folder = parser.get(ConfigParser.DEFAULTSECT, 'remote_folder')
        jar_file_remote = os.path.join(remote_folder, 'GNS.jar')  # path name on remote machine where jar will be stored
        gns_output_logs = os.path.join(remote_folder, 'gnslogs')  # remote folder where gns output will be stored
        paxos_log_folder = os.path.join(remote_folder, 'paxos_log')  # remote folder where paxos logs will be stored
        db_folder = os.path.join(remote_folder, 'gnsdb')  # remote folder where mongodb will store its logs

    if parser.has_option(ConfigParser.DEFAULTSECT, 'java_bin'):
        global remote_java_bin
        java_bin = parser.get(ConfigParser.DEFAULTSECT, 'java_bin')

    if parser.has_option(ConfigParser.DEFAULTSECT, 'mongo_bin'):
        global remote_mongo_bin
        mongo_bin = parser.get(ConfigParser.DEFAULTSECT, 'mongo_bin')


