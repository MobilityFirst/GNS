import os, sys

############### UPDATE VALUES BELOW ####################

# values of these variables are pathnames on the machine where the script is running
output_folder = '/home/abhigyan/gnrs/results/jan30/gns_output/'  # path where output from experiment will be stored
ns_file = '/home/abhigyan/gnrs/planetlab/nodes/pl_ns'  # file with list of name servers (one per line)
lns_file = '/home/abhigyan/gnrs/planetlab/nodes/pl_lns'  # file with list of local name servers (one per line)

user = 'umass_nameservice'  # user name to log in to every machine
ssh_key = '/home/abhigyan/.ssh/id_rsa'  # ssh key used for logging into remote machine
jar_file = '/home/abhigyan/gnrs/GNS.jar'  # path of the GNS jar on local machine,

# values of these variables are pathnames on the remote machine
java_bin = '/home/ec2-user/jdk/bin/'   # java binaries are located at this folder on remote machine
mongo_bin = '/home/ec2-user/mongodb/bin/'  # mongodb binaries are located at this folder on remote machine
jar_file_remote = '/home/ec2-user/GNS.jar'  # path name on remote machine where jar will be stored
gns_output_logs = '/home/ec2-user/gnslogs/'  # remote folder where gns output will be stored
paxos_log_folder = '/home/ec2-user/paxos_log/' # remote folder where paxos logs will be stored
run_db = True        # either True/False. if 'True', running mongodb instances on remote machine are killed, new instances are run, and all previous data is deleted from disk.
                      # if False,  already running mongo instances are used. Database state with same
db_folder = '/home/ec2-user/gnsdb/'  #  remote folder where mongodb will store its logs

##########################################################

# Dont need to change values below to run a test experiment

primary_name_server = 3  # Number of primary name servers. Must be less than number of name servers.
                         # To run with static placement, set primary_name_server to the number of replicas per name.
                         # and set replication_interval to a value more than the duration of the experiment.

replication_interval = 100000   ## (in seconds). Intervals at which auspice compute new set of active replicas.


ns_geo_file = 'abcd'
lns_geo_file = 'abcd'    ### needed for generating workload


restore_db = False # if true, backup state from DB
db_folder_backup = '/media/ephemeral0/gnsdb-backup/'  #  remote folder where mongodb will store its logs
no_load_db = False    # must set to true, if records already loaded in DB,


mongo_sleep = 60
ns_sleep = 20
experiment_run_time = 60  # duration for which requests are sent
extra_wait = 30

failed_nodes = None

scheme = 'locality'
schemes = {'beehive': 0, 'locality': 1, 'uniform': 2, 'static3': 3, 'replicate_all': 4}

debug_mode = False

is_experiment_mode = True

emulate_ping_latencies = True

variation = 0.10

copy_jar = True # if true, copy jar to remote machines from given location
download_jar = False   # if true, download jar from S3


hosts_ns_file = 'pl_ns'
remote_cpu_folder = '/media/ephemeral0/gnslogs/cpuUsageFolder'
local_cpu_folder = os.path.join(output_folder,'cpuUsageFolder')

############## Workload generator parameters #################

gen_workload = 'test'   # write 'test' to generate test workload with random values at each node, write 'locality' to generate locality-based workload

# folder where workload is generated
lookupTrace = os.path.join(output_folder, 'workload/lookupTrace') # '/home/abhigyan/gnrs/ec2_data/workload/lookupTrace/'
updateTrace = os.path.join(output_folder, 'workload/updateTrace') #'/home/abhigyan/gnrs/ec2_data/workload/updateTrace/'
other_data = os.path.join(output_folder, 'workload/otherData')#  data to generate placement, e.g., read rate, write rate, etc. are output in this folder.
#'/home/abhigyan/gnrs/ec2_data/workload/otherData/'

# workload parameters
regular_workload = 0  # number of regular/service names in the workload
mobile_workload = 1000  # number of mobile device names in the workload

lookup_count = 10000 # approx mobile lookups (for mobile workload generation)
update_count = 10000 # approx mobile updates (for mobile workload generation)
lookup_count_regular = 0 # fixed (for regular workload, number of lookups in lookup_regular_folder)

# folder where config files for each node are generated: this is not necessary anymore
#config_folder = os.path.join(output_folder, 'configFolder') #'
gen_config = True
config_folder = '/home/abhigyan/gnrs/ec2_data/ec2_config/'


update_trace_url = ''  #'https://s3.amazonaws.com/update100m/lookup_'
lookup_trace_url = ''  #'https://s3.amazonaws.com/lookup100m/update_'

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
lnslog = 'WARNING'
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


##################### PLANETLAB #########################

#pl_slice = 'umass_bittorrent'
#mongo_bin_path = '/home/umass_bittorrent/mongodb/bin/'
#mongo_data_path = '/home/umass_bittorrent/gnrs-db-mongodb/'


if __name__ == "__main__":
    #load = float(sys.argv[1])
    #alpha = get_alpha_based_on_load(load)
    #print 'load = ', load, '\talpha = ', alpha,'\tnormconst',1.0/alpha
    pass
