import os
import sys

# *** MUST set values of 'gnrs_jar' and 'working_dir' ***

# gns jar
gnrs_jar = '/Users/abhigyan/Documents/workspace/GNS-latest2/dist/GNS.jar'

# top level folder
working_dir = '/Users/abhigyan/Documents/gns_output/'  # location of top-level folder checked out from SVN.

# output folder: GNS logs for name servers, and local name servers are stored in this folder 
output_folder = working_dir + '/local/log_local/' 

# paxos log folder
paxos_log_folder = working_dir + '/local/paxoslog/'  # folder where paxos logs are stored

# config file storing list of nodes, port numbers, etc.
config_file = working_dir + '/local/local_config'

# trace folder: lookup and update requests sent by LNS are stored in this folder
trace_folder = working_dir + '/local/trace/'


# parameters used only by scripts for running experiments
experiment_run_time = 1    # duration of experiment (seconds)

delete_paxos_log = True   # if True, delete all paxos logs before starting a new experiment. if False, recover data from logs.

start_db = False   #

ns_sleep = 5      # after starting name servers, wait for ns_sleep seconds before starting local name servers.
extra_wait = 5   # extra wait time after LNS sends all requests

failed_nodes = None   # NOT used

num_ns = 3    # must be more than 3
num_lns = 1  # must be set to 1

#
#
# parameters for workload generator
regular_workload = 1     # number of names in the workload (GNS also read this parameter to load names into database)
mobile_workload = 0         # NOT used

gen_workload = True   # if True, generate new workload,

lookup_count = 10   # number of lookups at local name server,
update_count = 10   # number of updates at local name server

#
#
# GNS parameters common to name server / local name servers
is_experiment_mode = True  # set to True to run experiments, false otherwise.
primary_name_server = 3  # number of primary name servers

#lookupTrace = 'lookupTrace10'
#updateTrace = 'updateTrace10'

scheme = 'locality'         # 'locality' is for auspice
schemes = {'beehive':0, 'locality':1, 'uniform':2, 'static3':3, 'replicate_all':4}

#
#
# name server parameters
replication_interval = 10000   # interval (in sec) at which group changes are done.

name_actives = '' ## NOT used

#
#
# local name server parameters
queryTimeout = 300   # ms    # timeout value for a query (lookup/update)
maxQueryWaitTime = 2000  # ms  #  maximum wait time after which a query is declared failed

load_balancing = False  # Redirect to closest name server based on (RTT + server-load)


#
# logging options
nslog = 'FINE'       # Set to  FINER for more verbose output, and INFO, and SEVERE for less verbose output
nslogstat = 'FINE'  # Set to  FINER for more verbose output, and INFO, and SEVERE for less verbose output
lnslog = 'FINE'    # Set to  FINER for more verbose output, and INFO, and SEVERE for less verbose output
lnslogstat = 'FINE'  # Always set to 'FINE'


is_beehive_replication = False  # dont change
is_location_replication = False  # dont change
is_random_replication = False  # dont change
is_static_replication = False  # dont change

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
