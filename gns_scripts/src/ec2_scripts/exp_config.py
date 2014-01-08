import os, sys


gnrs_dir = '/Users/abhigyan/Documents/workspace/GNSev/'  ## location of top-level folder checked out from SVN.

paxos_log_folder = '/Users/abhigyan/Documents/workspace/GNSev/local/paxoslog/'  ## folder where paxos logs are stored

delete_paxos_log = False    ## if True, delete all paxos logs before starting a new experiment. if False, recover data from logs.
is_experiment_mode = False      ## set to True to run experiments, false otherwise.
# SET
experiment_run_time = 10    ## duration of experiment (seconds)

# 
num_ns = 8  ## NOT USED
num_lns = 8 ## NOT USED

#lookupTrace = 'lookupTrace10'
#updateTrace = 'updateTrace10'

#set
regular_workload = 10     ## number of names in the workload
mobile_workload = 0         ## NOT used


queryTimeout = 2000   # ms    ## timeout value for a query (lookup/update)
maxQueryWaitTime = 11000 # ms  ##  maximum wait time after which a query is declared failed

replication_interval = 10   ## interval (in sec) at which group changes are done. 

ns_sleep = 5      ## after starting name servers, wait for ns_sleep seconds before starting local name servers.
                 ## wait period is for loading names into database

failed_nodes = None   ## NOT used

load_balancing = False  ## Redirect to closest name server based on (RTT + server-load)

primary_name_server = 3  ## number of primary name servers



# 

nslog = 'FINE'       # Set to  FINER for more verbose output, and INFO, and SEVERE for less verbose output
nslogstat = 'FINE'  # Set to  FINER for more verbose output, and INFO, and SEVERE for less verbose output
lnslog = 'FINE'    # Set to  FINER for more verbose output, and INFO, and SEVERE for less verbose output
lnslogstat = 'FINE'  # Always set to 'FINE'


name_actives = '/Users/abhigyan/Documents/workspace/GNSev/local/nameActives'  ## NOT used



scheme = 'locality'         ## 'locality' is for auspice
schemes = {'beehive':0, 'locality':1, 'uniform':2, 'static3':3, 'replicate_all':4}

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
