import os, sys

# SET
experiment_run_time = 10

# SET
scheme = 'locality'
schemes = {'beehive':0, 'locality':1, 'uniform':2, 'static3':3, 'replicate_all':4}

# SET
lookupTrace = 'lookupTrace10'
updateTrace = 'updateTrace10'

# SET
num_ns = 80
num_lns = 80

ns_sleep = 5

load_balancing = False

primary_name_server = 3

replication_interval = 1000

is_beehive_replication = False
is_location_replication = False
is_random_replication = False
is_static_replication = False

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
