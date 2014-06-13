import os
import sys
import inspect

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

from util.write_utils import write_tuple_array

__author__ = 'abhigyan'


class RequestType:

    LOOKUP = '1'
    UPDATE = '2'
    ADD = '3'
    REMOVE = '4'
    GROUP_CHANGE = '5'
    DELAY = '6'  # this is not a request. it introduces delay between the preceding and the next
            # request. the name field for DELAY entry is an integer that specifies the delay.
    RATE = '7'   # sends subsequent requests at given rate/sec. rate can be specified multiple
               # times during a trace to change the rate of later requests

"""
# set request rate to 100/sec
100 7
# send add for my_name
my_name 3
# add delay of 10000 ms between previous and next requests
10000 6
# send lookup for my_name
my_name 1
# send update for my_name
my_name 2
# send lookup for my_name
my_name 1
# send remove for my_name
my_name 4
"""

class WorkloadParams:
    EXP_TYPE = 'exp_type'   # type of experiment: trace / connect_time

    # parameters for trace based experiments
    OBJECT_SIZE = 'object_size_kb'
    TTL = 'ttl'

    # parameters for connect time experiment
    MOBILE_UPDATE_INTERVAL = 'mobile_update_interval'

class ExpType:
    TRACE = 'trace'
    CONNECT_TIME = 'connect_time'


def get_trace_filename(trace_folder, client_id):
    return os.path.join(trace_folder, 'updateTrace', str(client_id))


def workload_writer(lns_req, folder):
    """ Writes workload for a set of local name servers given a dict whose key is lns_id and value is list of requests.
    The file name of a trace is the lns_id of the corresponding local name server.
    """
    os.system('mkdir -p ' + folder)
    # delete old files
    os.system('rm  ' + folder + '/*')
    for lns_id, req in lns_req.items():
        fname = os.path.join(folder, str(lns_id))
        write_tuple_array(req, fname, p=False)

