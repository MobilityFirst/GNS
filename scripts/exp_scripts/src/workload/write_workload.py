import os
import sys
import inspect

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

from util.write_utils import write_tuple_array
__author__ = 'abhigyan'


class RequestType:

    LOOKUP = 1
    UPDATE = 2
    ADD = 3
    REMOVE = 4
    GROUP_CHANGE = 5
    DELAY = 6  # this is not a request. it introduces delay between the preceding and the next
            # request. the name field for DELAY entry is an integer that specifies the delay.


class WorkloadParams:
    OBJECT_SIZE = 'object_size_kb'
    TTL = 'ttl'


def get_trace_filename(trace_folder, client_id):
    return os.path.join(trace_folder, 'updateTrace', str(client_id))

def workload_writer(lns_req, folder):
    os.system('mkdir -p ' + folder)
    for lns, req in lns_req.items():
        fname = os.path.join(folder, lns)
        write_tuple_array(req, fname)

