import os
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

