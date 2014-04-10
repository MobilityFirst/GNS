import os
__author__ = 'abhigyan'


class RequestType:

    LOOKUP = 1
    UPDATE = 2
    ADD = 3
    REMOVE = 4
    DELAY = 5  # this is not a request. it introduces delay between the preceding and the next
            # request. the name field for DELAY entry is an integer that specifies the delay.


def get_trace_filename(trace_folder, client_id):
    return os.path.join(trace_folder, 'updateTrace', str(client_id))