import os
__author__ = 'abhigyan'


class RequestType:

    LOOKUP = 1
    UPDATE = 2
    ADD = 3
    REMOVE = 4

def get_trace_filename(trace_folder, client_id):
    return os.path.join(trace_folder, 'updateTrace', str(client_id))