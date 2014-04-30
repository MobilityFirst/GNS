import os
import pickle

__author__ = 'abhigyan'


class EventType:
    NODE_CRASH = 1  # node failed
    NODE_RESTART = 2  # node restarted (i.e., it will recover from existing data)
    NODE_ADD = 3  # add a node to system
    NODE_REMOVE = 4  # remove a node from system


class ExperimentEvent:
    """Base class for all events during a test"""

    event_type = None
    event_time = None

    def get_str(self):
        pass


class NodeCrashEvent(ExperimentEvent):
    """Event denoting a failure of node at given time """
    node_id = None

    def __init__(self, node_id, event_time):
        self.event_time = event_time
        self.node_id = node_id
        self.event_type = EventType.NODE_CRASH


class NodeStartEvent(ExperimentEvent):
    """Event denoting a restarting of a node at a given time """
    node_id = None

    def __init__(self, node_id, event_time):
        self.event_time = event_time
        self.node_id = node_id
        self.event_type = EventType.NODE_RESTART


def write_events_to_file(events, events_file):
    """Writes a set of events to file"""
    parent_dir = os.path.split(events_file)[0]
    if not os.path.exists(parent_dir):
        os.system('mkdir -p ' + parent_dir)
    pickle.dump(events, open(events_file, 'wb'))


def parse_experiment_events(events_file):
    """Parses file describing events during an experiment and returns a list of events sorted chronologically"""
    if events_file is None or not os.path.exists(events_file):
        return None
    return pickle.load(open(events_file, 'rb'))

