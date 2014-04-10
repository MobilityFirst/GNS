
import os
import inspect
import sys

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

from util.read_utils import read_keys_values


__author__ = 'abhigyan'


class FinalStats:
    """After a test completes, this reads statistics regarding number of successful requests, and their latency."""
    requests = None
    success = None
    failed = None
    read = None
    write = None
    add = None
    remove = None
    read_failed = None
    write_failed = None
    add_failed = None
    remove_failed = None

    read_perc90 = None
    write_perc90 = None
    add_perc90 = None
    remove_perc90 = None

    failed_read_perc90 = None
    failed_write_perc90 = None
    failed_add_perc90 = None
    failed_remove_perc90 = None

    def __init__(self, stats_folder):
        summary_file = os.path.join(stats_folder, 'summary.txt')
        if os.path.exists(summary_file):
            keys_values = read_keys_values(summary_file)
            self.requests = int(keys_values['All'])
            self.success = int(keys_values['Success'])
            self.failed = int(keys_values['Failed'])
            self.read = int(keys_values['Read'])
            self.write = int(keys_values['Write'])
            self.add = int(keys_values['Add'])
            self.remove = int(keys_values['Remove'])
            self.read_failed = int(keys_values['Failed-Read'])
            self.write_failed = int(keys_values['Failed-Write'])
            self.add_failed = int(keys_values['Failed-Add'])
            self.remove_failed = int(keys_values['Failed-Remove'])
        latency_file = os.path.join(stats_folder, 'latency_stats.txt')
        if os.path.exists(latency_file):
            keys_values = read_keys_values(latency_file)
            if 'readperc90' in keys_values:
                self.read_perc90 = float(keys_values['readperc90'])
            if 'writeperc90' in keys_values:
                self.write_perc90 = float(keys_values['writeperc90'])
            if 'addeperc90' in keys_values:
                self.add_perc90 = float(keys_values['addperc90'])
            if 'removeeperc90' in keys_values:
                self.remove_perc90 = float(keys_values['removeperc90'])

            if 'failed_readperc90' in keys_values:
                self.failed_read_perc90 = float(keys_values['failed_readperc90'])
            if 'failed_writeperc90' in keys_values:
                self.failed_write_perc90 = float(keys_values['failed_writeperc90'])
            if 'failed_addeperc90' in keys_values:
                self.failed_add_perc90 = float(keys_values['failed_addperc90'])
            if 'failed_removeeperc90' in keys_values:
                self.failed_remove_perc90 = float(keys_values['failed_removeperc90'])



