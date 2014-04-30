import os
import sys
import inspect

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

from util.read_utils import read_col_from_file

import write_workload

__author__ = 'abhigyan'


def main():
    trace_folder = '../resources/workload'    # folder where output will be generated
    lns_geo_file = '../resources/pl_lns_geo_ec2'   # files with hostname, and lat-long of local name servers (lns)
    gen_add_requests(trace_folder, lns_geo_file)


def gen_add_requests(trace_folder, lns_file, number_names=10000, first_name=0, num_lns=-1,
                     append_to_file=False, lns_ids=None, name_prefix=None):
    """Generates 'add' requests for 'number_names' from a set of local name servers
    # Workload generation parameters
    number_names = 10000   # number of names in workload.
    first_name = 0    # workload will have names in range (first_name, first_name + number_names)


    num_lns = -1             # set this to -1 if you want to generate trace for all LNS in lns geo file.
                             # otherwise trace will be generated for first 'num_lns' in lns geo file

    append = True            # append new requests to end of trace files

    lns_ids = None           # list of IDs of local name servers in the order of their names in LNS geo file.
                             # if lns_ids is not None, trace file for that LNS is name the same as is ID.

    name_prefix = None       # if name-prefix is not None, append given prefix to all names
    """

    os.system('mkdir -p ' + trace_folder)

    lns_list = read_col_from_file(lns_file)
    if num_lns != -1 and num_lns < len(lns_list):
        lns_list = lns_list[:len(lns_list)]

    # start file writers
    fw_lns = []  # list of file writers for each lns
    for i, lns in enumerate(lns_list):
        f_name = lns
        if lns_ids is not None:
            f_name = lns_ids[i]
        output_file = os.path.join(trace_folder, f_name)
        if append_to_file:
            fw_lns.append(open(output_file, 'a'))
        else:
            fw_lns.append(open(output_file, 'w'))

    for i in range(number_names):
        lns_index = i % len(lns_list)
        if name_prefix is None:
            name = str(i + first_name)
        else:
            name = name_prefix + str(i)
        request = [name, write_workload.RequestType.ADD]
        for t in request:
            fw_lns[lns_index].write(str(t))
            fw_lns[lns_index].write('\t')
        fw_lns[lns_index].write('\n')

    # close all file writers
    for fw in fw_lns:
        fw.close()


if __name__ == '__main__':
    main()

