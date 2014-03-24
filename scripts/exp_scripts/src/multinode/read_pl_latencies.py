#!/usr/bin/env python
import os
import sys


def main():
    read_pl_latencies()


def read_pl_latencies_folder(folder,num_nodes):
    """ returns a dict with dict[node1][node2] = 0 is the latency between two nodes. node1 and node2 are strings. """
    ## assume IDs are 0, 1, 2 ... n - 1 
    
    files = os.listdir(folder)

    latency_dict = {}
    for i in range(num_nodes):
        latency_dict[str(i)] = {}
        for j in range(num_nodes):
            latency_dict[str(i)][str(j)] = -1

    node_to_ID = get_node_to_ID(os.path.join(folder,files[0]))

    for f in files:
        node_id = node_to_ID[f] ## assume filename is node name
        full_path =  os.path.join(folder,f)
        f1 = open(full_path)
        for line in f1:
            tokens = line.strip().split()
            node2_id = tokens[0]  ## 0-th is ID, 2-nd is hostname
            latency = float(tokens[4]) ## ping latency
            latency_dict[node_id][node2_id] = latency
    
    return latency_dict, node_to_ID


def get_node_to_ID(filename):
    from read_array_from_file import read_col_from_file2
    nodenames = read_col_from_file2(filename, 2) ## 2nd col is hostname
    IDs = read_col_from_file2(filename, 0) ## 0th col is ID
    node_to_ID = {}
    for ID,node in zip(IDs,nodenames):
        node_to_ID[node] = ID
    return node_to_ID


def read_pl_latencies(folder):
    files = os.listdir(folder)
    latency_dict = {}
    num_nodes = len(files)
    print 'Num nodes', num_nodes
    for i in range(num_nodes):
        latency_dict[i] = {}
        for j in range(num_nodes):
            latency_dict[i][j] = -1
    
    for f in files:
        node_id = int(f)
        full_path = os.path.join(folder,f)
        f1 = open(full_path)
        print full_path
        for line in f1:
            tokens = line.strip().split()
            node2 = int(tokens[0])
            latency = float(tokens[4])
            latency_dict[node_id][node2] = latency
    
    return latency_dict




if __name__ == "__main__":
    main()
