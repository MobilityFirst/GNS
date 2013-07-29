#!/usr/bin/env python
import os
import sys
from os.path import join


def main():
    node_id = sys.argv[1]
    num_names = 100000
    lookup_count = int(sys.argv[2])
    update_count = int(sys.argv[3])
    
    write_local_trace(node_id, lookup_count, update_count)

def write_local_trace(node_id, lookup_count, update_count):
    write_trace(join('lookupLocal', str(node_id)), lookup_count)
    write_trace(join('updateLocal', str(node_id)), update_count)
    
def write_trace(filename, size):
    x = []
    import random
    for i in range(num_names):
        x.append(random.randint(0, num_names))
    
    from write_array_to_file import write_array
    write_array(x, filename,  p = True)

                        

if __name__ == "__main__":
    #main()
    write_trace(sys.argv[1], int(sys.argv[2]))
