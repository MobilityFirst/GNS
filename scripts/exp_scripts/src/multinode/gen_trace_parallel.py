#!/usr/bin/env python
import os
import sys

def main():
    first_mobile_name = 10000000 #int(sys.argv[1])
    num_mobiles = 100000000 #int(sys.argv[2])
    num_lookups = 500000000
    num_updates = 500000000
    hosts_file = sys.argv[1]
    trace_folder = sys.argv[2]

    script_location = '/home/abhigyan/gnrs/ec2_scripts/gen_mobile_trace.py'
    
    splits = 10
    split_size = num_mobiles/splits
    from read_array_from_file import read_col_from_file
    hosts = read_col_from_file(hosts_file)
    print hosts
    print 'Num hosts:', len(hosts)
    #assert len(hosts) >= splits
    for i in range(splits):
        first = first_mobile_name + split_size * i
        split_trace_folder = os.path.join(trace_folder, 'part' + str(i))
        script_folder = os.path.split(script_location)[0]
        
        cmd = 'ssh ' + hosts[i % len(hosts)] + ' "cd ' + script_folder + '; nohup ' +  script_location + ' ' + split_trace_folder + ' ' + str(first) + ' ' + str(split_size) + ' ' + str(num_lookups/splits) + ' ' + str(num_updates/splits) + ' > output 2> output < /dev/null &"'
        print cmd
        os.system(cmd)
        print 'submitted split', i




if __name__ == '__main__':
    main()
