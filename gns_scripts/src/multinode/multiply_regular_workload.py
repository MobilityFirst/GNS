#!/usr/bin/env python
import os
import sys

def main():
    orig_folder = sys.argv[1] #  use gnrs/ec2_data/pl_data_new_100K or *_1M
    final_folder = sys.argv[2] # 
    final_requests = int(sys.argv[3]) 
    orig_requests = 1000000
    print 'Initial number of requests is 1M'
    num_names = 10000 ## hardcoded    
    num_names_final = 10000000    #int(sys.argv[3])
    scale = num_names_final/num_names  ## number of times we will multiply a name
    
    sample_ratio = 1.0 * final_requests / (orig_requests * scale)
    
    os.system('mkdir -p ' + final_folder)
    
    files = os.listdir(orig_folder)
    count = 0
    import random
    for f in files:
        print count, f
        orig_file = os.path.join(orig_folder, f)
        output_file = os.path.join(final_folder, f)
        from read_array_from_file import read_col_from_file
        values = read_col_from_file(orig_file)
        from random import shuffle
        shuffle(values)
        values = values[:int(sample_ratio * len(values))]
        fw = open(output_file, 'w')
        for val in values:
            val = int(val)
            for n in range(scale):
                new_val = num_names * n + val
                fw.write(str(new_val))
                fw.write('\n')
                count += 1
        fw.close()
    print 'Num values', count

if __name__ == '__main__':
    main()
