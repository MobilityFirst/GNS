#!/usr/bin/env python
import os
import sys
from gen_mobile_trace import generate_mobile_trace
from read_array_from_file import read_col_from_file
import random
import exp_config

pl_lns = exp_config.pl_lns_workload

lookup_regular_folder = exp_config.lookup_regular_folder

def main():
    #load = 1
    # regular-queries = 200 K (270 K)
    # regular-updates = 20
    # mobile-queries = 50 K  ---> show improvement across requests.
    # mobile-updates = 250 K
    
    # load = 20
    # regular-queries = 200K * 20 = 4 M
    # regular-updates = 400
    # mobile-queries = 1.2 M
    # mobile-updates = 5 M
    
    load = int(sys.argv[1])
    trace_generator(load)    


def trace_generator(load, lookup_trace, update_trace):
    regular_updates = int(20 * load)
    regular_names = 1000
    
    folder = os.path.split(lookup_trace)[0]
    mobile_update_trace = os.path.join(folder, 'updateTraceMobile')
    mobile_lookup_trace = os.path.join(folder, 'lookupTraceMobile')
    os.system('rm -rf ' + mobile_update_trace)
    os.system('mkdir -p ' + mobile_update_trace)
    os.system('rm -rf ' + mobile_lookup_trace)
    os.system('mkdir -p ' + mobile_lookup_trace)
    
    #update_trace = 'updateTrace' + str(load)
    #lookup_trace = 'lookupTrace' + str(load)    
    os.system('rm -rf ' + update_trace)
    os.system('mkdir -p ' + update_trace)
    os.system('rm -rf ' + lookup_trace)
    os.system('mkdir -p ' + lookup_trace)
    
    generate_mobile_trace(load, mobile_lookup_trace, mobile_update_trace)
    
    # combine lookupTrace for regular + mobile
    
    f = open(pl_lns)
    
    for line in f:
        host_name = line.split()[0].strip()
        # MODIFY
        load1 = int(load)
        #print 'Load = ', load1
        lookup_regular_orig_file = os.path.join(lookup_regular_folder, 'lookup_' + host_name)
        cmd = 'cat ' + ' '.join([lookup_regular_orig_file]*load1) + ' ' + \
              mobile_lookup_trace + '/lookup_' + host_name +\
              ' > ' + lookup_trace + '/lookup_' + host_name
        os.system(cmd)
        #os.system('wc ' + lookup_trace + '/lookup_' + host_name)
        if load - load1  > 0:
            append_fractional_trace(load - load1, lookup_regular_orig_file, lookup_trace + '/lookup_' + host_name)
            
        #print cmd
        randomize_trace_file(lookup_trace + '/lookup_' + host_name)
        
        #os.system('cat ' + ' '.join('updateTraceRegular/lookup_' + host_name))
    # combine updateTrace for regular + mobile
    
    os.system('cp -r ' + mobile_update_trace + '/* ' + update_trace + '/')
    
    # generate updateTrace regular
    random.seed(9987.0)
    regular_update_list = []
    for i in range(regular_updates):
        regular_name = random.randint(0, regular_names - 1) # randint method is inclusive
        regular_update_list.append(regular_name)
    #print 'Length:', len(regular_update_list), 'Updates:',regular_update_list
    
    update_files = os.listdir(update_trace)
    for i in range(len(regular_update_list)):
        # select random
        f1 = update_files[random.randint(0, len(update_files) - 1) ]
        #print i,f1
        os.system('echo ' + str(regular_update_list[i]) + ' >> ' + update_trace + '/' + f1)
    
    for update_file in update_files:
        randomize_trace_file(update_trace + '/' + update_file)

        
    # remove mobile trace
    os.system('rm -rf ' + mobile_update_trace + ' ' + mobile_lookup_trace)


def append_fractional_trace(load, regular_file,filename):
    
    f = open(regular_file)
    fw = open('temp.txt', 'w')
    for line in f:
        x = random.random()
        if x > load:
            continue
        else:
            fw.write(line)
    fw.close()
    os.system('cat temp.txt >> '  + filename)


def randomize_trace_file(filename):
    
    values = read_col_from_file(filename)
    random.shuffle(values)
    
    from write_array_to_file import write_array
    write_array(values, filename, p = False)
    #os.system('wc -l ' + filename)



if __name__ == "__main__":
    main()
