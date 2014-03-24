#!/usr/bin/env python
import os
import sys

hosts = [13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]

def main():
    f = open(sys.argv[1])
    count = 0

    for line in f:
        line = line.strip()
        if line == '':
            continue
        count += 1
        if line.endswith('/'):
            line = line[:-1]
        folder = line
        out_folder = folder + '_stats'

        cmd = './new_parser.py ' + folder + ' ' + out_folder + ' local '
        
        #host_id = count%len(hosts)
        #host = 'compute-0-' + str(hosts[host_id])
        #print host
        #print '\nFOLDER: Count = ', count, 'Folder:', folder,'\n'
        #cmd = 'ssh ' + host + ' "cd gnrs/logparse/;nohup ./new_parser.py ' + folder + ' ' + out_folder + ' local >/dev/null 2>/dev/null </dev/null &"'
        print cmd
        os.system(cmd)


if __name__ == "__main__":
    main()
