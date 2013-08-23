#!/usr/bin/env python
import os
import sys

def kill_local_gnrs():
    #os.system('killall -9 java')
    #return
    os.system('ps aux | grep GNS.jar > temp.txt ')
    pids = get_pids('temp.txt')
    if len(pids) <=2:
        print('GNS: No processes killed')
        return
    #print (pids)
    print('Killed',len(pids) - 2 ,'processes')
    os.system('kill -9 ' + ' '.join(pids) + ' 2>/dev/null')
    os.system('rm temp.txt')


def get_pids(filename):
    f = open(filename)
    pids = []
    for line in f:
        try:
            pids.append(line.split()[1])
        except:
            print('Excpetion:', line)
            continue
    return pids


if __name__ == "__main__":
    kill_local_gnrs()
