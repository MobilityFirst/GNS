#!/usr/bin/env python

import os, sys


def unzip_all(folder):
    files = os.listdir(folder)
    for f1 in files:
        path = os.path.join(folder, f1)
        os.system('cd ' + path + '; gzip -d gns_stat.xml.*.gz')


if __name__ == '__main__':
    unzip_all(sys.argv[1])
    
