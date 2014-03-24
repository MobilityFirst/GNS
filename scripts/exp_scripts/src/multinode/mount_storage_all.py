#!/usr/bin/env python
import os
import sys

import exp_config

def main():
    mount_storage_all()


def mount_storage_all():

    print 'Copying script to ec2 LNS and NS ...'
    pl_ns = exp_config.ns_file
    pl_lns = exp_config.lns_file
    os.system('cat ' + pl_ns + ' '+ pl_lns + ' | parallel -j+100 scp  -i ' +  exp_config.ssh_key + ' mount_storage.py ' + exp_config.user + '@{}:')
    print  "Copied\n"
    mount_storage2(pl_ns)
    mount_storage2(pl_lns)


def mount_storage2(filename):
    f = open(filename)

    for count, line in enumerate(f):
        hostname = line.strip()
        print 'Mounting storage at ' + str(count) + ' ' + hostname + ' ...'
        os.system('ssh -t -i ' + exp_config.ssh_key + ' -oStrictHostKeyChecking=no ' + exp_config.user + '@' + hostname + '  "./mount_storage.py"')
        print


if __name__ == "__main__":
    main()
