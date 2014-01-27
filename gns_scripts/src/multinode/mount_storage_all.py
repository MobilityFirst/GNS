#!/usr/bin/env python
import os
import sys


def main():
    mount_storage_all()


def mount_storage_all():
    print 'Copying script to ec2 LNS and NS ...'
    os.system('cat pl_ns pl_lns | parallel -j+100 scp  -i auspice.pem mount_storage.py ec2-user@{}:')
    print  "Copied\n"
    mount_storage2('pl_ns')
    mount_storage2('pl_lns')


def mount_storage2(filename):
    f = open(filename)

    for count, line in enumerate(f):
        hostname = line.strip()
        print 'Mounting storage at ' + str(count) + ' ' + hostname + ' ...'
        os.system('ssh -t -i auspice.pem -oStrictHostKeyChecking=no ec2-user@' + hostname + '  "./mount_storage.py"')
        print


if __name__ == "__main__":
    main()
