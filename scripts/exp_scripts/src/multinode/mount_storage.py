#!/usr/bin/env python

import os
import sys



def main():
    mount_partitions()


def mount_partitions():
    """Mounts partitions on an EC2 instance. If any parition is not mounted successfully, a file with name mountError is created."""
    
    os.system('rm -rf mountError')
    #os.system('ssh -t -i auspice.pem -l ec2-user ' + hostname + ' "sudo grep requiretty /etc/sudoers"')
    result1 = mount_partition('/dev/sdb', '/media/ephemeral0')
    
    result2 = mount_partition('/dev/sdc', '/media/ephemeral1')

    print result1
    print result2
    if result1 == False or result2 == False:
        fw = open('mountError', 'w')
        fw.write(str(result1) + '\n')
        fw.write(str(result2) + '\n')
        fw.close()


def mount_partition(device_name, mount_path):
    """Method mounts device_name at mount_path. Returns True if successful, False otherwise."""
    
    """sudo mkfs /dev/sdb
sudo mkdir -p /media/ephemeral0
sudo mount -t ext2 /dev/sdb /media/ephemeral0/
sudo chmod 777 /media/ephemeral0/"""
    if not os.path.exists(device_name):
        print 'ERROR: Device does not exist: ' + device_name
        return False
    if not os.path.exists(mount_path):
        os.system('sudo mkdir -p ' + mount_path)
    
    os.system('sudo mount -t ext2 ' + device_name + ' ' + mount_path)
    print device_name + ' mounted at ' + mount_path
    
    os.system('sudo chmod 777 ' + mount_path)
    print 'Write permissions updated'
    return check_mounted(mount_path)


def check_mounted(mount_path):
    """Checks if a partition is mounted at mount_path. Returns True if successful, False otherwise."""
    test_folder = os.path.join(mount_path, 'testFolder')
    if os.path.exists(test_folder):
        return True
    try:
        os.mkdir(test_folder)
        return True
    except:
        return False


main()
