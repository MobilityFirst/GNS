#!/usr/bin/env python

import os
import sys

home_folder = '/home/ec2-user/'

mongo_folder = 'mongodb'

java_folder = 'jdk'

def main():
    check_install_mongo_java()


def check_install_mongo_java():
    mongo_home = os.path.join(home_folder, mongo_folder)
    print '**********', mongo_home
    if not os.path.exists(mongo_home):
        install_mongo()
    else:
        print 'Mongo exists.'
    java_home = os.path.join(home_folder, java_folder)
    print '**********', java_home
    if not os.path.exists(java_home):
        install_java()
    else:
        print 'Java exists.'

def install_mongo():
    os.system('cd ' + home_folder + '; rm  -rf mongodb-linux-x86_64-2.4.8.tgz*; wget https://s3.amazonaws.com/auspice-gns/mongodb-linux-x86_64-2.4.8.tgz')
    os.system('cd ' + home_folder + '; tar -xzf mongodb-linux-x86_64-2.4.8.tgz; mv mongodb-linux-x86_64-2.4.8 ' + mongo_folder)
    os.system('cd ' + home_folder + '; rm -rf mongodb-linux-x86_64-2.4.8.tgz')
    print 'mongodb installed.'


def install_java():
    os.system('cd ' + home_folder + '; rm -rf server-jre-7u45-linux-x64* ; wget https://s3.amazonaws.com/auspice-gns/server-jre-7u45-linux-x64.gz')
    os.system('cd ' + home_folder + '; gzip -d server-jre-7u45-linux-x64.gz; tar -xf server-jre-7u45-linux-x64; mv jdk1.7.0_45 ' + java_folder)
    os.system('cd ' + home_folder + '; rm -rf server-jre-7u45-linux-x64')
    print 'jdk installed'


if __name__ == '__main__':
    main()
