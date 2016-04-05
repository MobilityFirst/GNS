import os
from run_cmds import run_cmds

__author__ = 'abhigyan'


def get_log(user, ssh_key, ns_ids, lns_ids, local_output_folder, remote_log_folder):
    """#!/bin/sh
user=$1
ssh_key=$2
pl_ns=$3
pl_lns=$4
local_output_folder=$5
remote_log_folder=$6

#nozip="$1"
date
echo "Remove earlier log ..."
rm -rf log/*
mkdir -p log

echo "Create local directories ..."
cat $pl_lns | parallel -j+100 mkdir log/log_lns_{} &
cat $pl_ns | parallel -j+100 mkdir log/log_ns_{} &

#if [ "$nozip" != "--nozip" ]; then
echo "Gzip logs at LNS and NS ..."
# remove console output files, delete log folder
cat $pl_ns $pl_lns | parallel -j+100 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{}  "gzip  $remote_log_folder/* $remote_log_folder/log/*"
#echo "Gzip logs at NS ..."

#date
#echo "Copying config file from LNS ..."
#cat $pl_lns | parallel -j+100 scp -i $ssh_key -r -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{}:pl_config log/log_lns_{} &

echo "Copying logs from LNS ..."
#echo "Copying gnrs_stat.xml files only ..."
cat $pl_lns | parallel -j+100 scp -i $ssh_key -r -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{}:$remote_log_folder/* log/log_lns_{}

#date
#echo "Copying config file from NS ..."
#cat $pl_ns | parallel -j+100 scp -i $ssh_key -r -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{}:pl_config log/log_ns_{} &

date
echo "Copying logs from NS ..."
cat $pl_ns | parallel -j+100 scp -i $ssh_key -r -oStrictHostKeyChecking=no -oConnectTimeout=60 -r $user@{}:$remote_log_folder/* log/log_ns_{}


#cat pl_ns | parallel -j+100 scp -i auspice.pem -r -oStrictHostKeyChecking=no -oConnectTimeout=60 ec2-user@{}:ping_output log/log_ns_{}
#cat pl_lns | parallel -j+100 scp -i auspice.pem -r -oStrictHostKeyChecking=no -oConnectTimeout=60 ec2-user@{}:ping_output log/log_lns_{}


# OLD
#cat pl_ns | parallel -j+100 scp -i auspice.pem -r -oStrictHostKeyChecking=no -oConnectTimeout=10 ec2-user@{}:log log/log_ns_{}
#For GNRS-westy
#cat pl_lns | parallel -j+100 scp -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=10 -r ec2-user@{}:log log/log_lns_{}

if [ "$local_output_folder" != "" ]; then
    mkdir -p $local_output_folder
    mv log/* $local_output_folder
fi
echo "Output copied to: " $local_output_folder
    """
    # local_output_folder = os.path.join(local_output_folder, 'log')
    # print '*********** GET LOGS BEGIN >>>>>> ',  local_output_folder
    os.system('rm -rf ' + local_output_folder)
    os.system('mkdir -p ' + local_output_folder)

    ns_lns_ids = {}
    ns_lns_ids.update(ns_ids)
    ns_lns_ids.update(lns_ids)
    print ns_ids
    print lns_ids

    print 'Create NS log folders ...'
    cmds = []
    for node_id in ns_ids:
        folder = os.path.join(local_output_folder, get_ns_folder(node_id))
        cmds.append('mkdir -p ' + folder)
    run_cmds(cmds)

    print 'Create LNS log folders ...'
    cmds = []
    for node_id in lns_ids:
        folder = os.path.join(local_output_folder, get_lns_folder(node_id))
        cmds.append('mkdir -p ' + folder)
    run_cmds(cmds)

    print 'Gzip remote folders ... '
    cmds = []
    for node_id, ns_host in ns_lns_ids.items():
        remote_node_folder = os.path.join(remote_log_folder, str(node_id))
        cmd = 'ssh -i ' + ssh_key + ' -oStrictHostKeyChecking=no -oConnectTimeout=60 ' + user + '@' + ns_host + \
              ' "gzip  ' + remote_node_folder + '/* ' + remote_node_folder + '/log/*" '
        cmds.append(cmd)
    run_cmds(cmds)

    print 'Copy LNS logs ...'
    cmds = []
    for node_id, ns_host in ns_ids.items():
        remote_node_folder = os.path.join(remote_log_folder, str(node_id))
        local_node_folder = os.path.join(local_output_folder, get_ns_folder(node_id))
        cmd = 'scp -i ' + ssh_key + ' -r -oStrictHostKeyChecking=no -oConnectTimeout=60 ' + user + '@' + ns_host + ':' + \
              remote_node_folder + '/* ' + local_node_folder
        cmds.append(cmd)
    run_cmds(cmds)
    
    print 'Copy NS logs ...'
    cmds = []
    for node_id, ns_host in lns_ids.items():
        remote_node_folder = os.path.join(remote_log_folder, str(node_id))
        local_node_folder = os.path.join(local_output_folder, get_lns_folder(node_id))
        cmd = 'scp -i ' + ssh_key + ' -r -oStrictHostKeyChecking=no -oConnectTimeout=60 ' + user + '@' + ns_host + ':' + \
              remote_node_folder + '/* ' + local_node_folder
        cmds.append(cmd)
    run_cmds(cmds)


def get_lns_folder(node_id):
    return 'log_lns_' + str(node_id)


def get_ns_folder(node_id):
    return 'log_ns_' + str(node_id)
