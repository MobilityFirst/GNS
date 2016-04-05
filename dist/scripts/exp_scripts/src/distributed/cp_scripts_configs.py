import os
from run_cmds import run_cmds

__author__ = 'abhigyan'


def cp_scripts_configs(user, ssh_key, ns_ids, lns_ids, remote_folder, config_file, workload_config_file,
                       node_config_folder, remote_node_config_file):
    """
    #!/bin/sh

    user=$1
    ssh_key=$2
    pl_ns=$3
    pl_lns=$4
    config_file=$5
    workload_config_file=$6
    node_config_folder=$7
    remote_folder=$8
    remote_node_config=$9

    echo "Copy scripts to NS and LNS ..."
    cat $pl_ns $pl_lns | parallel -j+200 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{}  "mkdir -p $remote_folder"
    cat $pl_ns $pl_lns | parallel -j+200 scp -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 name_server.py argparse.py local_name_server.py exp_config.py $config_file $workload_config_file  $user@{}:$remote_folder

    cat $pl_ns $pl_lns | parallel -j+200 scp -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $node_config_folder/config_{} $user@{}:$remote_folder/$remote_node_config
    echo "Copied!"
    """

    ns_lns_ids = {}
    ns_lns_ids.update(ns_ids)
    ns_lns_ids.update(lns_ids)

    # step 1: mkdir remote folders for each node
    cmds = []
    for ns, ns_host in ns_lns_ids.items():
        node_folder = os.path.join(remote_folder, str(ns))
        cmd = 'ssh -i ' + ssh_key + ' -oStrictHostKeyChecking=no -oConnectTimeout=60 ' + user + '@' + ns_host + \
              ' "mkdir -p ' + node_folder + '" '
        cmds.append(cmd)
    run_cmds(cmds)

    # step 2: copy files (except node config)
    cmds = []
    for ns, ns_host in ns_lns_ids.items():
        node_folder = os.path.join(remote_folder, str(ns))
        cmd = 'scp -i ' + ssh_key + ' -oStrictHostKeyChecking=no -oConnectTimeout=60 ' \
              ' name_server.py argparse.py local_name_server.py exp_config.py ' + \
              config_file + ' ' + workload_config_file + ' ' + user + '@' + ns_host + ':' + node_folder
        cmds.append(cmd)
    run_cmds(cmds)

    # step 3: copy node config file
    cmds = []
    for ns, ns_host in ns_lns_ids.items():
        node_folder = os.path.join(remote_folder, str(ns))
        node_config_local = os.path.join(node_config_folder, str(ns))
        cmd = 'scp -i ' + ssh_key + ' -oStrictHostKeyChecking=no -oConnectTimeout=60 ' + \
              node_config_local + ' ' + user + '@' + ns_host + ':' + os.path.join(node_folder, remote_node_config_file)
        cmds.append(cmd)
    run_cmds(cmds)
