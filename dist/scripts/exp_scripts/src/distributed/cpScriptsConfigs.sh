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

