#!/bin/sh
user=$1
ssh_key=$2
pl_ns=$3
pl_lns=$4
local_jar=$5
remote_jar_folder=$6
jar_file_remote=$7


#echo "NOT SENDING JAR TO LNS!!!"
#echo "Delete GNS.jar at LNS ..."
#cat $pl_ns $pl_lns > /tmp/pl_nodes

#echo "Delete GNS.jar at nodes ..."
#pssh -l umass_bittorrent -h /tmp/pl_nodes 'rm GNS.jar'
echo "Create folder at remote dirs where jar will be copied ... "
cat $pl_ns $pl_lns | parallel -j+100 ssh -l $user -i $ssh_key  -oConnectTimeout=60 -oStrictHostKeyChecking=no {} "mkdir -p $remote_jar_folder"
echo "Syncing jar from $local_jar to $jar_file_remote "
cat $pl_ns $pl_lns | parallel -j+100 rsync -e \"ssh -i $ssh_key\" $local_jar $user@{}:$jar_file_remote

#echo "Copy GNS.jar to nodes ..."
#pscp -l umass_bittorrent -h /tmp/pl_nodes  $jar_dir/GNS.jar  /home/umass_bittorrent/

#echo "Delete GNS.jar at NS ..."
#pssh -l umass_bittorrent -h pl_ns 'rm GNS.jar'

#echo "Copy GNS.jar to NS ..."
#pscp -l umass_bittorrent -h pl_ns $jar_dir/GNS.jar  /home/umass_bittorrent/ 
echo "DONE!"
