#!/bin/sh
user=$1
ssh_key=$2
pl_ns=$3
pl_lns=$4
gns_output_logs=$5

echo "Removing GNS logs from local name servers and name servers ..."
cat $pl_ns $pl_lns | parallel -j+100 ssh -l $user -i $ssh_key  -oConnectTimeout=60 -oStrictHostKeyChecking=no {} "rm -rf  $gns_output_logs"
#pssh -l ec2-user -i auspice.pem -h /tmp/rmLog "rm -rf  $paxos_log_folder $gns_output_logs"

echo "Removed GNS logs. Done!"
