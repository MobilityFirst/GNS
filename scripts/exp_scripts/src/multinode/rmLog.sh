user=$1
ssh_key=$2
pl_ns=$3
pl_lns=$4
paxos_log_folder=$5
gns_output_logs=$6

echo "Remove Paxos and GNS logs: local name servers and name servers ..."
cat $pl_ns $pl_lns | parallel -j+100 ssh -l $user -i $ssh_key  -oConnectTimeout=60 -oStrictHostKeyChecking=no {} "rm -rf  $paxos_log_folder $gns_output_logs"
#pssh -l ec2-user -i auspice.pem -h /tmp/rmLog "rm -rf  $paxos_log_folder $gns_output_logs"

echo "Remove log: done!"
