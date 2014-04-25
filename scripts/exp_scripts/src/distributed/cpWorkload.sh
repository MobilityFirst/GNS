#!/bin/sh


user=$1
ssh_key=$2
pl_lns=$3
remote_folder=$4

#lookupTrace=$6
updateTrace=$5
remoteUpdateTrace=$6

# This script assumes that file name of the request trace is the same as the hostname.

echo "Delete Workload ..."
cat $pl_lns | parallel -j+100 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{}  "rm -rf $remote_folder/$remoteUpdateTrace"

echo "Copying workload ..."

if [ -n "$updateTrace" ]; then
    echo "Copying update trace ....."
    cat $pl_lns | parallel -j+100 scp -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $updateTrace/{} $user@{}:$remote_folder/$remoteUpdateTrace
fi
echo "Workload copy complete."
