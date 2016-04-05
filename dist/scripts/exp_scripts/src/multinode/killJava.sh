#!/bin/sh
user=$1
ssh_key=$2
pl_ns=$3
pl_lns=$4

cat $pl_lns $pl_ns | parallel -j+100 ssh -i $ssh_key  -oConnectTimeout=60 -oStrictHostKeyChecking=no  -l $user {} 'killall -9 java mpstat'
#cat pl_ns | parallel -j+100 ssh -i auspice.pem   -oConnectTimeout=60 -oStrictHostKeyChecking=no -l ec2-user {} 'killall -9 java mpstat'
