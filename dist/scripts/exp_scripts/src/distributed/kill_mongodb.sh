#!/bin/sh
echo "Killing mongod instances ..."
user=$1
ssh_key=$2
pl_ns=$3
db_folder=$4


cat $pl_ns | parallel -j 50 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{} "killall -9 mongod"
echo "Deleting files ..."
cat $pl_ns | parallel -j 50 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{} "rm -rf $db_folder"
