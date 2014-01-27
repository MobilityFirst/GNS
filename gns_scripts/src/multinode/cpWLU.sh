
user=$1
ssh_key=$2
pl_lns=$3
remote_folder=$4

lookupTrace=$5
updateTrace=$6

echo "Delete Workload ..."
cat $pl_lns | parallel -j+100 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{}  "rm -rf $remote_folder/lookup_* $remote_folder/update_*"

echo "Copying workload: "$lookupTrace" and "$updateTrace" ..."
cat $pl_lns | parallel -j+100 scp -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $lookupTrace/lookup_{} $user@{}:$remote_folder
cat $pl_lns | parallel -j+100 scp -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $updateTrace/update_{} $user@{}:$remote_folder
echo "Workload copy complete."
