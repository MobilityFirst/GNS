lookupTrace=$1
updateTrace=$2

echo "Delete Workload ..."
cat pl_lns | parallel -j+100 ssh -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 ec2-user@{}  "rm lookup_* update_*"

echo "Copying workload: "$lookupTrace" and "$updateTrace" ..."
cat pl_lns | parallel -j+100 scp -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 $lookupTrace/lookup_{} ec2-user@{}:
cat pl_lns | parallel -j+100 scp -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 $updateTrace/update_{} ec2-user@{}:
echo "Workload copy complete."
