
echo "Copy create swap file: copy to NS ..."
cat pl_ns  | parallel -j+200 scp -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60   create_swap.sh ec2-user@{}:
echo "Copied!"

