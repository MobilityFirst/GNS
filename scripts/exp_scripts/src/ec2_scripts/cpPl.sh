config_folder=$1

echo "Copy pl: copy to NS and LNS ..."
cat pl_ns pl_lns | parallel -j+200 scp -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60   name-server.py local-name-server.py exp_config.py  ec2-user@{}:

cat pl_ns pl_lns | parallel -j+200 scp -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60   $config_folder/config_{}   ec2-user@{}:pl_config
echo "Copied!"

