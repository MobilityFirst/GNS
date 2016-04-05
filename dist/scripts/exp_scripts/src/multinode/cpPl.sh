
user=$1
ssh_key=$2
pl_ns=$3
pl_lns=$4
config_folder=$5
remote_folder=$6

#config_folder=$1

echo "Copy scripts to NS and LNS ..."
cat $pl_ns $pl_lns | parallel -j+200 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60  $user@{}  "mkdir -p $remote_folder"
cat $pl_ns $pl_lns | parallel -j+200 scp -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60   name-server.py local-name-server.py exp_config.py  $user@{}:$remote_folder

cat $pl_ns $pl_lns | parallel -j+200 scp -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60   $config_folder/config_{}   $user@{}:$remote_folder/pl_config
echo "Copied!"
