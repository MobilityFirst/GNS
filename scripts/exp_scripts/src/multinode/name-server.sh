date
gns_folder=$1
cat pl_ns | parallel -j+50 ssh -i auspice.pem  -oConnectTimeout=60 -oStrictHostKeyChecking=no -l ec2-user {} "mkdir -p $gns_folder\; cd $gns_folder\; python /home/ec2-user/name-server.py --ip {}"
date
