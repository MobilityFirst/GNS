date
gns_folder=$1
echo $gns_folder "a;lkdjfal;skjfakl;sjfdlk;j"
cat pl_lns | parallel  -j+150 ssh -i auspice.pem  -oConnectTimeout=60 -oStrictHostKeyChecking=no -l ec2-user {} "mkdir -p $gns_folder \; cd $gns_folder \; python /home/ec2-user/local-name-server.py  --lookupTrace /home/ec2-user/lookup_{} --updateTrace /home/ec2-user/update_{} --ip {}"

date
