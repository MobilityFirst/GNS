echo "Removing earlier jar ..."
cat pl_ns pl_lns | parallel -j+20 ssh -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 ec2-user@{} "rm -rf GNS.jar"
echo "Downloading new jar ..."
cat pl_ns pl_lns | parallel -j+20 ssh -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 ec2-user@{} "wget https://s3.amazonaws.com/auspice-gns/GNS.jar > /dev/null 2> /dev/null"
