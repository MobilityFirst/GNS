
cat pl_ns pl_lns | parallel -j+100 ssh -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 ec2-user@{} "rm -rf GNS.jar"
cat pl_ns pl_lns | parallel -j+100 ssh -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 ec2-user@{} "wget https://s3.amazonaws.com/auspice-gns/GNS.jar "
