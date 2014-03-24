
cat pl_lns | parallel -j+100 ssh -i auspice.pem  -oConnectTimeout=60 -oStrictHostKeyChecking=no  -l ec2-user {} 'killall -9 java' 
cat pl_ns | parallel -j+100 ssh -i auspice.pem   -oConnectTimeout=60 -oStrictHostKeyChecking=no -l ec2-user {} 'killall -9 java' 
