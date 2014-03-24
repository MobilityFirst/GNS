
echo 'Copy script to all locations ...'

cat pl_ns | parallel -j+100 scp -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60  install_mongo_java.py ec2-user@{}:

echo 'Execute script ...'

cat pl_ns | parallel -j+100 ssh -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 ec2-user@{} "nohup python install_mongo_java.py > foo.out 2> foo.err 3</dev/null &"
