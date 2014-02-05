source config.sh
echo 'Copy script to all locations ...'

cat $pl_ns $pl_lns | parallel -j+100 scp -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60  install_mongo_java.py $user@{}:

echo 'Execute script ...'

cat $pl_ns $pl_lns | parallel -j+100 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{} "python install_mongo_java.py"
# run in background
#cat $pl_ns $pl_lns | parallel -j+100 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{} "nohup python install_mongo_java.py > foo.out 2> foo.err 3</dev/null &"
