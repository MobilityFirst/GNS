echo "Killed mongod instances, deleted files ..."
db_folder=$1

cat pl_ns | parallel ssh -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 ec2-user@{} "killall -9 mongod"
cat pl_ns | parallel ssh -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 ec2-user@{} "rm -rf $db_folder"