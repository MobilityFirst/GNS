echo "Running mongo db on ..."
cat pl_ns

db_folder=$1

cat pl_ns | parallel ssh -i auspice.pem ec2-user@{} "mkdir -p $db_folder/{}"

cat pl_ns | parallel ssh -i auspice.pem ec2-user@{} "nohup /home/ec2-user/mongodb/bin/mongod --dbpath $db_folder/{} >/dev/null 2>/dev/null < /dev/null &"
echo "Done!"
