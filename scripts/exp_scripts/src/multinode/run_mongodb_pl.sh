
user=$1
ssh_key=$2
pl_ns=$3
mongo_path=$4
db_folder=$5


echo "Running mongo db on ..."
cat $pl_ns
echo 'Creating data folders ...'
cat $pl_ns | parallel ssh -i $ssh_key $user@{} "mkdir -p $db_folder/{}"

echo 'Running mongod process...'

#cat $pl_ns | parallel ssh -i $ssh_key $user@{} "nohup mongod --nojournal --smallfiles --dbpath $db_folder/{} >/dev/null 2>/dev/null < /dev/null &"
cat $pl_ns | parallel ssh -i $ssh_key $user@{} "nohup $mongo_path/mongod --smallfiles --dbpath $db_folder/{} >/dev/null 2>/dev/null < /dev/null &"

echo 'Check if  mongod process are running ...'
cat $pl_ns | parallel ssh -i $ssh_key $user@{} "ps aux | grep mongod"
echo "Done!"
