source config.sh

pl_ns=$1

db_folder=$2

backup_folder=$3

echo Backing up db state ...
cat $pl_ns | parallel ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{} "mongodb/bin/mongod --dbpath $db_folder/{} --shutdown \; rm -rf $backup_folder \; cp -r $db_folder $backup_folder"
echo Done!
