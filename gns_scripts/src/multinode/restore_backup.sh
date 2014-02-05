
user=$1

ssh_key=$2

pl_ns=$3

backup_folder=$4

db_folder=$5

echo Restoring backup from $backup_folder to $db_folder ....

cat $pl_ns | parallel -j+100 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{} "rm -rf $db_folder \; cp -r  $backup_folder $db_folder"
echo Done!
