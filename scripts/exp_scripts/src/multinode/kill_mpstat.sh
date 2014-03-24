user=$1
ssh_key=$2
hosts_file=$3
remote_folder=$4
local_folder=$5

echo "Killing mpstat ..."
cat $hosts_file | parallel ssh -i $ssh_key $user@{} "killall -9 mpstat"

rm -rf $local_folder
mkdir -p $local_folder

echo "Copy files ..."
cat $hosts_file | parallel scp -r -i $ssh_key $user@{}:$remote_folder/{} $local_folder/{}

echo "Done."
