
hosts_file=$1
remote_folder=$2
local_folder=$3

echo "Killing mpstat ..."
cat $hosts_file | parallel ssh -i auspice.pem ec2-user@{} "killall -9 mpstat"

rm -rf $local_folder
mkdir -p $local_folder

echo "Copy files ..."
cat $hosts_file | parallel scp -r -i auspice.pem ec2-user@{}:$remote_folder/{} $local_folder/{}

echo "Done."
