user=$1
ssh_key=$2
hosts_file=$3
remote_folder=$4

echo "Creating remote dirs ..."
cat $hosts_file | parallel -j+100 ssh -i $ssh_key $user@{} "mkdir -p $remote_folder/{}"

echo "Running mpstat ..."
cat $hosts_file | parallel ssh -i  $ssh_key  $user@{} "nohup mpstat 10 \> $remote_folder/{}/cpuUsage 2\> $remote_folder/{}/cpuUsageErr \< /dev/null &"

echo "Done."
