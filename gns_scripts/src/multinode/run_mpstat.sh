
hosts_file=$1
remote_folder=$2

echo "Creating remote dirs ..."
cat $hosts_file | parallel -j+100 ssh -i auspice.pem ec2-user@{} "mkdir -p $remote_folder/{}"

echo "Running mpstat ..."
cat $hosts_file | parallel ssh -i auspice.pem ec2-user@{} "nohup mpstat 10 \> $remote_folder/{}/cpuUsage 2\> $remote_folder/{}/cpuUsageErr \< /dev/null &"

echo "Done."
