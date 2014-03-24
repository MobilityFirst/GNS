hosts=$1
url=$2
filename=$3
remote_folder=$4

cat $hosts | parallel -j+100 ssh -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60  ec2-user@{}  "mkdir -p $remote_folder\; cd $remote_folder\; rm $filename\; wget $url_{}"
