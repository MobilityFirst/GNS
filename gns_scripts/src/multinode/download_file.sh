user=$1
ssh_key=$2
hosts=$3
url=$4
filename=$5
remote_folder=$6
echo Downloading file $filename fromm $url at $remote_folder ....

cat $hosts | parallel -j+10 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=500  $user@{}  "mkdir -p $remote_folder\; cd $remote_folder\; rm $filename\; wget $url > /dev/null 2>/dev/null"
echo 'Downloaded.'