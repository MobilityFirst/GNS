user=$1
ssh_key=$2
hosts=$3
nameActivesRemote=$4

echo "Unzipping nameActives file "$nameActivesRemote"  ..."
cat $hosts | parallel -j+100 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{} "gzip -f -d $nameActivesRemote.gz"

echo "Name actives copy complete. " $hosts
