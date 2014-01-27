hosts=$1
nameActivesRemote=$2

echo "Unzipping nameActives file "$nameActivesRemote"  ..."
cat $hosts | parallel -j+100 ssh -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 ec2-user@{} "gzip -f -d $nameActivesRemote.gz"

echo "Name actives copy complete. " $hosts
