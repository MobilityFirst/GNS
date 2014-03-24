nameActivesLocal=$1
nameActivesRemote=$2


echo "Copying nameActives file "$nameActivesLocal"  ..."
cat pl_ns | parallel -j+100 scp -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 $nameActivesLocal ec2-user@{}:$nameActivesRemote".gz"

echo "Unzipping nameActives file "$nameActivesRemote"  ..."
cat pl_ns | parallel -j+100 ssh -i auspice.pem -oStrictHostKeyChecking=no -oConnectTimeout=60 ec2-user@{} "gzip -f -d $nameActivesRemote.gz"

echo "Name actives copy complete."
