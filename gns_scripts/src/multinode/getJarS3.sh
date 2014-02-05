
user=$1
ssh_key=$2
pl_ns=$3
pl_lns=$4

# url of jar file is hardcoded
# location of jar file is hard coded

echo "Removing earlier jar ..."
cat $pl_ns $pl_lns | parallel -j+20 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{} "rm -rf GNS.jar"
echo "Downloading new jar ..."
cat $pl_ns $pl_lns | parallel -j+20 ssh -i $ssh_key -oStrictHostKeyChecking=no -oConnectTimeout=60 $user@{} "wget https://s3.amazonaws.com/auspice-gns/GNS.jar > /dev/null 2> /dev/null"
