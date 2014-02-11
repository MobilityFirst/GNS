#!/bin/bash 

NODE0=ec2-23-20-23-213.compute-1.amazonaws.com
NODE1=ec2-75-101-240-225.compute-1.amazonaws.com
NODE2=ec2-50-19-36-89.compute-1.amazonaws.com
NODE3=ec2-174-129-172-80.compute-1.amazonaws.com
NODE4=ec2-23-22-101-251.compute-1.amazonaws.com
NODE5=ec2-23-20-255-39.compute-1.amazonaws.com
NODE6=ec2-23-21-9-39.compute-1.amazonaws.com
NODE7=ec2-107-21-171-119.compute-1.amazonaws.com
NODES="$NODE0 $NODE1 $NODE2 $NODE3 $NODE4 $NODE5 $NODE6 $NODE7"

for NODE in $NODES
do
osascript <<END 
tell app "Terminal" to do script "cd Documents/Code/GNRS-westy/scripts/ec2; ssh -i Westy.pem ec2-user@$NODE" 
END
done