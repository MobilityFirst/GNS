#!/bin/bash

NODE0=54.243.196.158
NODE1=54.245.116.194
NODE2=54.247.72.211
NODE3=54.251.32.133
NODE4=54.248.86.81
NODES=($NODE0 $NODE1 $NODE2 $NODE3 $NODE4)

PEM0="WestyUMass.pem"
PEM1="WestyOregon.pem"
PEM2="WestyIreland.pem"
PEM3="WestySingapore.pem"
PEM4="WestyTokyo.pem"
PEMS=($PEM0 $PEM1 $PEM2 $PEM3 $PEM4)

for ((i = 0; i < 5; i++))
  do
  scp -i ${PEMS[i]} install.sh ec2-user@${NODES[i]}:/home/ec2-user
done
