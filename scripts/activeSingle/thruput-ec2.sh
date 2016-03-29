#!/bin/bash
ip=$1
guid=$2
benign=$3
rate=$4
active=$5

if [ ! -d "result" ]; then
mkdir result
fi

echo "Create $2 guids with $3 benign guids on server $1, and send requests at the initial rate of $4 reqs/sec for each guid"

./scripts/client/runEC2Client edu.umass.cs.gnsclient.examples.CreateMultiGuidClient $ip 0 $guid $benign $active 2> /tmp/log

for i in {1..20}
do
echo "send the requests at the rate $(($i*$2*$4))"
./scripts/client/runEC2Client edu.umass.cs.gnsclient.examples.CapacityTestClient $ip 0 $guid $(($i*$rate)) $benign &> result/output-$guid-$benign-$(($i*$rate))
cat result/output-$guid-$benign-$(($i*$rate)) | grep "The average latency is"
done

echo ""

python process_result.py $guid $benign $rate
