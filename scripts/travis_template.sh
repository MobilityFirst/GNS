#!/usr/bin/env bash

echo 0 > "successes"
echo 0 > "failures"

i=0
while true; do
	
COMMAND_HERE
err_status=$?

successes=`cat successes`
failures=`cat failures`
if [[ $err_status == 0 ]]; then
  successes=`expr $successes + 1`
else 
  failures=`expr $failures + 1`
fi
i=`expr $i + 1`
echo $successes > "successes"
echo $failures > "failures"
echo "===Iteration $i, successes=$successes, failures=$failures==="
done &

echo "Background monitoring started at `date`.."
# Travis gives 49 minutes exactly. ant takes ~1:30. 30 minutes buffer. So, 47 minutes = 2820 seconds.
sleep 2820

successes=`cat successes`
failures=`cat failures`
echo "=== Summary: successes=$successes, failures=$failures ==="
if [ $successes -eq 0 ] && [ $failures -eq 0 ]; then
	echo "No progress, something went wrong.."
	exit 1
elif [ $failures -gt 0 ]; then
	echo "There are failures, exiting with 1.."
	exit 1
else
	echo "All good, exiting with 0"
	exit 0
fi
