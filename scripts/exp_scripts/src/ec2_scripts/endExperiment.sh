echo "Kill name servers and local name servers ..."
output_folder=$1
log_folder=$2
./killJava.sh&
#./killPython.sh &
echo "Copy all logs ..."
./getLog.sh $output_folder $log_folder
