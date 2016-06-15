#
kill -s TERM `ps -ef | grep GNS.jar | grep -v grep | grep -v "context" | awk '{print $2}'`
