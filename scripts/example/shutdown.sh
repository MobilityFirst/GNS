#
kill -s TERM `ps -ef | grep GNS.jar | grep -v grep | awk '{print $2}'`