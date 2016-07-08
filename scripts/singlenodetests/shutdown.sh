#
kill -s TERM `ps -ef | grep GNS.jar | grep -v ServerIntegrationTestgrep -v grep | grep -v "context" | awk '{print $2}'`
