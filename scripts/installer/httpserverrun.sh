#!/bin/bash
nohup java -jar httpserver.jar -lnsid $1 -nsfile name-server-info  > logfile 2>&1 &
