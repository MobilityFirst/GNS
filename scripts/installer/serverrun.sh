#!/bin/bash
nohup java -jar $1 -planetlab -id $2 -nsfile name-server-info  > logfile 2>&1 &
