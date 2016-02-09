#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
#echo $SCRIPTS
$SCRIPTS/reset-server.sh
$SCRIPTS/run-nossl-8servers-norepall-debug.sh