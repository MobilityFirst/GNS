#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
$SCRIPTS/reset-server.sh
$SCRIPTS/run-debug.sh
