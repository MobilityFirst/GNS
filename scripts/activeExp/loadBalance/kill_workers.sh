#!/bin/bash
kill $(ps aux | grep 'edu.umass.cs.gnsserver.activecode.worker.ActiveCodeWorker' | awk '{print $2}')
