import os, sys, time
cmd = 'pkill -9 -f context-nodoc-GNS.jar'
os.system(cmd)
print "All context service instances stopped"
