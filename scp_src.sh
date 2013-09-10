ssh abhigyan@skuld.cs.umass.edu "rm -rf gnrs/GNS/src gnrs/GNS/dist/GNS.jar gnrs/GNS.jar"
tar -czf src.tgz src
scp src.tgz abhigyan@skuld.cs.umass.edu:gnrs/GNS
ssh abhigyan@skuld.cs.umass.edu "cd gnrs/GNS/; tar -xzf src.tgz; ant all; cp dist/GNS.jar .."
