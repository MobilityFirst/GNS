#!/usr/bin/env bash

rm -rf *.o 
rm -rf ./edu 
rm -rf ./org
~/Downloads/j2objc/dist/j2objc --build-closure -d . -sourcepath ../src:../../gig_ios/src -classpath /Users/kanantharamu/gig_ios/lib/json-smart-1.2.jar ../src/edu/umass/cs/gnsclient/client/GNSClient.java /Users/kanantharamu/gig_ios/src/sun/misc/Cleaner.java #`find /Users/kanantharamu/gig_ios/src/edu/umass/cs/reconfiguration/interfaces -name "*.java"` `find /Users/kanantharamu/gig_ios/src/edu/umass/cs/gigapaxos/interfaces -name "*.java"`
read -rsp $'Implement IOSKeyStore and Press any key to continue ...\n' -n1 key
~/Downloads/j2objc/dist/j2objcc -c -I. -I ~/json-smart-v1/json-smart/build  `find . -name "*.m"`

rm libgnsclient.a
rm GNSClient.o 
rm JSONObject.o 
rm JSONArray.o 
rm JSONException.o 
rm JSONTokener.o
ar -r libgnsclient.a *.o
ls -l
#rm *.o
~/Downloads/j2objc/dist/j2objcc -ObjC -o test -g -I. -l jre_emul -l junit libgnsclient.a ~/gig_ios/ios/libs/libjsonsmart.a edu/umass/cs/gnsclient/client/GNSClient.m