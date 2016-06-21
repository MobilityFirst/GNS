#!/bin/bash

HEAD=`dirname $0`

DEV_MODE=1
if [[ $DEV_MODE == 1 ]]; then
  CLASSPATH=$CLASSPATH:$HEAD/../build/classes
  ENABLE_ASSERTS="-ea"
fi

VERBOSE=1

# Use binaries before jar if available. Convenient to use with
# automatic building in IDEs.
CLASSPATH=$CLASSPATH:.:\
`ls $HEAD/../dist/gigapaxos-[0-9].[0-9].jar`

LOG_PROPERTIES=logging.properties
GP_PROPERTIES=gigapaxos.properties

ACTIVE="active"
RECONFIGURATOR="reconfigurator"

SSL_OPTIONS="-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/keyStore/node100.jks"

# remove classpath
ARGS_EXCEPT_CLASSPATH=`echo "$@"|\
sed -E s/"\-(cp|classpath) [ ]*[^ ]* "/" "/g`

# reconstruct classpath by adding supplied classpath
CLASSPATH="`echo "$@"|grep " *\-\(cp\|classpath\) "|\
sed -E s/"^.* *\-(cp|classpath)  *"//g|\
sed s/" .*$"//g`:$CLASSPATH"

DEFAULT_CLIENT_ARGS=`echo "$@"|sed s/".*-D[^ ]* "//g`

# separate out JVM args
declare -a args
index=0
for arg in $ARGS_EXCEPT_CLASSPATH; do
  if [[ ! -z `echo $arg|grep "\-D.*="` ]]; then
    key=`echo $arg|grep "\-D.*="|sed s/-D//g|sed s/=.*//g`
    value=`echo $arg|grep "\-D.*="|sed s/-D//g|sed s/.*=//g`
    if [[ $key == "gigapaxosConfig" ]]; then
      GP_PROPERTIES=$value
    fi
  else
    args[$index]=$arg
    index=`expr $index + 1`
  fi
done

DEFAULT_JVMARGS="$ENABLE_ASSERTS -cp $CLASSPATH \
-Djava.util.logging.config.file=$LOG_PROPERTIES \
-Dlog4j.configuration=log4j.properties \
-DgigapaxosConfig=$GP_PROPERTIES"

JVM_APP_ARGS="$DEFAULT_JVMARGS `echo $ARGS_EXCEPT_CLASSPATH`"

APP=`cat $GP_PROPERTIES|grep "^[ \t]*APPLICATION="|\
sed s/"^[ \t]*APPLICATION="//g`

# default clients
if [[ $APP == "edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp" && \
-z `echo "$@"|grep edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp` ]];
then
  DEFAULT_CLIENT="edu.umass.cs.gigapaxos.examples.noop.NoopPaxosAppClient \
$DEFAULT_CLIENT_ARGS"
elif [[ $APP == \
"edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp" && \
-z `echo "$@"|grep edu.umass.cs.gigapaxos.examples.noop.NoopApp` ]];
then
  DEFAULT_CLIENT="edu.umass.cs.reconfiguration.examples.NoopAppClient \
$DEFAULT_CLIENT_ARGS"
fi

echo "java $SSL_OPTIONS $JVM_APP_ARGS" 

java $SSL_OPTIONS $JVM_APP_ARGS $DEFAULT_CLIENT
