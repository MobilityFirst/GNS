HEAD=`dirname $0` 

# use jars from default location if available
FILESET=`ls $HEAD/../jars/*.jar 2>/dev/null`
DEFAULT_GP_CLASSPATH=`echo $FILESET|sed -E s/"[ ]+"/:/g`
# developers can use quick build 
DEV_MODE=0
if [[ $DEV_MODE == 1 ]]; then
# Use binaries before jar if available. Convenient to use with
# automatic building in IDEs.
  DEFAULT_GP_CLASSPATH=$HEAD/../build/classes:$DEFAULT_GP_CLASSPATH
  ENABLE_ASSERTS="-ea"
fi

# Wipe out any existing classpath, otherwise remote installations will
# copy unnecessary jars. Set default classpath to jars in ../jars by
# default. It is important to ensure that ../jars does not have
# unnecessary jars to avoid needless copying in remote installs.
export CLASSPATH=$DEFAULT_GP_CLASSPATH

CONF=conf

function set_default_conf {
  default=$1
  if [[ ! -e $defaul && -e $CONF/$default ]]; then
    echo $CONF/$default
  else
    echo $default
  fi 
}

# default java.util.logging.config.file
DEFAULT_LOG_PROPERTIES=logging.properties
LOG_PROPERTIES=$(set_default_conf $DEFAULT_LOG_PROPERTIES)

# default log4j properties (used by c3p0)
DEFAULT_LOG4J_PROPERTIES=log4j.properties
LOG4J_PROPERTIES=$(set_default_conf $DEFAULT_LOG4J_PROPERTIES)

# default gigapaxos properties
DEFAULT_GP_PROPERTIES=gigapaxos.properties
GP_PROPERTIES=$(set_default_conf $DEFAULT_GP_PROPERTIES)

# 0 to disable
VERBOSE=2

JAVA=java

ACTIVE="active"
RECONFIGURATOR="reconfigurator"
APP_ARGS_KEY="appArgs"
APP_RESOURCES_KEY="appResourcePath"

DEFAULT_APP_RESOURCES=app_resources

DEFAULT_KEYSTORE_PASSWORD="qwerty"
DEFAULT_TRUSTSTORE_PASSWORD="qwerty"
DEFAULT_KEYSTORE=keyStore.jks
DEFAULT_TRUSTSTORE=trustStore.jks
keyStore=$(set_default_conf $DEFAULT_KEYSTORE)
trustStore=$(set_default_conf $DEFAULT_TRUSTSTORE)

DEFAULT_SSL_OPTIONS="\
-Djavax.net.ssl.keyStorePassword=$DEFAULT_KEYSTORE_PASSWORD \
-Djavax.net.ssl.keyStore=$keyStore \
-Djavax.net.ssl.trustStorePassword=$DEFAULT_TRUSTSTORE_PASSWORD \
-Djavax.net.ssl.trustStore=$trustStore"


# Usage
if [[ -z "$@" || -z `echo "$@"|grep " \(start\|stop\|restart\) "` ]];
then
  echo "Usage: "`dirname $0`/`basename $0`" [JVMARGS] \
[-D$APP_RESOURCES_KEY=APP_RESOURCES_DIR] \
[-D$APP_ARGS_KEY=\"APP_ARGS\"] \
stop|start  all|server_names"
echo "Examples:"
echo "    `dirname $0`/`basename $0` start AR1"
echo "    `dirname $0`/`basename $0` start AR1 AR2 RC1"
echo "    `dirname $0`/`basename $0` start all"
echo "    `dirname $0`/`basename $0` stop AR1 RC1"
echo "    `dirname $0`/`basename $0` stop all"
echo "    `dirname $0`/`basename $0` \
-DgigapaxosConfig=/path/to/gigapaxos.properties start all"
echo "    `dirname $0`/`basename $0` -cp myjars1.jar:myjars2.jar \
-DgigapaxosConfig=/path/to/gigapaxos.properties \
-D$APP_RESOURCES_KEY=/path/to/app/resources/dir/ \
-D$APP_ARGS_KEY=\"-opt1=val1 -flag2 \
-str3=\\""\"quoted arg example\\""\" -n 50\" \
 start all" 
fi

# remove classpath
ARGS_EXCEPT_CLASSPATH=`echo "$@"|\
sed -E s/"\-(cp|classpath) [ ]*[^ ]* "/" "/g`

# set JVM args except classpath
JVMARGS="$JVMARGS `echo $ARGS_EXCEPT_CLASSPATH|\
sed s/-$APP_ARGS_KEY.*$//g|\
sed -E s/" (start|stop|restart) .*$"//g`"

# extract classpath in args
ARG_CLASSPATH="`echo "$@"|grep " *\-\(cp\|classpath\) "|\
sed -E s/"^.* *\-(cp|classpath)  *"//g|\
sed s/" .*$"//g`"
# Reconstruct final classpath as the classpath supplied in args plus
# the default gigapaxos classpath. 
if [[ ! -z $ARG_CLASSPATH ]]; then
  CLASSPATH=$ARG_CLASSPATH:$DEFAULT_GP_CLASSPATH
fi

# separate out gigapaxos.properties and appArgs
declare -a args
index=1
start_stop_found=0
for arg in "$@"; do
  if [[ ! -z `echo $arg|grep "\-D.*="` ]]; then
    # JVM args and gigapaxos properties file
    key=`echo $arg|grep "\-D.*="|sed s/-D//g|sed s/=.*//g`
    value=`echo $arg|grep "\-D.*="|sed s/-D//g|sed s/.*=//g`
    if [[ $key == "gigapaxosConfig" ]]; then
      GP_PROPERTIES=$value
    elif [[ ! -z `echo $arg|grep "\-D$APP_RESOURCES_KEY="` ]]; 
    then
      # app args
      APP_RESOURCES="`echo $arg|grep "\-D$APP_RESOURCES_KEY="|\
        sed s/\-D$APP_RESOURCES_KEY=//g`"
    elif [[ ! -z `echo $arg|grep "\-D$APP_ARGS_KEY="` ]]; then
      # app args
      APP_ARGS="`echo $arg|grep "\-D$APP_ARGS_KEY="|\
        sed s/\-D$APP_ARGS_KEY=//g`"
    fi
  elif [[ $arg == "start" || $arg == "stop" || $arg == "restart" ]]; 
  then
    # server names
    args=(${@:$index})
  fi
  index=`expr $index + 1`
done

APP_RESOURCES_SIMPLE=`echo $APP_RESOURCES|sed s/"^.*\/"//g`

# get APPLICATION from gigapaxos.properties file
APP=`grep "^[ \t]*APPLICATION[ \t]*=" $GP_PROPERTIES|sed s/^.*=//g`
  if [[ $APP == "" ]]; then
    # default app
    APP="edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp"
  fi
APP_SIMPLE=`echo $APP|sed s/".*\."//g`

# Can add more JVM args here. JVM args specified on the command-line
# will override defaults.
DEFAULT_JVMARGS="$ENABLE_ASSERTS -cp $CLASSPATH \
-Djava.util.logging.config.file=$LOG_PROPERTIES \
-Dlog4j.configuration=log4j.properties \
-DgigapaxosConfig=$GP_PROPERTIES"

JVMARGS="$DEFAULT_JVMARGS $JVMARGS"

# set servers to start here
if [[ ${args[1]} == "all" ]]; then

  # get reconfigurators
    reconfigurators=`cat $GP_PROPERTIES|grep -v "^[ \t]*#"|\
      grep "^[ \t]*$RECONFIGURATOR"|\
      sed s/"^.*$RECONFIGURATOR."//g|sed s/"=.*$"//g`
  
  # get actives
	actives=`cat $GP_PROPERTIES|grep -v "^[ \t]*#"|\
      grep "^[ \t]*$ACTIVE"| sed \ s/"^.*$ACTIVE."//g|\
      sed s/"=.*$"//g`
  
  servers="$actives $reconfigurators"

echo $servers
  
else 
  servers="${args[@]:1}"

fi

function get_value {
  key=$1
  cmdline_args=$2
  default_value_container=$3
  record=`echo $cmdline_args|grep $key`
  if [[ -z $record ]]; then
	record=`cat $GP_PROPERTIES|grep -v "^[ \t]*#"|grep $key`
  fi
  if [[ -z $record ]]; then
    record=$default_value_container
  fi
  if [[ ! -z `echo $record|grep "="` ]]; then
    value=`echo $record| sed s/"^.*$key="//g|\
      sed s/" .*$"//g`
  else 
    value=$default_value_container
  fi
}

function print {
  level=$1
  msg=$2
  if [[ $VERBOSE -ge $level ]]; then
    i=0
    while [[ $i -lt $level ]]; do
      echo -n "  "
      i=`expr $i + 1`
    done
    echo $msg
  fi
  if [[ $level == 9 ]]; then
    exit
  fi
}

# files for remote transfers if needed
function get_file_list {
  cmdline_args=$@
  jar_files="`echo $CLASSPATH|sed s/":"/" "/g`"
  get_value "javax.net.ssl.keyStore" "$cmdline_args" "$keyStore"
  keyStore=$value
  get_value "javax.net.ssl.keyStorePassword" "$cmdline_args" \
    "$DEFAULT_KEYSTORE_PASSWORD"
  keyStorePassword=$value
  get_value "javax.net.ssl.trustStore" "$cmdline_args" "$trustStore"
  trustStore=$value
  get_value "javax.net.ssl.trustStorePassword" "$cmdline_args" \
    "$DEFAULT_TRUSTSTORE_PASSWORD"
  trustStorePassword=$value
  get_value "java.util.logging.config.file" "$cmdline_args" $LOG_PROPERTIES
  LOG_PROPERTIES=$value
  get_value "java.util.logging.config.file" "$cmdline_args" $LOG4J_PROPERTIES
  LOG4J_PROPERTIES=$value

  conf_transferrables="$GP_PROPERTIES $keyStore $trustStore $LOG_PROPERTIES\
     $LOG4J_PROPERTIES $APP_RESOURCES"
  print 3 "transferrables="$jar_files $conf_transferrables
}

# trims conf_transferrables only to files that exist
function trim_file_list {
  list=$1
  for i in $list; do
    if [[ -e $i ]]; then
      existent="$existent $i"
    fi
  done
  conf_transferrables=$existent
}


get_file_list "$@"
trim_file_list "$conf_transferrables"

# disabling warnings to prevent manual override; can supply ssh keys
# here if needed, but they must be the same on the local host and on
# the first host that continues the installation.
SSH="ssh -x -o StrictHostKeyChecking=no"

RSYNC_PATH="mkdir -p $APP_SIMPLE"
RSYNC="rsync --force -a "

username=`grep "USERNAME=" $GP_PROPERTIES|grep -v "^[ \t]*#"|\
  sed s/"^[ \t]*USERNAME="//g`
if [[ -z $username ]]; then
  username=`whoami`
fi

function append_to_ln_cmd {
  src_file=$1
  default=$2
  simple=`echo $1|sed s/".*\/"//g`
  
  if [[ -e $1 ]]; then
    if [[ -z $LINK_CMD ]]; then
      LINK_CMD="ln -fs ~/$APP_SIMPLE/conf/$simple \
        $APP_SIMPLE/$default "
    else
      LINK_CMD="$LINK_CMD; ln -fs ~/$APP_SIMPLE/conf/$simple \
        $APP_SIMPLE/$default "
    fi
  fi
}

append_to_ln_cmd $GP_PROPERTIES $DEFAULT_GP_PROPERTIES
append_to_ln_cmd $keyStore $DEFAULT_KEYSTORE
append_to_ln_cmd $trustStore $DEFAULT_TRUSTSTORE
append_to_ln_cmd $LOG_PROPERTIES $DEFAULT_LOG_PROPERTIES
append_to_ln_cmd $LOG4J_PROPERTIES $DEFAULT_LOG4J_PROPERTIES
append_to_ln_cmd $APP_RESOURCES $DEFAULT_APP_RESOURCES

function rsync_symlink {
  address=$1
  print 1 "Transferring conf files to $address:$APP_SIMPLE"
  print 2 "$RSYNC --rsync-path=\"$RSYNC_PATH; $LINK_CMD && rsync\" \
    $conf_transferrables $username@$address:$APP_SIMPLE/conf/"
  $RSYNC --rsync-path="$RSYNC_PATH; $LINK_CMD && rsync" \
    $conf_transferrables $username@$address:$APP_SIMPLE/conf/
}

SSL_OPTIONS="-Djavax.net.ssl.keyStorePassword=$keyStorePassword \
-Djavax.net.ssl.keyStore=$keyStore \
-Djavax.net.ssl.trustStorePassword=$trustStorePassword \
-Djavax.net.ssl.trustStore=$trustStore"

LOG_FILE=gigapaxos.log

# gets address and port of server from gigapaxos properties file
function get_address_port {
  server=$1
  # for verbose printing
  addressport=`grep "\($ACTIVE\|$RECONFIGURATOR\).$server=" \
    $GP_PROPERTIES| grep -v "^[ \t]*#"|\
    sed s/"^[ \t]*$ACTIVE.$server="//g|\
    sed s/"^[ \t]*$RECONFIGURATOR.$server="//g`
  address=`echo $addressport|sed s/:.*//g`
  if [[ $addressport == "" ]]; then
    non_existent="$server $non_existent"
    return
  fi
  # check if interface is local
  ifconfig_found=`type ifconfig 2>/dev/null`
}

# start server if local, else append to non_local list
function start_server {
  server=$1
  get_address_port $server
  if [[ $ifconfig_found != "" && `ifconfig|grep $address` != "" ]]; then
    if [[ $VERBOSE == 2 ]]; then
      echo "$JAVA $JVMARGS $SSL_OPTIONS \
        edu.umass.cs.reconfiguration.ReconfigurableNode $server&"
    fi
    $JAVA $JVMARGS $SSL_OPTIONS \
      edu.umass.cs.reconfiguration.ReconfigurableNode $server&
  else
    # first rsync files to remote server
    non_local="$server=$addressport $non_local"
    echo "Starting remote server $server"
    print 1 "Transferring jar files $jar_files to $address:$APP_SIMPLE"
    print 2 "$RSYNC --rsync-path=\"$RSYNC_PATH && rsync\" \
      $jar_files $username@$address:$APP_SIMPLE/jars/ "
    $RSYNC --rsync-path="$RSYNC_PATH && rsync" \
      $jar_files $username@$address:$APP_SIMPLE/jars/ 
    rsync_symlink $address

    # then start remote server
    print 2 "$SSH $username@$address \"cd $APP_SIMPLE; nohup \
      $JAVA $JVMARGS $SSL_OPTIONS \
      -DgigapaxosConfig=gigapaxos.properties \
      edu.umass.cs.reconfiguration.ReconfigurableNode \
      $APP_ARGS $server \""
    
    $SSH $username@$address "cd $APP_SIMPLE; nohup \
      $JAVA $JVMARGS $SSL_OPTIONS \
      -DgigapaxosConfig=gigapaxos.properties \
      edu.umass.cs.reconfiguration.ReconfigurableNode \
      $APP_ARGS $server "&
  fi
}

function start_servers {
if [[ $servers != "" ]]; then
  # print app and app args
  if [[ $APP_ARGS != "" ]]; then
    echo "[$APP $APP_ARGS]"
  else
    echo "[$APP]"
  fi
  # start servers
  for server in $servers; do
    start_server $server
  done
  if [[ $non_local != "" && $VERBOSE != 0 ]]; then
    echo "Ignoring non-local server(s) \" $non_local\""
  fi
fi
}

function stop_servers {
  for i in $servers; do
      get_address_port $i
      KILL_TARGET="ReconfigurableNode .*$i"
      if [[ ! -z $ifconfig_found && `ifconfig|grep $address` != "" ]]; 
      then
        pid=`ps -ef|grep "$KILL_TARGET"|grep -v grep|\
          awk '{print $2}' 2>/dev/null`
        if [[ $pid != "" ]]; then
          foundservers="$i($pid) $foundservers"
          pids="$pids $pid"
        fi
      else 
        # remote kill
        echo "Stopping remote server $server"
        echo $SSH $username@$address "\"kill -9 \`ps -ef|\
          grep \"$KILL_TARGET\"|grep -v grep|awk \
          '{print \$2}'\` 2>/dev/null\""
        $SSH $username@$address "kill -9 \`ps -ef|\
          grep \"$KILL_TARGET\"|grep -v grep|awk \
          '{print \$2}'\` 2>/dev/null"
      fi
  done
  if [[ `echo $pids|sed s/" *"//g` != "" ]]; then
    echo killing $foundservers
    kill -9 $pids 2>/dev/null
  fi
}

case ${args[0]} in

start)
  start_servers
;;

restart)
    stop_servers
    start_servers
;;

stop)
  stop_servers

esac
