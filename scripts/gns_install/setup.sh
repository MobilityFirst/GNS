#!/bin/bash

#Param1: tool name <mongodb,jdk>
parallel_exec() {
	echo "pscp -h host.txt -l" $user "installer.sh" $spath
	pscp -h host.txt -l $user "installer.sh" $spath
	echo "pssh -h host.txt -l" $user "'sh" $spath"/installer.sh " $1 $2 $spath "'"
	pssh -h host.txt -l $user "' sh " $spath"/installer.sh " $1 $2 $spath "'"
}


#Option Parser
while getopts ":j:m:g:" opt; do
  case $opt in
    j)
      echo "-j was triggered, Parameter: $OPTARG" >&2
      oarg=($OPTARG)
      user=${oarg[0]}
      spath=${oarg[1]}
      echo "Java path in remote machine =>" $spath
      parallel_exec "jdk" "download link"
      ;;
    m)
	  echo "-m option is triggered, Parameter: $OPTARG" >&2
	  oarg=($OPTARG)
      user=${oarg[0]}
      spath=${oarg[1]}
      echo "Mongo path in remote machine =>" $spath
	  parallel_exec "mongodb" "download link"
	  ;;
	g)
	  echo "-g option is triggered, Parameter: $OPTARG" >&2
	  gopt=($OPTARG)
	  user=${gopt[0]}
	  gnsfilepath=${gopt[1]}
	  gpath=${gopt[2]}
	  echo $gnsfilepath ", " $gpath
	  echo "pscp -h host.txt -l" $user $gnsfilepath $gpath
	  ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done



