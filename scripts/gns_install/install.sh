#!/bin/bash


tool=$1
download=$2
path=$3

: '
Sample Download Path 

MongoDB: http://downloads.mongodb.org/linux/mongodb-linux-i686-2.6.1.tgz
Java: http://download.oracle.com/otn-pub/java/jdk/7u55-b13/jdk-7u55-linux-i586.tar.gz

'

if [[ $tool == 'mongodb' ]]
	then
	version=$(mongo -version)
elif [[ $tool == 'jdk' ]]
	then
	version=$(java -version 2>&1 | head -n 1 | cut -d\" -f 2)
	jcookie='--header  "Cookie: oraclelicense=accept-securebackup-cookie"  '
else
	echo "Unable to identify the tool to be installed"
	exit 1
fi


if [ -z "$version" ]
 then
 	cd $path  #navigating to desired directory
 	echo "wget" $download
	wget --no-check-certificate --no-cookies $jcookie $download 
	if [ $? -eq 0 ]
	then
  		echo "sucessfully downloaded"
  		echo "tar -xvf" $1*
  		tar -xvf $1*
	else
 			echo $1 "unsuccessfully downloaded"
 			exit 1
	fi
 	else
 		echo $1 "is already installed in the system , version =>" $version
fi


: '
Finally the self-Descruting Myself :(
	rm -f install.sh
	echo "Everything Successfull GoodBye"
'

#-----------------End of Installation Script --------------------------------