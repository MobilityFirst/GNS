#!/bin/bash

: '
Sample Download Path 
MongoDB: http://downloads.mongodb.org/linux/mongodb-linux-i686-2.6.1.tgz
Java: http://download.oracle.com/otn-pub/java/jdk/7u55-b13/jdk-7u55-linux-i586.tar.gz
'

tool=$1
download=$2
path=$3


#Returns 0 if file exists else returns 1
test_exist() {
	if [ -f $1 ]
	then 
		echo "0"
	else
		echo "1"
	fi
}

#Assuming the bashrc to be in the username home directory
#If .bashrc is not present, then its created and made source.

set_path() {
	PATH=$PATH:$1
	echo "the new PATH is " $PATH
	result=$(test_exist ~/.bashrc)
	if [ $result -eq 0 ]
		then
		sed -i '/export PATH/d' ~/.bashrc
	fi
	echo "export PATH="$PATH >> ~/.bashrc
	source ~/.bashrc
}




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
  		#find . -type f -name "$tool*gz" -print | xargs tar -xvf
  		find . -maxdepth 1 -name $tool\*gz -exec tar -zxvf {} \;
  		#echo "tar -xvf" $tool*\.(tgz|tar\.gz)
  		#tar -xvf $1*
  		dname=`ls -l | grep '^d' | grep -E $tool*[1..9] | awk '{print $9}'`
  		set_path $path/$dname/bin  #Need to test the parameter passing
	else
 			echo $1 "unsuccessfully downloaded"
 			exit 1
	fi
 	else
 		echo $1 "is already installed in the system , version =>" $version
fi

#: 
# Finally the self-Descruting Myself :(
rm -f install.sh
#	echo "Everything Successfull GoodBye"




#-----------------End of Installation Script --------------------------------