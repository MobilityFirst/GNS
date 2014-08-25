

yum --quiet --assumeyes update
yum --quiet --assumeyes install java-1.7.0-openjdk
# if you want to use ant
JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.65.x86_64
export JAVA_HOME
#mongo
echo "[MongoDB]
name=MongoDB Repository
baseurl=http://downloads-distro.mongodb.org/repo/redhat/os/x86_64
gpgcheck=0
enabled=1" > mongodb.repo
mv mongodb.repo /etc/yum.repos.d/mongodb.repo
yum --quiet --assumeyes install mongodb-org
service mongod start
sudo yum -y install ant
sudo yum -y install subversion
sudo yum -y install emacs

svn checkout svn+ssh://none.cs.umass.edu/svn/mobility-first/GNS/trunk GNS

cd GNS

ant

# change the hostname and port in ../conf/singleNStest/node_config_1ns_1lns

emacs ../conf/singleNStest/node_config_1ns_1lns

