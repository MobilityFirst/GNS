# GNS
### Overview
The GNS (Global Name Service) is a logically centralized server that maintains the list of mappings from keys to
values.
As a database, the GNS shares some semantics with a [NoSQL database](http://en.wikipedia.org/wiki/NoSQL) with 
each document (a key value container) having a default index which is a Globally Unique Identifier (GUID).

A GUID is similar in spirit of [Host Identity Protocol](http://en.wikipedia.org/wiki/Host_Identity_Protocol) and
is a self-certifying public key with an associated human readable name.
In the GNS a GUID is a 160bit self-certifying identifier derived from a public / private key pair - specifically
it is the hash of the public key. 
GUIDs also have an associated human readable name sometimes referred to as its alias.

Certain GUIDs, called account GUIDs have an alias which must be an email address. Upon creation the 
email address is used to authenticate the account GUID.
Account GUIDs can be used by the GUID owner to create additional GUIDs. 
All GUIDs can also have additional associated human readable names (alias) by which they can be referenced.

### How Can We Use the GNS?

The GNS can be a replacement for the [Domain Name System](http://en.wikipedia.org/wiki/Domain_Name_System) (DNS). 
Similar to GNS, DNS associates various information with user friendly names. Looking up IP addresses
used to locate computer services and devices is the prominent example. Unfortunately, DNS conflates naming 
with location and also is subject to a variety of [security](http://en.wikipedia.org/wiki/Domain_Name_System#Security_issues) 
and other issues. 

A GNS based lookup service would keep the location and name of the device or service separate. 
It would also provide protection against spoofing of names because GUIDs are protected by the use of the private/public key.

GNS enabled routing would be able to find any device or service anywhere (if it wanted to be found). A GNS would replace the
current mix of NAT's, cell networks, VPNs and all the infrastructure that has 
been built to graft mobility and security on top of the internet. 
The GNS would also deal with the issue of dwindling IP addresses.

If a GNS GUID were associated with every piece of content on the internet, finding, storing and distributing content could be simplified and made more efficient. One example of this is content storing routers exploiting less expensive storage and keeping
data close to the place it is used.

Above from from https://gns.name/wiki/index.php?title=GNS_Overview


### Starting local context service

1. Details about context service config files

   a) Context service config files are in conf/contextServiceConf directory

   b) contextServiceNodeSetup.txt files has context service node ip port information. Currently, even locally context service starts four 
      nodes, needed for hyperspace hashing. First column in the file is nodeID of context service node, second is IP address and third is 
      port. These ports should not conflict with local GNS ports.

   c) dbNodeSetup.txt file has database connection information. It has database infomation for each context service node, which is 4 in this
      setup. Column names are specified in the file. If a non standard port mysql is running then that port can be changed in the file.
      Username and password should be updated according to the mysql credentials. If a mysql is running without root, in a local directory 
      then those options can be specified as 6th argument, but not need for system and default installation of mysql.

   d) attributeInfo.txt contains attribute information. Currently, seach and updates can only support attributes specified in this file. 
      Seach query have a "GeojsonOverlap" function, which takes a geoJSON as input and internally converts that into these attributes, so 
      these attributes are not directly visible in geoJSON. This files also contains minimum value, maximum value, default value and data 
      type of each attribute. Any update to an attribute's value should be between minimum and maximum value specified. Minimum, maximum, 
      default values can be changed in this file and system will incorporate that on next run.

   e) subspaceInfo.txt file contains the subspace info. Most of the time this file will not require any changes. Each subspace is defned by 
      two lines in thsi file. Currently, there is just one subspace so just two lines. First line denotes the "subspaceID, nodeid1, 
      nodeid2, ...". It has subspace id and node ids on which that subspace is defined. Format of second line is "subpsaceId, attribute1, 
      attribute2, ...", subspace id and attributes of that subpsace.

2. MySQL Installation needed:

   a) Install and start mysql on the local node.

   b) Set some root or other username password.

   c) Update that password in conf/contextServiceConf/dbNodeSetup.txt , 4-5th coloumn

3. Start context service.

   a) From the GNS top level diectory
      python ./scripts/contextServiceScripts/StartContextServiceGNS.py
      to start context service.

   b) Its output goes in nohup.out in the GNS top level directory.

   c) ./scripts/contextServiceScripts directory also contains context-nodoc-GNS.jar that is needed to start context service. This jar is not 
      included in GNS lib jars.

4. a) Start GNS using scripts in ./scripts/**

   b) Start context service as described in 3.
 
5. Running a sample test.

   a) A sample junit test for the local setup is in test folder in edu.umass.cs.contextservice.test package, "UpdateSearchTest.java". If this 
      test passes then context service is working.

6. Context service can be enabled or disabled by setting this option in ./scripts/singlenodetests/ns_nossl.properties. Currently it is only 
   set in ./scripts/singlenodetests/ns_nossl.properties file.

7. Currently I have tested with ./scripts/singlenodetests/nossl_reset_and_restart.sh Some other things might need to be set for other GNS 
   setups.

##TODOs
1. I have added support for update forwarding for "SINGLE_FIELD_CREATE" update types, in fucntion sendTriggerToContextService of Update.java file. But support for other types of updates needs to be added.

