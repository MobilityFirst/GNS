[![Build Status](https://travis-ci.org/MobilityFirst/GNS.svg?branch=master)](https://travis-ci.org/MobilityFirst/GNS)
# GNS
### Overview

[![Join the chat at https://gitter.im/MobilityFirst/GNS](https://badges.gitter.im/MobilityFirst/GNS.svg)](https://gitter.im/MobilityFirst/GNS?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
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


1. MySQL Installation needed:

	a) Install and start mysql on the local node.

	b) Set some root or other username password.

	c) Update that username and password in conf/singleNodeConf/contextServiceConf/dbNodeSetup.txt , 4-5th coloumn

2. Start context service.

	From the GNS top level diectory ./scripts/contextServiceScripts/runCSLocal.sh to start context service.

3. Start GNS 

	a) set enableContextService flag in scripts/3nodeslocal/ns.properties
   	add these two lines in ns.properties
   

   	enableContextService = true


   	contextServiceHostPort = 127.0.0.1:8000


	b) Buld GNS, ant jar


	c) Start GNS ./scripts/3nodeslocal/reset_and_restart.sh


4. Run ant test to check if context service is working correctly.

   	test_811_contextServiceTest checks for context service.

5. Details about context service config files.

   	a)  Context service config files are in conf/singleNodeConf/contextServiceConf directory

	b) attributeInfo.txt denotes the attributes that are supported for search and udpates.

	c) contextServiceNodeSetup.txt specifies the node setup and ip port information.


   	d) dbNodeSetup.txt describes the db access information.


   	e) csConfigFile.txt is used internally by context service.
