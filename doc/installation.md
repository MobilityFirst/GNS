---
title: Installation
keywords: installation
last_updated: June 21, 2016
sidebar: doc_sidebar
permalink: /installation/
folder: doc
---
## Prerequisites

Downloading, running or compiling the GNS server and client require one or more of the following:

* Git
* Java 8
* Ant
* MongoDB v2.4.9 or the latest compatible version
* GNS source code or binary
* A machine with a reachable IP address so that GNS clients can connect. The server typically uses ports in the 20000s, but these are configurable in the gigapaxos.properties file.

You should be able to find all dependencies in your favorite package manager.

### Git

Git is required if you're going to be downloading the source code.

This is probably your best link for getting git: https://git-scm.com/book/en/v2/Getting-Started-Installing-Git

### Java and Ant Requirement

GNS requires Java 8 or higher. Only the Java Runtime Environment (JRE) is required to run a GNS server or client, but for compiling sources you will need the Java Development Kit (JDK).

Ant is required only if you are compiling from sources. It is available as a binary download from http://ant.apache.org. You can also install it using most of the installation packages like RPM, Yum, appget, homebrew, etc.

### Mongo
MongoDB is required for running a GNS server (not for a GNS client). The MongoDB installation instructions are easy to follow: http://docs.mongodb.org/manual/installation OSX Users: The homebrew install on OSX is also straightforward for MongoDB. 


## Compiling
Use git to clone the GNS repo

```
git clone https://github.com/MobilityFirst/GNS.git
```

Compile the project

```
cd GNS
ant jar
```
