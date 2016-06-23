---
title: Installation
keywords: installation
last_updated: June 23, 2016
sidebar: doc_sidebar
permalink: /install/
folder: doc
---
## Prerequisites

The GNS requires the following when using a binary:

* Java 8 JRE
* MongoDB v2.4.9 or the latest compatible version


The GNS requires the following when building from the source:

* Java 8 JDK + JRE
* Ant
* MongoDB v2.4.9 or the latest compatible version


You should be able to find all dependencies in your favorite package manager.

### Ubuntu and derivatives
```
sudo apt-get install mongodb ant openjdk-8-jdk 
```

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
