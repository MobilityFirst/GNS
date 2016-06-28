---
title: "Getting Started"
keywords: installation
last_updated: June 23, 2016
sidebar: documentation_sidebar
permalink: /server_getting_started/
toc: true
---

Prerequisites: `JRE1.8+`, `bash`, `mongodb` (optional).

### Obtaining GNS
Download the latest, stable, binary-only package from the [GNS releases page](https://github.com/MobilityFirst/GNS/releases).

### Single-node, local GNS server
The GNS is a gigapaxos application, which means that gigapaxos is the distributed engine responsible for replicating and reconfiguring GNS replica nodes as needed. The GNS uses mongodb as its underlying database by default, so it is necessary to have mongodb running (on localhost and its default port) before starting the GNS. If mongodb is not already running, refer to the [mongodb installation instructions page](https://docs.mongodb.com/manual/installation/) to set that up (recommended). If you prefer to not use mongodb for now, refer to the instructions further below to run the GNS in the in-memory database mode (not recommended for production use). 

Start a single-node, local GNS server as 

```
bin/gpServer.sh restart all
```

To connect to this GNS server using the command-line interface (CLI), refer to the [CLI introduction page TBD](.).

### Simple client example
To connect to the GNS server above using a Java client, we will use a simple client, [ClientExample](https://github.com/MobilityFirst/GNS/blob/master/src/edu/umass/cs/gnsclient/examples/ClientExample.java) [(doc)](http://mobilityfirst.github.io/GNS/doc/edu/umass/cs/gnsclient/examples/ClientExample.html), that creates a record keyed by a globally unique identifier (GUID), performs a few field reads and writes, and deletes the record. Run this example client as

```
bin/gpClient.sh edu.umass.cs.gnsclient.examples.ClientExample
```

The client's output should be self-explanatory and it should exit gracefully with a success message. If it runs into exceptions, refer to this [troubleshooting TBD](.) page for common possible reasons.

### In-memory database mode
If you prefer to not use mongodb, uncomment or enter `IN_MEMORY_DB=true` in the default configuration properties file, `gigapaxos.properties`. Using the in-memory mode does not affect durability as that is ensured by gigapaxos, however, the database size will be limited by memory, so it is not recommended for production use.

### Stopping GNS server(s)

The GNS server can be stopped as

```
bin/gpServer.sh stop all
```

***

### Configuration properties overview
The gigapaxos server script `gpServer.sh` script above looks for a properties file named `gigapaxos.properties` by default in the current directory and, if not found, in the `./conf/` subdirectory. A detailed explanation of gigapaxos properties is at the [gigapaxos properties TBD](.) page. We just need to observe here that the `APPLICATION` is GNSApp and the file specifies a single _reconfigurator_ and a single _active_ replica, both listening on localhost ports. Both gigapaxos- and GNS-specific properties are specified in the same properties file.

In general, the client and server are expected to use different properties files. The above example involved both the server and client using the same file, `./gigapaxos.properties`, for simplicity. Verify that the behavior is the same as above with the following commands each of which explicitly specifies the corresponding properties file.

```
bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.server.singleNode.local.properties restart all

bin/gpClient.sh -DgigapaxosConfig=conf/gigapaxos.client.singleNode.local.properties edu.umass.cs.gnsclient.examples.ClientExample
```

For safety, many gigapaxos properties, especially node names and addresses, can not be manually changed after bootstrap. So it is best to create a fresh server install in a new directory in order to change the properties or use a different properties file for the different server configurations below. Alternatively, you can use the following command to clear all state created by the current install before proceeding to use a different properties file as in the multi-node example below.

```
bin/gpServer.sh clear all
```

### Multi-node, local GNS
```
bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.server.local.properties restart all

bin/gpClient.sh -DgigapaxosConfig=conf/gigapaxos.server.local.properties edu.umass.cs.gnsclient.examples.ClientExample
```

### Distributed (remote) GNS 
Firing up distributed GNS servers on remote machines is very similar. It is necessary that the username on all of the remote machines is the same and, if different from the current shell's username, it must be specified using the USERNAME property in the properties file. It is convenient to set up passwordless ssh a priori from the local machine to the remote hosts, otherwise the script will prompt for a password for each remote host.

```
bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.server.ec2.properties restart all

bin/gpClient.sh -DgigapaxosConfig=conf/gigapaxos.server.ec2.properties edu.umass.cs.gnsclient.examples.ClientExample
```
