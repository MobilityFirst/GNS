
Prerequisites: `JRE1.8+`, `bash`, `mongodb` (optional).

### Obtaining GNS
Download the latest, stable, binary-only package from the [GNS releases page](https://github.com/MobilityFirst/GNS/releases).

[//]: # (This is a comment, it will not be included)

### Single-node, local GNS server
The GNS is a [gigapaxos](https://github.com/MobilityFirst/gigapaxos) appplication, which means that gigapaxos is the distributed engine responsible for replicating and reconfiguring GNS replica nodes as needed. The GNS uses mongodb as its underlying database by default, so it is necessary to have mongodb running (on localhost and its default port) before starting the GNS. If mongodb is not already running, refer to the [mongodb installation instructions page](https://docs.mongodb.com/manual/installation/) to set that up (recommended). If you prefer to not use mongodb for now, refer to the instructions further below to run the GNS in the in-memory database mode (not recommended for production use). 

Start a single-node, local GNS server as: 

```
bin/gpServer.sh start all
```

To connect to this GNS server using the command-line interface (CLI), refer to the [CLI introduction page](https://github.com/MobilityFirst/GNS/wiki/Command-Line-Interface).

### Simple client example
To connect to the GNS server above using a Java client, we will use a simple client, [ClientExample](https://github.com/MobilityFirst/GNS/blob/master/src/edu/umass/cs/gnsclient/examples/ClientExample.java) [(doc)](http://mobilityfirst.github.io/GNS/doc/edu/umass/cs/gnsclient/examples/ClientExample.html), that creates a record keyed by a globally unique identifier (GUID), performs a few field reads and writes, and deletes the record. Run this example client as

```
bin/gpClient.sh edu.umass.cs.gnsclient.examples.ClientExample
```

The client's output should be self-explanatory and it should exit gracefully with a success message. If it runs into exceptions, refer to this [troubleshooting TBD] page for common possible reasons.

### In-memory database mode
If you prefer to not use mongodb, uncomment or enter `IN_MEMORY_DB=true` in the default configuration properties file, `gigapaxos.properties`. Using the in-memory mode does not affect durability or fault-tolerance as they are ensured by gigapaxos, however, the database size will be limited by memory, so it is not recommended for production use.

### Stopping GNS server(s)

The GNS server can be stopped as

```
bin/gpServer.sh stop all
```

The above commands were intentionally designed to be simple, but it is important to understand what happened underneath the covers in order to be able to run a distributed server setup, which we do next by understanding how configuration properties are specified.

### Configuration properties overview
Running the gigapaxos server script `bin/gpServer.sh` without any arguments prints a usage summary.

The script looks for a properties file named `gigapaxos.properties` by default in the current directory and, if not found, in the `./conf/` subdirectory. A detailed explanation of gigapaxos properties is at the [gigapaxos configuration properties](https://github.com/MobilityFirst/gigapaxos/wiki/Configuration-properties) page. We just need to observe here that the `APPLICATION` is GNSApp and the file specifies a single _reconfigurator_ and a single _active_ replica, both listening on localhost ports. Both gigapaxos- and GNS-specific properties are specified in the same properties file.

In general, the client and server are expected to use different property files. The above example involved both the server and client using the same file, `./gigapaxos.properties`, for simplicity. Verify that the behavior is the same as above with the following commands each of which explicitly specifies the corresponding properties file. Note that <tt>restart</tt> is equivalent to a <tt>stop</tt> followed by a <tt>start</tt>.


    bin/gpServer.sh -DgigapaxosConfig=conf/gnsserver.1local.properties restart all

    bin/gpClient.sh -DgigapaxosConfig=conf/gnsclient.1local.properties edu.umass.cs.gnsclient.examples.ClientExample

    bin/gpServer.sh -DgigapaxosConfig=conf/gnsserver.1local.properties stop all


For safety, many gigapaxos properties, especially node names and addresses, can not be manually changed after bootstrap. The best practice is to create a fresh server install in a new directory or to specify a different data directory in order to change the set of servers or the properties file. The server names in the multi-node, local properties file <tt>gnsserver.3local.properties</tt> below have no overlap with those in the single-node, local properties file used above, so we can re-use the same installation directory, but we do need to stop any already running servers before starting new ones in order to avoid port conflicts.

### Multi-node, local GNS
Stop any previously started GNS servers by issuing <tt>stop all</tt> with the corresponding properties file. Then start a 3-replicated GNS server as

```
bin/gpServer.sh -DgigapaxosConfig=conf/gnsserver.3local.properties restart all

bin/gpClient.sh -DgigapaxosConfig=conf/gnsclient.3local.properties edu.umass.cs.gnsclient.examples.ClientExample
```

### Distributed GNS 
Firing up distributed GNS servers on remote machines is nearly identical. It is necessary that the username on all of the remote machines is the same and, if different from the current shell's username, it must be specified using the `USERNAME` property in the properties file. It is convenient to set up passwordless ssh a priori from the local machine to the remote hosts, otherwise the script will prompt for a password for each remote host.

```
bin/gpServer.sh -DgigapaxosConfig=conf/gnsserver.ec2.properties restart all

bin/gpClient.sh -DgigapaxosConfig=conf/gnsclient.ec2.properties edu.umass.cs.gnsclient.examples.ClientExample
```
