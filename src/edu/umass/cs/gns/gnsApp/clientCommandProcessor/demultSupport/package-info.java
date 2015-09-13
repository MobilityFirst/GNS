/**
 *
 * Implements a local name server module for GNS.
 * <p>
 * Module implements following functionality:
 * <ul>
 * <li>sending requests to appropriate name servers and retransmitting them if necessary.
 * <li>obtaining the active replicas a name from replica controllers and caching them
 * <li>maintaining a TTL-based cache of name records based on recent requests received
 * <li>forwarding replies received from name servers to the Intercessor module or to the socket
 * on which the request was received.
 * <li>logging statistics for each request: success/failure, latency, name servers contacted etc.
 * </ul>
 * <p>
 * <b>Sending requests to name servers:</b> GNS designates some name servers as replica controllers for a name
 * and some as active replicas. The role of replica controllers is to decide the set of active replicas, while
 * active replicas store the name records. Client requests are sent to active replicas, except for requests
 * that ADD or REMOVE a name from GNS, which are sent to replica controllers. Local name server sends a request
 * first to the nearest name server, and upon a timeout request is retransmitted to the next closest name server.
 * If no response is received until a given maximum wait time, local name server sends a failure message to the client.
 * <p>
 * <b>Getting active replicas for names:</b> A key functionality of local name servers is to obtain the active replicas
 * for a name from replica controllers can cache them locally. The set of replica controllers is fixed and can be computed
 * locally by local name server. But the set of active replicas can change, after which the cached set of active replicas
 * is updated by again contacting the replica controllers. This update process happens only upon the arrival of a
 * request for a name. If upon sending the request, the local name server finds that the set of active replicas has
 * changed, it contacts replica controller for the current set of active replicas. Local name server queues up any
 * requests for a name for which it is obtaining the current set of active replicas. Its executes the queued requests
 * for that name after its receives its current active replica set.
 * <p>
 * <b>TTL-based caching:</b> Local name server keeps a TTL-based cache of name records. Cache is used to reply to
 * lookup requests by clients. The TTL-value is not set by local name server but it depends on TTL associated with name record.
 * This cached name record is updated upon a response to a lookup request. The cached name record is invalidated in two
 * cases: (1) either its TTL expires (2) on a response to an update or a remove request for the name.
 * The same cache entry for a name is used to store both the set active replicas for a name as well as its name record.
 * The caching is implemented using Google's guava caching library. The size of the cache is a configurable parameter.
 * <p>
 * <b>Forwarding replies:</b>To forward replies back to clients, local name server remembers the client-assigned request
 * ID as well as socket address and port on which the request was received (this feature is currently broken).
 * Alternatively, if the request was sent via Intercessor, it forwards the reply containing the client-assigned request
 * ID to the Intercessor.
 * <p>
 * <b>Logging statistics:</b>For each request, local name server logs a single entry indicating the success/failure of
 * request and other statistics. These statistics are output in a different set of log files. The format of these logs
 * is described in a separate document TODO.
 * <p>
 *
 * Created by abhigyan on 3/5/14.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport;