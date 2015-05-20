package edu.umass.cs.gns.gigapaxos.deprecated;
/**
@author V. Arun
 */

/* Arun: Changed the "boolean recovery" argument to "boolean doNotReplyToClient"
 * as that is what it really means. More importantly, doNotReplyToClient may
 * be true even during non-recovery periods as only one of the replicas, 
 * either the replica that originally received the request ot the coordinating
 * replica for that request (either should be easily pluggable) by design 
 * replies to the client.
 * 
 * FIXME: The boolean argument should go from Application and be only in Replicable.
 */
public interface Application {
	  public boolean handleDecision(String name, String value, boolean doNotReplyToClient);
}
