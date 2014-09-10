package edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PaxosPacket;

/**
@author V. Arun
 */

/* This messaging task means that each msg needs
 * to be sent to all recipients. Note: it does 
 * *NOT* mean that each msg needs to be sent to
 * its corresponding recipient; in fact, the 
 * sizes of the two arrays, recipients and msgs,
 * may be different.
 * 
 * This is a utility class as it is really just
 * a container for two arrays.
 */
public class MessagingTask {

	public final NodeId<String>[] recipients;
	public final PaxosPacket[] msgs;
	
	// Unicast
	public MessagingTask(NodeId<String> destID, PaxosPacket pkt) {
		assert(pkt!=null) : "Incorrect usage: MessagingTask should not be instantiated with no messages";
		this.recipients = new NodeId[1];
		this.recipients[0] = destID;
		
		this.msgs = new PaxosPacket[pkt==null?0:1];
		if(pkt!=null) msgs[0] = pkt;
	}
	// Multicast
	public MessagingTask(NodeId<String>[] destIDs, PaxosPacket pkt) {
		assert(pkt!=null && destIDs!=null) : "Incorrect usage: MessagingTask should not be instantiated with null messages or destinations";
		this.recipients = destIDs;
		this.msgs = new PaxosPacket[pkt==null?0:1];
		if(pkt!=null) msgs[0] = pkt;
	}
	// Unicast multiple packets
	public MessagingTask(NodeId<String> destID, PaxosPacket[] pkts) {
		assert(pkts!=null && pkts.length>0 && pkts[0]!=null) : "Incorrect usage: MessagingTask should not be instantiated with no messages";

		this.recipients = new NodeId[1];
		this.recipients[0] = destID;
		this.msgs = pkts==null ? new PaxosPacket[0] : pkts;
	}
	/* Multicast multiple packets. This is a special type of constructor compared to the others
	 * above. This is the only constructor that allows MessagingTask to be instantiated with
	 * no messages. We need this as it is convenient to create an empty MessagingTask in a 
	 * logging-only LogMessagingTask object.
	 */
	public MessagingTask(NodeId<String>[] destIDs, PaxosPacket[] pkts) {
		assert(pkts!=null && destIDs!=null) : "Incorrect usage: MessagingTask should not be instantiated with null messages or destinations";
		for(Object obj : pkts) assert(obj!=null) : "Incorrect usage: MessagingTask should not be instantiated with null messages";

		this.recipients = (destIDs==null ? new NodeId[0] : destIDs);
		this.msgs = pkts==null ? new PaxosPacket[0] : pkts;
	}
	
	public void putPaxosIDVersion(String paxosID, short version) {
		if(msgs==null) return;
		for(int i=0; i<msgs.length; i++) {
			assert(msgs[i]!=null) : "Incorrect usage: MessagingTask should not be instantiated with null messages, " +
					"msg " + i + " out of " + msgs.length + " is null";
			msgs[i].putPaxosID(paxosID, version);
		}
	}
	public boolean isEmpty() {
		return this.msgs==null || this.msgs.length==0 || this.recipients==null || this.recipients.length==0;
	}
	
	/* Converts an object array to a PaxosPacket array because there is
	 * annoyingly no easy way to convert a collection of a child
	 * type to an array of a parent type. Yuck.
	 */
	public static PaxosPacket[] toPaxosPacketArray(Object[] ppChildArray) {
		assert(ppChildArray!=null && ppChildArray.length>0);
		for(int i=0; i<ppChildArray.length; i++) assert(ppChildArray[i]!=null) : "Incorrect usage: MessagingTask should " +
				"not be instantiated with null messages, msg " + i + " out of " + ppChildArray.length + " is null";
		PaxosPacket[] ppArray = new PaxosPacket[ppChildArray.length];
		for(int i=0; i<ppChildArray.length; i++) {
			assert(ppChildArray[i] instanceof PaxosPacket) : "Incorrect usage: MessagingTask can only take PaxosPacket objects";
			ppArray[i] = (PaxosPacket)ppChildArray[i];
		}
		return ppArray;
	}

	// Mostly just pretty printing
	public String toString() {
		if(msgs.length==0) return "NULL";
		String s = (msgs[0].getType().toString());
		s+= " to recipients: [";
		for(int i=0;i<this.recipients.length; i++) {
			s += this.recipients[i]+" ";
		}
		s += "]; Message";
		s += (msgs.length>1) ? "s:\n[" : ": ";
		for(int i=0; i<msgs.length; i++) {
			s += (msgs.length>1 ? "\n    " : "");
			if(i==10 && msgs.length>25) {
				s+=".... (skipping " + (msgs.length-19) + ")";
				i=msgs.length-11;
			} else 	s+=msgs[i];
		}
		if(msgs.length>1) s+="\n]";
		return s;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}
}
