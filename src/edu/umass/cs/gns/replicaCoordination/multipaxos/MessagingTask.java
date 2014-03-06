package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.util.ArrayList;

import edu.umass.cs.gns.packet.PaxosPacket;

/**
@author V. Arun
 */

/* This messaging task means that each msg needs
 * to be sent to all recipients. Note: it does 
 * *NOT* mean that each msg needs to be sent to
 * its corresponding recipient; in fact, the 
 * sizes of the two arrays, recipients and msgs,
 * may be different.
 */
public class MessagingTask {

	protected final int[] recipients;
	protected final PaxosPacket[] msgs;
	
	// Unicast
	MessagingTask(int destID, PaxosPacket pkt) {
		assert(pkt!=null) : "Incorrect usage: MessagingTask can not be instantiated with no messages";
		this.recipients = new int[1];
		this.recipients[0] = destID;
		
		this.msgs = new PaxosPacket[1];
		msgs[0] = pkt;
	}
	// Multicast
	MessagingTask(int[] destIDs, PaxosPacket pkt) {
		assert(pkt!=null && destIDs!=null) : "Incorrect usage: MessagingTask can not be instantiated with no messages";
		this.recipients = destIDs;
		this.msgs = new PaxosPacket[1];
		msgs[0] = pkt;
	}
	// Unicast multiple packets
	MessagingTask(int destID, PaxosPacket[] pkts) {
		//assert(pkts!=null && pkts.length>0 && pkts[0]!=null) : "Incorrect usage: MessagingTask can not be instantiated with no messages";

		this.recipients = new int[1];
		this.recipients[0] = destID;
		this.msgs = pkts==null ? new PaxosPacket[0] : pkts;
	}
	// Multicast multiple packets
	MessagingTask(int[] destIDs, PaxosPacket[] pkts) {
		//assert(pkts!=null && pkts.length>0 && pkts[0]!=null) : "Incorrect usage: MessagingTask can not be instantiated with no messages";

		this.recipients = (destIDs==null ? new int[0] : destIDs);
		this.msgs = pkts==null ? new PaxosPacket[0] : pkts;
	}
	
	public void putPaxosID(String paxosID) {
		if(msgs==null) return;
		for(int i=0; i<msgs.length; i++) {
			assert(msgs[i]!=null) : "Incorrect usage: MessagingTask can not be instantiated with null messages";
			msgs[i].putPaxosID(paxosID);
		}
	}
	public boolean isEmpty() {
		return this.msgs==null || this.msgs.length==0 || this.recipients==null || this.recipients.length==0;
	}
	public String toString() {
		if(msgs.length==0) return "NULL";
		String s = PaxosPacket.typeToString[msgs[0].getType()];
		s+= ": Recipients: [";
		for(int i=0;i<this.recipients.length; i++) {
			s += this.recipients[i]+" ";
		}
		s += "] , ";
		for(int i=0; i<msgs.length; i++) {
			s += msgs[i];
		}
		return s;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
