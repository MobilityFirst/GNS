package edu.umass.cs.gigapaxos.paxosutil;


import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;

/**
 * 
 * @author V. Arun
 * 
 *         This messaging task means that each msg needs to be sent to all
 *         recipients. Note: it does *NOT* mean that each msg needs to be sent
 *         to its corresponding recipient; in fact, the sizes of the two arrays,
 *         recipients and msgs, may be different. This utility class is really
 *         just a container for two arrays.
 */
public class MessagingTask {

	/**
	 * List of recipients.
	 */
	public final int[] recipients;
	/** 
	 * List of paxos packets to be sent.
	 */
	public final PaxosPacket[] msgs;
	
	/**
	 * Unicast
	 * @param destID
	 * @param pkt
	 */
	public MessagingTask(int destID, PaxosPacket pkt) {
		assert(pkt!=null) : "Incorrect usage: MessagingTask should not be instantiated with no messages";
		this.recipients = new int[1];
		this.recipients[0] = destID;
		
		this.msgs = new PaxosPacket[pkt==null?0:1];
		if(pkt!=null) msgs[0] = pkt;
	}
	/**
	 * Multicast
	 * @param destIDs
	 * @param pkt
	 */
	public MessagingTask(int[] destIDs, PaxosPacket pkt) {
		assert(pkt!=null && destIDs!=null) : "Incorrect usage: MessagingTask should not be instantiated with null messages or destinations";
		this.recipients = destIDs;
		this.msgs = new PaxosPacket[pkt==null?0:1];
		if(pkt!=null) msgs[0] = pkt;
	}
	/**
	 * Unicast multiple packets
	 * @param destID
	 * @param pkts
	 */
	public MessagingTask(int destID, PaxosPacket[] pkts) {
		assert(pkts!=null && pkts.length>0 && pkts[0]!=null) : "Incorrect usage: MessagingTask should not be instantiated with no messages";

		this.recipients = new int[1];
		this.recipients[0] = destID;
		this.msgs = pkts==null ? new PaxosPacket[0] : pkts;
	}
	/**
	 *  Multicast multiple packets. This is a special type of constructor compared to the others
	 * above. This is the only constructor that allows MessagingTask to be instantiated with
	 * no messages. We need this as it is convenient to create an empty MessagingTask in a 
	 * logging-only LogMessagingTask object.
	 * @param destIDs 
	 * @param pkts 
	 */
	public MessagingTask(int[] destIDs, PaxosPacket[] pkts) {
		assert(pkts!=null && destIDs!=null) : "Incorrect usage: MessagingTask should not be instantiated with null messages or destinations";
		for(Object obj : pkts) assert(obj!=null) : "Incorrect usage: MessagingTask should not be instantiated with null messages";

		this.recipients = (destIDs==null ? new int[0] : destIDs);
		this.msgs = pkts==null ? new PaxosPacket[0] : pkts;
	}
	
	/**
	 * @param paxosID
	 * @param version
	 */
	public void putPaxosIDVersion(String paxosID, int version) {
		if(msgs==null) return;
		for(int i=0; i<msgs.length; i++) {
			assert(msgs[i]!=null) : "Incorrect usage: MessagingTask should not be instantiated with null messages, " +
					"msg " + i + " out of " + msgs.length + " is null";
			msgs[i].putPaxosID(paxosID, version);
		}
	}
	
	/**
	 * @return Whether the list of recipients or messages is null or empty,
	 *         i.e., there is nothing to do.
	 */
	public boolean isEmpty() {
		return this.msgs==null || this.msgs.length==0 || this.recipients==null || this.recipients.length==0;
	}
	
	/**
	 * Converts an object array to a PaxosPacket array because there is
	 * annoyingly no easy way in Java to convert a collection of a child
	 * type to an array of a parent type. Yuck. But we need to do this
	 * because toArray() methods in Collections return Object arrays.
	 * 
	 * @param ppChildArray
	 * @return The PaxosPacket array from the Object[].
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
	
	/**
	 * @param array1
	 * @param array2
	 * @return PaxosPacket[] array with two members supplied as parameters.
	 */
	public static PaxosPacket[] toPaxosPacketArray(PaxosPacket[] array1, PaxosPacket[] array2) {
		int size1 = (array1!=null ? array1.length : 0); 
		int size2 = (array2!=null ? array2.length : 0);
		PaxosPacket[] combined = new PaxosPacket[size1+size2];
		if(array1!=null) for(int i=0; i<array1.length; i++) combined[i] = array1[i];
		if(array2!=null) for(int j=size1; j<size1+size2; j++) combined[j] = array2[j-size1];
		return combined;
	}

	/**
	 *  Just for pretty printing.
	 */
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
			if(i==5 && msgs.length>25) {
				s+=".... (skipping " + (msgs.length-9) + ")";
				i=msgs.length-6;
			} else 	s+=msgs[i].getSummary();
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
