package edu.umass.cs.gns.nio;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import java.net.InetSocketAddress;

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
public class MessagingTask extends GenericMessagingTask<NodeId<String>,Object> {

	public final Object[] recipients;
	public final Object[] msgs;
	
	// Unicast
	public MessagingTask(NodeId<String> destID, Object pkt) {
		assert(pkt!=null) : "Incorrect usage: MessagingTask should not be instantiated with no messages";
		this.recipients = new NodeId[1];
		this.recipients[0] = destID;
		
		this.msgs = new Object[pkt==null?0:1];
		if(pkt!=null) msgs[0] = pkt;
	}
	// Multicast
	public MessagingTask(NodeId<String>[] destIDs, Object pkt) {
		assert(pkt!=null && destIDs!=null) : "Incorrect usage: MessagingTask should not be instantiated with null messages or destinations";
		this.recipients = toObject(destIDs);
		this.msgs = new Object[pkt==null?0:1];
		if(pkt!=null) msgs[0] = pkt;
	}
	// Unicast multiple packets
	public MessagingTask(NodeId<String> destID, Object[] pkts) {
		assert(pkts!=null && pkts.length>0 && pkts[0]!=null) : "Incorrect usage: MessagingTask should not be instantiated with no messages";

		this.recipients = new NodeId[1];
		this.recipients[0] = destID;
		this.msgs = pkts==null ? new Object[0] : pkts;
	}
	/* Multicast multiple packets. This is a special type of constructor compared to the others
	 * above. This is the only constructor that allows MessagingTask to be instantiated with
	 * no messages. We need this as it is convenient to create an empty MessagingTask in a 
	 * logging-only LogMessagingTask object.
	 */
	public MessagingTask(NodeId<String>[] destIDs, Object[] pkts) {
		assert(pkts!=null && destIDs!=null) : "Incorrect usage: MessagingTask should not be instantiated with null messages or destinations";
		for(Object obj : pkts) assert(obj!=null) : "Incorrect usage: MessagingTask should not be instantiated with null messages";

		this.recipients = (destIDs==null ? new NodeId[0] : toObject(destIDs));
		this.msgs = pkts==null ? new Object[0] : pkts;
	}
        
        // Unicast to addresses... all we need for now.
        public MessagingTask(InetSocketAddress destID, Object pkt) {
		assert(pkt!=null) : "Incorrect usage: MessagingTask should not be instantiated with no messages";
		this.recipients = new InetSocketAddress[1];
		this.recipients[0] = destID;
		
		this.msgs = new Object[pkt==null?0:1];
		if(pkt!=null) msgs[0] = pkt;
	}
	
	public boolean isEmpty() {
		return this.msgs==null || this.msgs.length==0 || this.recipients==null || this.recipients.length==0;
	}
	
	
	public static MessagingTask[] toArray(MessagingTask mtask1, MessagingTask mtask2) {
		MessagingTask[] mtasks = new MessagingTask[2];
		mtasks[0] = mtask1; mtasks[1] = mtask2;
		return mtasks;
	}
	public MessagingTask[] toArray() {
		MessagingTask[] mtasks = new MessagingTask[1];
		mtasks[0] = this;
		return mtasks;
	}
	public static boolean isEmpty(MessagingTask[] mtasks) {
		if(mtasks==null || mtasks.length==0) return true;
		boolean empty = true;
		for(MessagingTask mtask : mtasks) {
			empty = empty && mtask.isEmpty();
		}
		return empty;
	}

	// Mostly just pretty printing
	public String toString() {
		if(msgs.length==0) return "NULL";
		String s = ""; 
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
        
        public static NodeId<String>[] toObject(NodeId<String>[] nodeArray) {
 
		NodeId<String>[] result = new NodeId[nodeArray.length];
		for (int i = 0; i < nodeArray.length; i++) {
			result[i] = nodeArray[i];
		}
		return result;
 
	}
 

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}
}
