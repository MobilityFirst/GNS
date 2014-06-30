package edu.umass.cs.gns.nio;


/**
@author V. Arun
 */
public class GenericMessagingTask<NodeIDType, MessageType> {
	public final Object[] recipients;
	public final Object[] msgs;
	
	public GenericMessagingTask() {this.recipients=null; this.msgs=null;}
	// Unicast
	public GenericMessagingTask(NodeIDType destID, MessageType pkt) {
		assert(pkt!=null) : "Incorrect usage: InterfaceMessagingTask should not be instantiated with no messages";
		this.recipients = new Object[1];
		this.recipients[0] = destID;
		
		this.msgs = new Object[pkt==null?0:1];
		if(pkt!=null) msgs[0] = pkt;
	}
	// Multicast
	public GenericMessagingTask(Object[] destIDs, MessageType pkt) {
		assert(pkt!=null && destIDs!=null) : "Incorrect usage: InterfaceMessagingTask should not be instantiated with null messages or destinations";
		this.recipients = destIDs;
		this.msgs = new Object[pkt==null?0:1];
		if(pkt!=null) msgs[0] = pkt;
	}
	// Unicast multiple packets
	public GenericMessagingTask(NodeIDType destID, Object[] pkts) {
		assert(pkts!=null && pkts.length>0 && pkts[0]!=null) : "Incorrect usage: InterfaceMessagingTask should not be instantiated with no messages";

		this.recipients = new Object[1];
		this.recipients[0] = destID;
		this.msgs = pkts==null ? new Object[0] : pkts;
	}
	/* Multicast multiple packets. This is a special type of constructor compared to the others
	 * above. This is the only constructor that allows InterfaceMessagingTask to be instantiated with
	 * no messages. We need this as it is convenient to create an empty InterfaceMessagingTask in a 
	 * logging-only LogInterfaceMessagingTask object.
	 */
	public GenericMessagingTask(Object[] destIDs, Object[] pkts) {
		assert(pkts!=null && destIDs!=null) : "Incorrect usage: InterfaceMessagingTask should not be instantiated with null messages or destinations";
		for(Object obj : pkts) assert(obj!=null) : "Incorrect usage: InterfaceMessagingTask should not be instantiated with null messages";

		this.recipients = (destIDs==null ? new Object[0] : destIDs);
		this.msgs = pkts==null ? new Object[0] : pkts;
	}
	
	public boolean isEmpty() {
		return this.msgs==null || this.msgs.length==0 || this.recipients==null || this.recipients.length==0;
	}
	
	public static GenericMessagingTask<?,?>[] toArray(GenericMessagingTask<?,?> mtask1, GenericMessagingTask<?,?> mtask2) {
		GenericMessagingTask<?,?>[] mtasks = new GenericMessagingTask[2];
		mtasks[0] = mtask1; mtasks[1] = mtask2;
		return mtasks;
	}
	public GenericMessagingTask<?,?>[] toArray() {
		GenericMessagingTask<?,?>[] mtasks = new GenericMessagingTask[1];
		mtasks[0] = this;
		return mtasks;
	}
	public static boolean isEmpty(GenericMessagingTask<?,?>[] mtasks) {
		if(mtasks==null || mtasks.length==0) return true;
		boolean empty = true;
		for(GenericMessagingTask<?,?> mtask : mtasks) {
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

	public static void main(String[] args) {
		GenericMessagingTask<Integer,String> mtask = new GenericMessagingTask<Integer,String>(23, "Hello");
		assert(mtask.msgs.length==1 && mtask.msgs[0].equals("Hello"));
		Integer[] nodes = {23, 45, 66};
		GenericMessagingTask<Integer,String> mtask1 = new GenericMessagingTask<Integer,String>(nodes, "World");
		assert(mtask1.msgs.length==1 && mtask1.msgs[0].equals("World"));
	}
}
