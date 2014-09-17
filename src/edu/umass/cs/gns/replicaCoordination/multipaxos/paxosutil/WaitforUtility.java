package edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;

/**
@author V. Arun
 */

/* A waitfor utility for integer node identifiers that uses 
 * roughly 8(N+1) bytes when waiting for responses from N nodes.
 * Note that the final array members will not occupy space in 
 * each instance as it will simply be a pointer to the 
 * corresponding array in the paxos instance that created it.
 */
public class WaitforUtility {

	private final NodeId<String>[] members;
	private final boolean[] responded;
	private int heardCount=0;
	private int initTime=0; // to calculate how long we have been waiting
	private int retransmissionCount=0;
	
	public WaitforUtility(NodeId<String>[] m) {
		this.members = m;
		this.initTime = (int)System.currentTimeMillis(); // Warning: we have to cast to int throughout
		responded = new boolean[m.length];
		for(int i=0; i< m.length; i++) {
			responded[i] = false;
		}
	}
	
	public boolean updateHeardFrom(NodeId<String> node) {
		boolean changed=false;
		int index = this.getIndex(node);
		if(index>=0 && index <this.members.length) {
			if(!responded[index]) {
				changed = true;
				heardCount++;
			}
			responded[index]=true;
		}
		return changed;
	}
	
	public boolean heardFromMajority() {
		if(this.heardCount > this.members.length/2) return true;
		return false;
	}
	public boolean alreadyHeardFrom(NodeId<String> node) {
		int index = this.getIndex(node);
		if(index>=0 && index <this.members.length) {
			if(responded[index]) return true; 
		}
		return false;
	}
	public int getHeardCount() {return this.heardCount;}
	
	public boolean contains(NodeId<String> node) {
		return getIndex(node) >= 0;
	}
	public NodeId<String>[] getMembers() {
		return this.members;
	}
	public NodeId<String>[] getMembersHeardFrom() {
		NodeId<String>[] membersHF = new NodeId[this.heardCount];
		int i=0;
		for(NodeId<String> member : this.getMembers()) {
			if(alreadyHeardFrom(member)) membersHF[i++] = member;
		}
		return membersHF;
	}
	public long totalWaitTime() {
		return (int)System.currentTimeMillis() - this.initTime;
	}
	public void setInitTime() {
		this.initTime = (int)System.currentTimeMillis();
	}
	private int getIndex(NodeId<String> node) {
		int index = -1;
		for(int i=0; i<this.members.length; i++) {
			if(this.members[i].equals(node)) index = i;
		}
		return index;
	}
	public void incrRetransmissonCount() {
		this.retransmissionCount++;
	}
	public int getRetransmissionCount() {
		return this.retransmissionCount;
	}
	public String toString() {
		String s="{Members: [ ";
		for(int i=0; i<members.length; i++) s+=members[i] + " ";
		s+="], Responded: [";
		for(int i=0; i<members.length; i++) {
			if(responded[i]) s+=members[i] + " ";
		}
		s+="]}";
		return s;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		NodeId[] members = {new NodeId<String>(0), new NodeId<String>(9), new NodeId<String>(4), new NodeId<String>(23)};
		WaitforUtility wfor = new WaitforUtility(members);
		assert(!wfor.contains(new NodeId<String>(32)));
		assert(wfor.contains(new NodeId<String>(9)));
		assert(wfor.getMembers()==members);
		assert(wfor.getIndex(new NodeId<String>(4))==2);
		assert(wfor.updateHeardFrom(new NodeId<String>(9)));
		assert(!wfor.heardFromMajority());
		assert(wfor.updateHeardFrom(new NodeId<String>(23)));
		assert(!wfor.heardFromMajority());
		assert(wfor.updateHeardFrom(new NodeId<String>(0)));
		assert(wfor.heardFromMajority());

		System.out.println("Success. The following methods were tested: [heardFrom, heardFromMajority, contains, getMembers, getIndex].");
	}

}
