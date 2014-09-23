package edu.umass.cs.gns.util;

import java.util.Set;

/**
@author V. Arun
 */
public class Waitfor<NodeType> {

	private final Object[] members;
	private final boolean[] responded;
	private int heardCount=0;
	private long initTime=0; // to calculate how long we have been waiting
	private int retransmissionCount=0;
	
	public Waitfor(Object[] m) {
		this.members = m;
		responded = new boolean[m.length];
		initialize();
	}
	public Waitfor(Set<NodeType> m) {
		this.members = m.toArray();
		responded = new boolean[m.size()];
		initialize();
	}
	private void initialize() {
		this.initTime = System.currentTimeMillis();
		for(int i=0; i< this.members.length; i++) {
			responded[i] = false;
		}
	}
	
	public boolean updateHeardFrom(NodeType node) {
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
	public boolean alreadyHeardFrom(NodeType node) {
		int index = this.getIndex(node);
		if(index>=0 && index <this.members.length) {
			if(responded[index]) return true; 
		}
		return false;
	}
	public int getHeardCount() {return this.heardCount;}
	
	public boolean contains(NodeType node) {
		return getIndex(node) >= 0;
	}
	public Object[] getMembers() {
		return this.members;
	}
	public Object[] getMembersHeardFrom() {
		Object[] membersHF = new Object[this.heardCount];
		int i=0;
		for(Object memberObj : this.getMembers()) {
			@SuppressWarnings("unchecked")
			NodeType member = (NodeType)memberObj;
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
	private int getIndex(NodeType node) {
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
		Integer[] members = {0, 9, 4, 23};
		Waitfor<Integer> wfor = new Waitfor<Integer>(members);
		assert(!wfor.contains(32));
		assert(wfor.contains(9));
		assert(wfor.getMembers()==members);
		assert(wfor.getIndex(4)==2);
		assert(wfor.updateHeardFrom(9));
		assert(!wfor.heardFromMajority());
		assert(wfor.updateHeardFrom(23));
		assert(!wfor.heardFromMajority());
		assert(wfor.updateHeardFrom(0));
		assert(wfor.heardFromMajority());

		System.out.println("Success. The following methods were tested: [heardFrom, heardFromMajority, contains, getMembers, getIndex].");
	}
}
