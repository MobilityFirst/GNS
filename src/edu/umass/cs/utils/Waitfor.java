package edu.umass.cs.utils;

import java.util.HashSet;
import java.util.Set;

/**
@author V. Arun
 * @param <NodeIDType> 
 */
public class Waitfor<NodeIDType> {

	private final Set<NodeIDType> members;
	private final Set<NodeIDType> responded;
	private int heardCount=0;
	private long initTime=0; // to calculate how long we have been waiting
	private int retransmissionCount=0;
	
	/**
	 * @param m
	 */
	public Waitfor(NodeIDType[] m) {
		this.members = new HashSet<NodeIDType>();
		for(NodeIDType node : m) this.members.add(node);
		this.responded = new HashSet<NodeIDType>();
		initialize();
	}
	/**
	 * @param m
	 */
	public Waitfor(Set<NodeIDType> m) {
		this.members = m;
		this.responded = new HashSet<NodeIDType>();
		initialize();
	}
	private void initialize() {
		this.initTime = System.currentTimeMillis();
	}
	
	/**
	 * @param node
	 * @return True if changed.
	 */
	public boolean updateHeardFrom(NodeIDType node) {
		boolean changed=false;
		if(!this.responded.contains(node)) {
			changed = true;
			this.heardCount++;
		}
		this.responded.add(node);
		return changed;
	}
	
	/**
	 * @return True if heard from majority.
	 */
	public boolean heardFromMajority() {
		if(this.responded.size() > this.members.size()/2) return true;
		return false;
	}
	/**
	 * @param node
	 * @return True if already heard from {@code node}.
	 */
	public boolean alreadyHeardFrom(NodeIDType node) {
		return this.responded.contains(node);
	}
	/**
	 * @return Number of distinct nodes heard from.
	 */
	public int getHeardCount() {return this.heardCount;}
	
	private boolean contains(NodeIDType node) {
		return this.members.contains(node);
	}
	/**
	 * @return All nodes.
	 */
	public Set<NodeIDType> getMembers() {
		return this.members;
	}
	/**
	 * @return Nodes from which we have heard.
	 */
	public Set<NodeIDType> getMembersHeardFrom() {
		return this.responded;
	}
	/**
	 * @return Total time since creation.
	 */
	public long totalWaitTime() {
		return (int)System.currentTimeMillis() - this.initTime;
	}
	
	/**
	 * 
	 */
	public void incrRetransmissonCount() {
		this.retransmissionCount++;
	}
	/**
	 * @return The total number of retransmissions.
	 */
	public int getRetransmissionCount() {
		return this.retransmissionCount;
	}
	public String toString() {
		String s="{Members: [ ";
		for(NodeIDType node : this.members) s+=node;
		s+="], Responded: [";
		for(NodeIDType node : this.responded) {
			s+=node;
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
		for(int i : members) assert(wfor.contains(i));
		assert(wfor.contains(4));
		assert(wfor.updateHeardFrom(9));
		assert(!wfor.heardFromMajority());
		assert(wfor.updateHeardFrom(23));
		assert(!wfor.heardFromMajority());
		assert(wfor.updateHeardFrom(0));
		assert(wfor.heardFromMajority());

		System.out.println("Success. The following methods were tested: [heardFrom, heardFromMajority, contains, getMembers, getIndex].");
	}
}
