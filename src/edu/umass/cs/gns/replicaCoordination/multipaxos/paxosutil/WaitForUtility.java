package edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil;
/**
@author V. Arun
 */

/* A waitfor utility for integer node identifiers that uses 
 * roughly 4(N+1) bytes when waiting for responses from N nodes.
 * Note that the final array members will not occupy space in 
 * each instance as it will simply be a pointer to the 
 * corresponding array in the paxos instance that created it.
 */
public class WaitForUtility {

	private final int[] members;
	private final boolean[] responded;
	private int heardCount=0;
	private int initTime=0; // to calculate how long we have been waiting
	
	public WaitForUtility(int[] m) {
		this.members = m;
		responded = new boolean[m.length];
		for(int i=0; i< m.length; i++) {
			responded[i] = false;
		}
	}
	
	public boolean updateHeardFrom(int node) {
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
	public boolean alreadyHeardFrom(int node) {
		int index = this.getIndex(node);
		if(index>=0 && index <this.members.length) {
			if(responded[index]) return true; 
		}
		return false;
	}
	public boolean contains(int node) {
		return getIndex(node) >= 0;
	}
	public int[] getMembers() {
		return this.members;
	}
	public long totalWaitTime() {
		return System.currentTimeMillis() - this.initTime;
	}
	private int getIndex(int node) {
		int index = -1;
		for(int i=0; i<this.members.length; i++) {
			if(this.members[i] == node) index = i;
		}
		return index;
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
		int[] members = {0, 9, 4, 23};
		WaitForUtility wfor = new WaitForUtility(members);
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
