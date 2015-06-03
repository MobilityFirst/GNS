package edu.umass.cs.gns.gigapaxos.paxosutil;

import edu.umass.cs.utils.Util;

/**
@author V. Arun
 */

/* A simple utility class to serialize and deserialize paxos state for
 * pause and hot restore.
 */
public class HotRestoreInfo {
	public final String paxosID;
	public final short version;
	public final int[] members;
	public final int accSlot;
	public final Ballot accBallot;
	public final int accGCSlot;
	public final Ballot coordBallot;
	public final int nextProposalSlot;
	public final int[] nodeSlots;
	public HotRestoreInfo(String paxosID, short version, int[] members, int accSlot, Ballot accBallot, int accGCSlot, 
			Ballot coordBallot, int nextProposalSlot, int[] nodeSlots) {
		this.paxosID=paxosID;
		this.version=version;
		this.members=members;
		this.accBallot=accBallot;
		this.accSlot=accSlot;
		this.accGCSlot=accGCSlot;
		this.coordBallot=coordBallot;
		this.nextProposalSlot=nextProposalSlot;
		this.nodeSlots=nodeSlots;
	}
	/* NOTE: The order of the following operations is important */
	public HotRestoreInfo(String serialized) {
		String[] tokens = serialized.split("\\|");
		this.paxosID = tokens[0];
		this.version = Short.parseShort(tokens[1]);
		this.members = Util.stringToIntArray(tokens[2]);
		this.accSlot = Integer.parseInt(tokens[3]);
		this.accGCSlot = Integer.parseInt(tokens[5]);
		this.accBallot = new Ballot(tokens[4]);
		this.coordBallot = !tokens[6].equals("null") ? new Ballot(tokens[6]) : null;
		this.nextProposalSlot = Integer.parseInt(tokens[7]);
		this.nodeSlots = !tokens[8].equals("null") ? Util.stringToIntArray(tokens[8]) : null;
	}
	
	private static final char SEP = '|';
	public String toString() {
		return paxosID + SEP + version + SEP + Util.arrayOfIntToString(members) + SEP + accSlot + SEP + 
				accBallot + SEP + accGCSlot + SEP + (coordBallot!=null?coordBallot:"null") + SEP + 
				nextProposalSlot + SEP + (nodeSlots!=null ? Util.arrayOfIntToString(nodeSlots) : "null");
	}
	
	public static void main(String[] args) {
		int[] members = {1, 4, 67};
		int[] nodeSlots = {1, 3, 5};
		HotRestoreInfo hri1 = new HotRestoreInfo("paxos0", (short)2, members, 5, new Ballot(3, 4), 3, new Ballot(45, 67), 34, nodeSlots);
		System.out.println(hri1.toString());
		String str1 = hri1.toString();
		HotRestoreInfo hri2 = new HotRestoreInfo(str1);
		String str2 = hri2.toString();
		System.out.println(str2);
		assert(str1.equals(str2));
	}
	
}
