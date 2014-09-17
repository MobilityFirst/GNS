package edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;

/**
@author V. Arun
 */

public class Ballot implements Comparable<Ballot>{
	public final int ballotNumber;
	public final NodeId<String> coordinatorID;

	public Ballot(int ballotNumber, NodeId<String> coordinatorID) {
		this.ballotNumber = ballotNumber;
		this.coordinatorID = coordinatorID;
	}

	public Ballot(String s) {
		String[] tokens = s.split(":");
		this.ballotNumber = new Integer(tokens[0]);
		this.coordinatorID = new NodeId<String>(tokens[1]);
	}
	
	@Override
	public  int compareTo(Ballot b) {
		if (ballotNumber != b.ballotNumber ) return ballotNumber - b.ballotNumber; // will handle wraparounds correctly
		else return coordinatorID.compareTo(b.coordinatorID);
	}
	public int compareTo(int bnum, NodeId<String> coord) {
		if (ballotNumber != bnum ) return ballotNumber - bnum; // will handle wraparounds correctly
		else return coordinatorID.compareTo(coord);
	}
	
	public boolean equals(Ballot b) {
		return compareTo(b)==0;
	}
	/* Need to implement hashCode if we modify equals. The
	 * only property we need is equals() => hashcodes are 
	 * also equal. The method below just ensures that. It
	 * also roughly tries to incorporate that ballotnum
	 * is the more significant component, but that is 
	 * not strictly necessary for anything.
	 */
        @Override
	public int hashCode() {
		return (100+ballotNumber)*(100+ballotNumber) + coordinatorID.hashCode();
	}

	@Override
	public   String toString() {return ballotNumber + ":" + coordinatorID;}
	public static String getBallotString(int bnum, NodeId<String> coord) {return bnum+":"+coord.get();}

}
