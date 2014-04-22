package edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil;
/**
@author V. Arun
 */

public class Ballot implements Comparable<Ballot>{
	public final int ballotNumber;
	public final int coordinatorID;

	public Ballot(int ballotNumber, int coordinatorID) {
		this.ballotNumber = ballotNumber;
		this.coordinatorID = coordinatorID;
	}

	public Ballot(String s) {
		String[] tokens = s.split(":");
		this.ballotNumber = new Integer(tokens[0]);
		this.coordinatorID = new Integer(tokens[1]);
	}
	
	@Override
	public  int compareTo(Ballot b) {
		if (ballotNumber != b.ballotNumber ) return ballotNumber - b.ballotNumber; // will handle wraparounds correctly
		else return coordinatorID - b.coordinatorID;
	}
	public int compareTo(int bnum, int coord) {
		if (ballotNumber != bnum ) return ballotNumber - bnum; // will handle wraparounds correctly
		else return coordinatorID - coord;
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
	public int hashCode() {
		return (100+ballotNumber)*(100+ballotNumber) + coordinatorID;
	}

	@Override
	public   String toString() {return ballotNumber + ":" + coordinatorID;}
	public static String getBallotString(int bnum, int coord) {return bnum+":"+coord;}

}
