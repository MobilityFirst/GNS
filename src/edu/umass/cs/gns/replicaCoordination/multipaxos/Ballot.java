package edu.umass.cs.gns.replicaCoordination.multipaxos;
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
		if (ballotNumber != b.ballotNumber ) return ballotNumber - b.ballotNumber;
		else return coordinatorID - b.coordinatorID;
	}
	
	public boolean equals(Ballot b) {
		return compareTo(b)==0;
	}

	@Override
	public   String toString() {
		return ballotNumber + ":" + coordinatorID;
	}
}
