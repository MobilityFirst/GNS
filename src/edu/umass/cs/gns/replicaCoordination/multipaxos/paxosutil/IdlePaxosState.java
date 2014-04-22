package edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil;
/**
@author V. Arun
 */
public class IdlePaxosState {
	String paxosID;
	short version;
	int[] members;
	
	int acceptorBallotNum;
	int acceptorBallotCoord;
	int acceptorSlot;
	int acceptorGCSlot;
	
	int coordBallotNum;
	int coordBallotCoord;
	int coordNextSlot;
	int[] acceptorGCSlots;
}
