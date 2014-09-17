package edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;

/**
@author V. Arun
 */

/* Used by PaxosInstanceStateMachine and PaxosLogger to get 
 * checkpoint information. Just a container class.
 */
public class SlotBallotState {
	public final int slot;
	public final int ballotnum;
	public final NodeId<String> coordinator;
	public final String state;
	
	public SlotBallotState(int s, int bn, NodeId<String> c) {
		this.slot = s;
		this.ballotnum = bn;
		this.coordinator = c;
		this.state=null;
	}
	public SlotBallotState(int s, int bn, NodeId<String> c, String st) {
		this.slot = s;
		this.ballotnum = bn;
		this.coordinator = c;
		this.state=st;
	}

	public int getSlot() {return this.slot;}
	public int getBallotnum() {return this.ballotnum;}
	public NodeId<String> getCoordinator() {return this.coordinator;}
        @Override
	public String toString() {
		return "[slot="+slot+", ballot="+ballotnum+":"+coordinator+", state = "+ state+"]";
	}
}
