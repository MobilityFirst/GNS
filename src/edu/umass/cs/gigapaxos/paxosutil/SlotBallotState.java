package edu.umass.cs.gigapaxos.paxosutil;
/**
@author V. Arun
 */

/* Used by PaxosInstanceStateMachine and PaxosLogger to get 
 * checkpoint information. Just a container class.
 */
public class SlotBallotState {
	public final int slot;
	public final int ballotnum;
	public final int coordinator;
	public final String state;
	
	public SlotBallotState(int s, int bn, int c) {
		this.slot = s;
		this.ballotnum = bn;
		this.coordinator = c;
		this.state=null;
	}
	public SlotBallotState(int s, int bn, int c, String st) {
		this.slot = s;
		this.ballotnum = bn;
		this.coordinator = c;
		this.state=st;
	}

	public int getSlot() {return this.slot;}
	public int getBallotnum() {return this.ballotnum;}
	public int getCoordinator() {return this.coordinator;}
	public String toString() {
		return "[slot="+slot+", ballot="+ballotnum+":"+coordinator+", state = "+ state+"]";
	}
}
