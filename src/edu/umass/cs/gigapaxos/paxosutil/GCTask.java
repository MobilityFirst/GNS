package edu.umass.cs.gigapaxos.paxosutil;

public class GCTask {
	public final String paxosID;
	public final short version;
	public final Ballot ballot;
	public final int gcSlot;
	public final int lastCPSlot;

	public GCTask(String paxosID, short version, int gcSlot, Ballot ballot, int lastCPSlot) {
		this.paxosID = paxosID;
		this.version = version;
		this.ballot = ballot;
		this.gcSlot = gcSlot;
		this.lastCPSlot = lastCPSlot;
	}
}