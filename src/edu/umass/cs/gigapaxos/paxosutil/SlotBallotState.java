package edu.umass.cs.gigapaxos.paxosutil;

/**
 * @author V. Arun
 * 
 *         Used by PaxosInstanceStateMachine and PaxosLogger to get checkpoint
 *         information. Just a container class.
 */
@SuppressWarnings("javadoc")
public class SlotBallotState {
	public final int slot;
	public final int ballotnum;
	public final int coordinator;
	/**
	 * The checkpointed state.
	 */
	public final String state;
	
	final int version;
	final long createTime;

	public SlotBallotState(int s, int bn, int c, int version, long createTime) {
		this.slot = s;
		this.ballotnum = bn;
		this.coordinator = c;
		this.state = null;
		this.version = version;
		this.createTime = createTime;
	}

	public SlotBallotState(int s, int bn, int c, String st, int version, long createTime) {
		this.slot = s;
		this.ballotnum = bn;
		this.coordinator = c;
		this.state = st;
		this.version = version;
		this.createTime = createTime;
	}

	public int getSlot() {
		return this.slot;
	}

	public int getBallotnum() {
		return this.ballotnum;
	}

	public int getCoordinator() {
		return this.coordinator;
	}
	
	public int getVersion() {
		return this.version;
	}
	public long getCreateTime() {
		return this.createTime;
	}

	public String toString() {
		return "[slot=" + slot + ", ballot=" + ballotnum + ":" + coordinator
				+ ", state = " + state + "]";
	}
}
