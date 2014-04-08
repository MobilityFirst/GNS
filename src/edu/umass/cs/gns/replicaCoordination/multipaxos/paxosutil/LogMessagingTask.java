package edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil;

import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;

/**
@author V. Arun
 */
/* Utility class extending MessagingTask by adding a to-log message.
 */
public class LogMessagingTask extends MessagingTask {
	public final PaxosPacket logMsg;
	
	public LogMessagingTask(int r, PaxosPacket p, PaxosPacket toLog) {
		super(r, p);
		assert(p!=null);
		this.logMsg = toLog;
		assert(logMsg!=null);
	}

	public LogMessagingTask(PaxosPacket toLog) {
		super(new int[0], new PaxosPacket[0]);
		this.logMsg = toLog;
		assert(logMsg!=null);
	}

	public void putPaxosIDVersion(String paxosID, short version) {
		super.putPaxosIDVersion(paxosID, version);
		if(this.logMsg!=null) this.logMsg.putPaxosID(paxosID, version);
	}
	public boolean isEmpty() {
		return super.isEmpty() && (this.logMsg==null);
	}
	
	public String toString() {
		return "toLog = " + (logMsg.getType()) +": " + logMsg + "; \ntoMsg = " + super.toString();
	}
}
