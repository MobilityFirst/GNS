package edu.umass.cs.gns.replicaCoordination.multipaxos;

import edu.umass.cs.gns.packet.PaxosPacket;

/**
@author V. Arun
 */
public class LogMessagingTask extends MessagingTask {
	protected final PaxosPacket logMsg;
	
	public LogMessagingTask(int r, PaxosPacket p, PaxosPacket toLog) {
		super(r, p);
		this.logMsg = toLog;
	}

	public LogMessagingTask(PaxosPacket toLog) {
		super(new int[0], new PaxosPacket[0]);
		this.logMsg = toLog;
	}
}
