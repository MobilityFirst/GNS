package edu.umass.cs.gigapaxos.paxosutil;

import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;

/**
 * @author V. Arun
 * 
 *         Utility class extending MessagingTask by adding a to-log message.
 */
@SuppressWarnings("javadoc")
public class LogMessagingTask extends MessagingTask {
	/**
	 * The logMessage that needs to be persistently logged and then transmitted
	 * upon successful logging.
	 */
	public final PaxosPacket logMsg;

	public LogMessagingTask(int r, PaxosPacket p, PaxosPacket toLog) {
		super(r, p);
		assert (p != null);
		this.logMsg = toLog;
		assert (logMsg != null);
	}

	public LogMessagingTask(PaxosPacket toLog) {
		super(new int[0], new PaxosPacket[0]);
		this.logMsg = toLog;
		assert (logMsg != null);
	}

	public void putPaxosIDVersion(String paxosID, int version) {
		super.putPaxosIDVersion(paxosID, version);
		if (this.logMsg != null)
			this.logMsg.putPaxosID(paxosID, version);
	}

	public boolean isEmpty() {
		return super.isEmpty() && (this.logMsg == null);
	}

	public String toString() {
		return "toLog = " + (logMsg.getType()) + ": " + logMsg + "; toMsg = "
				+ super.toString();
	}
}
