package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.TimerTask;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;
import edu.umass.cs.gns.nsdesign.packet.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PreparePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Messenger;

/**
@author V. Arun
 */

/* FIXME: This class is not used anymore and will be removed soon.
 * 
 * This class is a task to log either the checkpoint or a log message.
 * It is somewhat wasteful as some fields are relevant to only one of 
 * the two, e.g., group and gcSlot only to checkpoint. 
 * 
 * FIXME: Needs to be cleaned up. Not enough code reuse.
 */
public final class PaxosLogTask extends TimerTask {
	public static final boolean DEBUG=PaxosManager.DEBUG;

	private final AbstractPaxosLogger paxosLogger;
	private final String paxosID;
	private final short version;
	private int[] group; // relevant only for checkpoint
	private final int slot;
	private final Ballot ballot;
	private final PaxosPacketType type;
	private final PaxosPacket toLog; // could be checkpoint state or log message depending on type
	private final int gcSlot;
	private final Messenger messenger; // relevant only for logAndMessage
	private final PaxosInstanceStateMachine paxosInstance; // relevant only for logAndExecute
	private final LogMessagingTask logMsgTask;

	private static int avgMsgLogTime=0;
	private static int numMsgLogs=1;
	private static int avgCPTime=0;
	private static int numCPs=1;

	private static Logger log = Logger.getLogger(PaxosLogTask.class.getName()); // GNS.getLogger();	

	// for log and message
	PaxosLogTask(AbstractPaxosLogger logger, String pid, short ver, int s, Ballot b, PaxosPacketType t, PaxosPacket packet, Messenger pm, LogMessagingTask lmTask) {
		paxosLogger=logger;
		paxosID=pid;
		this.version=ver;
		slot=s;
		ballot=b;
		type=t;
		toLog=packet;
		messenger = pm;
		paxosInstance=null;
		gcSlot=0;
		logMsgTask = lmTask;
	}
	// for log and execute
	PaxosLogTask(AbstractPaxosLogger logger, String pid, short ver, int s, Ballot b, PaxosPacketType t, PaxosPacket packet, PaxosInstanceStateMachine pism) {
		paxosLogger=logger;
		paxosID=pid;
		this.version=ver;
		slot=s;
		ballot=b;
		type=t;
		toLog=packet;
		messenger = null;
		paxosInstance = pism;
		gcSlot=0;
		logMsgTask = null;
	}
	// for checkpoint
	PaxosLogTask(AbstractPaxosLogger logger, String pid, short ver, int[] g, int s, Ballot b, PaxosPacket state, int gcs) {
		paxosLogger=logger;
		paxosID=pid;
		this.version=ver;
		group=g;
		slot=s;
		ballot=b;
		type=PaxosPacketType.CHECKPOINT_STATE;
		toLog=state;
		gcSlot=gcs;
		messenger=null;
		paxosInstance=null;
		logMsgTask=null;
	}
	
	public void run() {
		long t1 = System.currentTimeMillis();
		// switch for log task
		switch(type) {
		case PREPARE: case ACCEPT: case DECISION:
			String toLogString = null;
			try {
				toLogString = PaxosPacket.setRecovery(toLog);
			} catch(JSONException je) {je.printStackTrace();}

			//paxosLogger.log(paxosID, version, slot, ballot.ballotNumber, ballot.coordinatorID, type, toLogString);
			
			if(type==PaxosPacketType.PREPARE) {log.info("Node "+((PreparePacket)toLog).receiverID+ " logger " +
					"logging&replying to node "+((PreparePacket)toLog).coordinatorID+"'s PREPARE: "+this.logMsgTask);}
			avgMsgLogTime += (System.currentTimeMillis()-t1); numMsgLogs++;
			break;
		}
		try {
			// switch for message task
			switch(type) {
			case PREPARE: case ACCEPT:
				messenger.send(this.logMsgTask);
				break;
			case DECISION:
				PValuePacket decision = (PValuePacket)(this.toLog);
				// Can not use local messenger as that won't stamp the paxosID in missing decision requests
				paxosInstance.sendMessagingTask(paxosInstance.extractExecuteAndCheckpoint(decision));
				avgCPTime += (System.currentTimeMillis()-t1); numCPs++;
				break;
			}
		} catch(IOException ioe) {
			log.severe("Logged message but could not send response: " + logMsgTask); ioe.printStackTrace();
		} catch(JSONException je) {
			je.printStackTrace();
		}
	}

	public static double getAvgLogTime() {return avgMsgLogTime*1.0/numMsgLogs;}
	public static double getAvgCheckpointTime() {return avgCPTime*1.0/numCPs;}
	public static int getNumCheckpoints() {return numCPs;}


}
