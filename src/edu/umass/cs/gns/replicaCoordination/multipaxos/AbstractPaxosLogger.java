package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Timer;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;
import edu.umass.cs.gns.nsdesign.packet.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.StatePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.MessagingTask;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Messenger;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.SlotBallotState;

/**
 * @author V. Arun
 */

/* This quick-and-dirty logger is a memory-based logger, so it is fake. 
 * Not only is it fake, but it scales poorly as it uses up much more memory 
 * than the paxos instances themselves, so it will limit the number of instances
 * per machine. 
 * 
 * Use DerbyPaxosLogger that extends this class for a more scalable, efficient, 
 * and persistent logger.
 * 
 * Testing: Only shows that this logger barely scales to a few hundred 
 * thousand paxos instances.
 */
public abstract class AbstractPaxosLogger {
	public static final boolean DEBUG=PaxosManager.DEBUG;
	protected final int myID;
	protected final String logDirectory;
	protected static final Timer timer = new Timer();

	private static Logger log = Logger.getLogger(AbstractPaxosLogger.class.getName()); // GNS.getLogger();

	AbstractPaxosLogger(int id, String logDir) {
		this.myID = id;
		logDirectory = (logDir==null ? "." : logDir)+"/";
	}

	/************* Start of non-extensible methods **********************/
	/* Logs a message before sending another message.
	 */
	public static final void logAndMessage(AbstractPaxosLogger logger, LogMessagingTask logMTask, Messenger messenger) throws JSONException, IOException {
		assert(logMTask!=null);
		if(logMTask.logMsg!=null) {
			// spawn a log and message task
			if(DEBUG) log.info("Node " + logger.myID + " logging " + (logMTask.logMsg.getType().getLabel())+": " + logMTask.logMsg.toString());
			PaxosPacket packet = logMTask.logMsg;
			assert(packet.getPaxosID()!=null) : ("Null paxosID in " + packet); 
			assert(packet.getVersion()!=-1) : ("Null version in " + packet);

			int[] sb = PaxosLogTask.getSlotBallot(packet); assert(sb.length==3);
			PaxosLogTask task = new PaxosLogTask(logger, packet.getPaxosID(), packet.getVersion(), sb[0], new Ballot(sb[1], sb[2]), 
					packet.getType(), packet, messenger, logMTask);
			timer.schedule(task, 0);
		} else {
			// no logging, send right away
			messenger.send(logMTask);
		}
	}
	public static final void logAndExecute(AbstractPaxosLogger logger, PValuePacket decision, PaxosInstanceStateMachine pism) {
		PaxosLogTask task = new PaxosLogTask(logger, pism.getPaxosID(), pism.getVersion(), decision.slot, decision.ballot, 
				decision.getType(), decision, pism);
		timer.schedule(task, 0);
	}
	/* Will replay logged messages from checkpoint onwards. Static
	 * because logger could actually be any implementation, e.g.,
	 * DerbyPaxosLogger. */
	public final static void rollForward(AbstractPaxosLogger logger, String paxosID, Messenger messenger) {
		ArrayList<PaxosPacket> loggedMessages = logger.getLoggedMessages(paxosID);
		if(loggedMessages!=null) {
			for(PaxosPacket paxosMsg : loggedMessages) {
				MessagingTask mtask = new MessagingTask(logger.myID, paxosMsg);
				try {
					messenger.send(mtask);
				} catch (IOException e) {
					log.severe("IOException encountered while replaying logged message " + paxosMsg); e.printStackTrace();
				} catch(JSONException e) {
					log.severe("IOException encountered while replaying logged message " + paxosMsg); e.printStackTrace();
				} 
				// must continue, exceptions, warts, and all
			}
		}
	}

	public static final String listToString(ArrayList<PaxosPacket> list) {
		String s="\n---------------BEGIN log------------------\n";
		int count=0; int size=list.size();
		if(list!=null) for(PaxosPacket packet : list) {
			++count;
			if(size>25) {
				if(!(count>10 && count<=size-10)) // for pretty printing
					s+=(packet.getType()) + ":" + packet+"\n";
				else if (count==11) 
					s+="\n...(skipping "+(size-20)+" entries)\n\n";
			}
		}
		s+="---------------END log------------------";
		return s;
	}
	/************* End of non-extensible methods **********************/


	/************* Start of extensible methods ************************
	 * These methods can be implemented using a file, an embedded 
	 * database, or anything else. Not all of the methods are 
	 * independent nor do they all make sense for a generic,
	 * non-database logger.
	 */
	// getting meta-information about all paxos instances upon recovery
	public abstract ArrayList<RecoveryInfo> getAllPaxosInstances();

	// checkpointing methods
	public abstract String getCheckpointState(String paxosID);
	public abstract Ballot getCheckpointBallot(String paxosID);
	public abstract int getCheckpointSlot(String paxosID);
	public abstract SlotBallotState getSlotBallotState(String paxosID);
	public abstract void putCheckpointState(String paxosID, short version, int[] group, int slot, Ballot ballot, String state, int gcSlot);
	public abstract  StatePacket getStatePacket(String paxosID);
	protected abstract boolean initiateReadCheckpoints();
	protected abstract  RecoveryInfo readNextCheckpoint();

	protected abstract void closeReadAll();
	public abstract boolean remove(String paxosID);

	// message logging methods
	public abstract boolean log(PaxosPacket packet);
	public abstract boolean log(String paxosID, short version, int slot, int ballotnum, int coordinator, PaxosPacketType type, String message);
	public abstract ArrayList<PaxosPacket> getLoggedMessages(String paxosID); 
	public abstract Map<Integer,PValuePacket> getLoggedAccepts(String paxosID, int firstSlot);
	public abstract ArrayList<PValuePacket> getLoggedDecisions(String paxosID, int minSlot, int maxSlot) throws JSONException;
	protected abstract boolean initiateReadLogMessages(String paxosID);
	protected abstract PaxosPacket readNextLogMessage();

	/**************** End of extensible methods ***********************/

	public static void main(String[] args) {
		int million = 1000000;
		int nNodes = (int)(million*0.7);
		int numPackets = 10;
		AbstractPaxosLogger[] loggers = new AbstractPaxosLogger[nNodes];
		for(int i=0; i<nNodes;i++) {
			int[] group = {i, i+1, i+23, i+44};
			Ballot ballot = new Ballot(i,i);
			int slot = i;
			String state = "state"+i;
			loggers[i] = new DummyPaxosLogger(i,null);
			PaxosPacket[] packets = new PaxosPacket[numPackets];
			String paxosID = "paxos"+i;
			for(int j=0; j<packets.length; j++) {
				packets[j] = new RequestPacket(25,  "26", false);
				packets[j].putPaxosID(paxosID, (short)0);
				loggers[i].log(packets[j]);
			}
			loggers[i].putCheckpointState(paxosID, (short)0, group, slot, ballot, state, 0);
		}
	}
}