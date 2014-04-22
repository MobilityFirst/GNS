package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;
import edu.umass.cs.gns.nsdesign.packet.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PreparePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.StatePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.MessagingTask;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Messenger;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.SlotBallotState;

/**
 * @author V. Arun
 */

/* This abstract class exists to make the logger pluggable. An example is a quick
 * and dirty, fake, memory-based logger DummyPaxosLogger that was implemented long
 * back but it not maintained anymore. Use DerbyPaxosLogger that extends this class 
 * for a more scalable, efficient, and persistent logger.
 */
public abstract class AbstractPaxosLogger {
	public static final boolean DEBUG=PaxosManager.DEBUG;
	protected final int myID;
	protected final String logDirectory;

	private final BatchLogger batchLogger;
	private final Messenger messenger;
	Timer timer = new Timer(); // for checkpointing, so single thread is fine

	private static Logger log = Logger.getLogger(AbstractPaxosLogger.class.getName()); // GNS.getLogger();

	AbstractPaxosLogger(int id, String logDir, Messenger msgr) {
		this.myID = id;
		logDirectory = (logDir==null ? "." : logDir)+"/";
		this.messenger=msgr;
		this.batchLogger = new BatchLogger(this, this.messenger);
		(new Thread(this.batchLogger)).start();
	}

	/************* Start of non-extensible methods **********************/
	// Logs a message **before** sending the reply message.
	public static final void logAndMessage(AbstractPaxosLogger logger, LogMessagingTask logMTask, Messenger messenger) 
			throws JSONException, IOException {
		assert(logMTask!=null);
		if(logMTask.logMsg!=null) {
			// spawn a log-and-message task
			if(DEBUG) log.info("Node " + logger.myID + " logging " + (logMTask.logMsg.getType().getLabel())+
					": " + logMTask.logMsg.toString());
			PaxosPacket packet = logMTask.logMsg;
			assert(packet.getPaxosID()!=null) : ("Null paxosID in " + packet); 
			assert(packet.getVersion()!=-1) : ("Null version in " + packet);

			logger.batchLogger.enqueue(logMTask); // batchLogger will also send
		} else {
			messenger.send(logMTask); // no logging, send right away

		}
	}
	// Will log and execute a decision. The former need not happen before the latter.
	public static final void logAndExecute(AbstractPaxosLogger logger, PValuePacket decision, PaxosInstanceStateMachine pism) {
		logger.batchLogger.enqueue(new LogMessagingTask(decision));
		
		long t1=System.currentTimeMillis();
		pism.sendMessagingTask(pism.extractExecuteAndCheckpoint(decision));
		if(!decision.isRecovery()) updateExecTime(System.currentTimeMillis()-t1, 1);
	}

	// Designed to offload checkpointing to its own task so that the paxos instance can move on.
	public static final void checkpoint(AbstractPaxosLogger logger, String paxosID, short version, 
			int[] members, int slot, Ballot ballot, String state, int gcSlot) {
		Checkpointer checkpointer = logger.new Checkpointer(logger, paxosID, version, members, 
				slot, ballot, state, gcSlot);
		logger.timer.schedule(checkpointer, 0);
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
	protected abstract RecoveryInfo getRecoveryInfo(String paxosID);

	protected abstract void closeReadAll(); 
	public abstract boolean remove(String paxosID);
	public abstract boolean removeAll();

	// message logging methods
	public abstract boolean log(PaxosPacket packet);
	public abstract boolean log(String paxosID, short version, int slot, int ballotnum, int coordinator, PaxosPacketType type, String message);
	public abstract boolean logBatch(PaxosPacket[] packets);
	public abstract ArrayList<PaxosPacket> getLoggedMessages(String paxosID); 
	public abstract Map<Integer,PValuePacket> getLoggedAccepts(String paxosID, int firstSlot);
	public abstract ArrayList<PValuePacket> getLoggedDecisions(String paxosID, int minSlot, int maxSlot) throws JSONException;
	//protected abstract boolean initiateReadLogMessages(String paxosID);
	//protected abstract PaxosPacket readNextLogMessage();
	
	// pausing methods
	protected abstract boolean pause(String paxosID, String serialized);
	protected abstract HotRestoreInfo unpause(String paxosID);

	/**************** End of extensible methods ***********************/

	private static long totalLogTime=0;
	private static int numLogged=1;
	private static long totalExecTime=0;
	private static int numExecd=1;
	public static long totalLockTime=0;
	public static long numLocked=1;

	private synchronized static void updateLogTime(long t, int n) {totalLogTime += t;numLogged+=n;}
	protected synchronized static double getAvgLogTime() {return totalLogTime*1.0/numLogged;}
	
	private synchronized static void updateExecTime(long t, int n) {totalExecTime += t;numExecd+=n;}
	protected synchronized static double getAvgExecTime() {return totalExecTime*1.0/numExecd;}
	
	// A utility method with seemingly no other place to put
	public static int[] getSlotBallot(PaxosPacket packet) {
		int slot=-1;
		Ballot ballot=null;
		PValuePacket pvalue = null;
		switch(packet.getType()) {
		case PREPARE:
			PreparePacket prepare = (PreparePacket)packet;
			ballot = prepare.ballot;
			break;
		case ACCEPT: case DECISION:
			pvalue = (PValuePacket)packet;
			slot = pvalue.slot;
			ballot = pvalue.ballot;
			break;
		default:
			assert(false);
		}
		assert(ballot!=null);
		int[] slotBallot = {slot, ballot.ballotNumber, ballot.coordinatorID};
		return slotBallot;
	}
	
	/*********************************** Private utility classes below ****************************/
	// Makes sure that message logging batches as much as possible.
	private class BatchLogger implements Runnable {
		private final AbstractPaxosLogger logger;
		private final Messenger messenger;
		private final ArrayList<LogMessagingTask> logMessages = new ArrayList<LogMessagingTask>();

		BatchLogger(AbstractPaxosLogger lgr, Messenger msgr) {
			this.logger = lgr;
			this.messenger = msgr;
		}
		public void run() {
			while(true) {
				synchronized(this.logMessages) {
					try {
						while(this.logMessages.isEmpty()) this.logMessages.wait();

					} catch(InterruptedException ie) {ie.printStackTrace();}
				}
				LogMessagingTask[] lmTasks = this.dequeueAll();
				assert(lmTasks.length>0);
				PaxosPacket[] packets = new PaxosPacket[lmTasks.length];
				for(int i=0; i<lmTasks.length; i++) {
					packets[i] = lmTasks[i].logMsg;
				}

				long t1=System.currentTimeMillis();
				this.logger.logBatch(packets); // the main point of this class
				updateLogTime(System.currentTimeMillis()-t1, packets.length);

				for(LogMessagingTask lmTask : lmTasks) {
					try {
						this.messenger.send(lmTask);
					} catch(JSONException je) {
						log.severe("Logged message but could not send response: " + lmTask); 
						je.printStackTrace();
					}
					catch(IOException ioe) {
						log.severe("Logged message but could not send response: " + lmTask); 
						ioe.printStackTrace();
					}
				}
			}
		}
		private void enqueue(LogMessagingTask lmTask) {
			synchronized(this.logMessages) {
				this.logMessages.add(lmTask);
				this.logMessages.notify();
			}

		}
		// There is no dequeue(), just dequeueAll()
		private LogMessagingTask[] dequeueAll() {
			synchronized(this.logMessages) {
				LogMessagingTask[] lmTasks = new LogMessagingTask[this.logMessages.size()];
				this.logMessages.toArray(lmTasks);
				this.logMessages.clear();
				return lmTasks;
			}
		}
	}
	
	// Just spawns a task to do a single checkpoint
	private class Checkpointer extends TimerTask {
		final AbstractPaxosLogger logger; final String paxosID; 
		final short version; final int[] members; final int slot; 
		final Ballot ballot; final String state; final int gcSlot;
		Checkpointer(AbstractPaxosLogger logger, String paxosID, short version, 
				int[] members, int slot, Ballot ballot, String state, int gcSlot) {
			this.logger=logger;
			this.paxosID=paxosID;
			this.version=version;
			this.members=members;
			this.slot=slot;
			this.ballot=ballot;
			this.state=state;
			this.gcSlot=gcSlot;
		}
		public void run() {
			logger.putCheckpointState(paxosID, version, members, slot, ballot, state, gcSlot);
		}
	}
	/******************************** End of private utility classes ****************************/

}