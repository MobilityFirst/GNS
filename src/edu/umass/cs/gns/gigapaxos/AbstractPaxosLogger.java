package edu.umass.cs.gns.gigapaxos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gns.gigapaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PaxosPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PreparePacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.StatePacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gns.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gns.gigapaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gns.gigapaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gns.gigapaxos.paxosutil.MessagingTask;
import edu.umass.cs.gns.gigapaxos.paxosutil.Messenger;
import edu.umass.cs.gns.gigapaxos.paxosutil.PaxosInstrumenter;
import edu.umass.cs.gns.gigapaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gns.gigapaxos.paxosutil.SlotBallotState;

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
	protected final int myID; // protected coz the pluggable logger needs it
	protected final String logDirectory; // protected coz the pluggable logger needs it
	
	private static ArrayList<AbstractPaxosLogger> instances = new ArrayList<AbstractPaxosLogger>();
	protected static boolean firstConnect = true; // coz db needs a create=true flag just once
	protected static boolean closeCalled = false;
	protected static synchronized boolean firstClose() {boolean tmp = !closeCalled; closeCalled = true; return tmp;}
	protected static synchronized boolean isCloseCalled() {return closeCalled;}
	protected static boolean isFirstConnect() {boolean b=firstConnect; firstConnect=false; return b;}

	private final BatchLogger batchLogger;
	private final Messenger messenger;
	private final CCheckpointer collapsingCheckpointer;

	private static Logger log = Logger.getLogger(AbstractPaxosLogger.class.getName()); // GNS.getLogger();

	AbstractPaxosLogger(int id, String logDir, Messenger msgr) {
		this.myID = id;
		logDirectory = (logDir==null ? "." : logDir)+"/";
		this.messenger=msgr;
		this.batchLogger = new BatchLogger(this, this.messenger);
		(new Thread(this.batchLogger)).start();
		this.collapsingCheckpointer = new CCheckpointer();
		(new Thread(this.collapsingCheckpointer)).start();
		addLogger(this);
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
		if(!decision.isRecovery()) PaxosInstrumenter.update("EEC", t1);
	}

	// Designed to offload checkpointing to its own task so that the paxos instance can move on.
	public static final void checkpoint(AbstractPaxosLogger logger, String paxosID, short version, 
			int[] members, int slot, Ballot ballot, String state, int gcSlot) {
		CheckpointTask checkpointer = logger.new CheckpointTask(logger, paxosID, version, members, 
				slot, ballot, state, gcSlot);
		logger.collapsingCheckpointer.enqueue(checkpointer);
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
					if(DEBUG) log.info("Node " + logger.myID + " rolling forward " + mtask);
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
	
	/* Needed in order to first remove the (at most one) pending checkpoint
	 * and then invoke the child's remove method.
	 */
	public static final boolean kill(AbstractPaxosLogger logger, String paxosID) {
		logger.collapsingCheckpointer.dequeue(paxosID);
		return logger.remove(paxosID);
	}

	public static void waitToFinishAll() {
		for(AbstractPaxosLogger logger : AbstractPaxosLogger.instances) {
			logger.batchLogger.waitToFinish();
			logger.collapsingCheckpointer.waitToFinish();
			logger.waitToFinish();
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
	
	protected void stop() {
		this.batchLogger.stop();
		this.collapsingCheckpointer.stop();
	}

	private static void addLogger(AbstractPaxosLogger logger) {
		synchronized(AbstractPaxosLogger.instances) {
			if(!AbstractPaxosLogger.instances.contains(logger)) {
				AbstractPaxosLogger.instances.add(logger);
			}
		}
	}

	/************* End of non-extensible methods **********************/


	/************* Start of extensible methods ************************
	 * These methods can be implemented using a file, an embedded 
	 * database, or anything else. Not all of the methods are 
	 * independent nor do they all make sense for a generic,
	 * non-database logger.
	 * 
	 * FIXME: There is no good reason why the recovery methods are
	 * protected and others public. They could all be public or all
	 * be protected. They are called by either PaxosManager and/or
	 * by PaxosInstanceStateMachine. 
	 */

	// checkpointing methods
	public abstract String getCheckpointState(String paxosID);
	public abstract Ballot getCheckpointBallot(String paxosID);
	public abstract int getCheckpointSlot(String paxosID);
	public abstract SlotBallotState getSlotBallotState(String paxosID);
	public abstract SlotBallotState getSlotBallotState(String paxosID, short version, boolean matchVersion);
	public abstract void putCheckpointState(String paxosID, short version, int[] group, int slot, Ballot ballot, String state, int gcSlot);
	public abstract void putCheckpointState(String paxosID, short version, Set<String> group, int slot, Ballot ballot, String state, int gcSlot);
	public abstract  StatePacket getStatePacket(String paxosID);

	// recovery methods
	protected abstract ArrayList<RecoveryInfo> getAllPaxosInstances(); // not used anymore in favor of the cursor methods below
	protected abstract RecoveryInfo getRecoveryInfo(String paxosID);
	protected abstract boolean initiateReadCheckpoints(boolean b); // starts a cursor
	protected abstract  RecoveryInfo readNextCheckpoint(boolean b); // reads next checkpoint 
	protected abstract boolean initiateReadMessages(); // starts a cursor
	protected abstract  PaxosPacket readNextMessage(); // reads next checkpoint 
	protected abstract void closeReadAll();  // closes the cursor

	public abstract void waitToFinish();
	public abstract void close();
	public abstract boolean remove(String paxosID);
	public abstract boolean removeAll();

	// message logging methods
	public abstract boolean log(PaxosPacket packet);
	public abstract boolean log(String paxosID, short version, int slot, int ballotnum, int coordinator, PaxosPacketType type, String message);
	public abstract boolean logBatch(PaxosPacket[] packets);
	public abstract ArrayList<PaxosPacket> getLoggedMessages(String paxosID); 
	public abstract Map<Integer,PValuePacket> getLoggedAccepts(String paxosID, int firstSlot);
	public abstract ArrayList<PValuePacket> getLoggedDecisions(String paxosID, int minSlot, int maxSlot) throws JSONException;

	// pausing methods
	protected abstract boolean pause(String paxosID, String serialized);
	protected abstract HotRestoreInfo unpause(String paxosID);

	/**************** End of extensible methods ***********************/

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
		private boolean processing = false;
		private boolean stopped = false;

		BatchLogger(AbstractPaxosLogger lgr, Messenger msgr) {
			this.logger = lgr;
			this.messenger = msgr;
		}
		public void run() {
			while(true) {
				synchronized(this.logMessages) {
					try {
						while(this.logMessages.isEmpty() && !isStopped()) this.logMessages.wait();
						if(isStopped()) break;
					} catch(InterruptedException ie) {ie.printStackTrace();}
				}
				LogMessagingTask[] lmTasks = this.dequeueAll();
				assert(lmTasks.length>0 || isStopped());
				PaxosPacket[] packets = new PaxosPacket[lmTasks.length];
				for(int i=0; i<lmTasks.length; i++) {
					packets[i] = lmTasks[i].logMsg;
				}

				this.setProcessing(true);
				long t1=System.currentTimeMillis();
				this.logger.logBatch(packets); // the main point of this class
				PaxosInstrumenter.update("log", t1, packets.length);
				this.setProcessing(false);

				synchronized(this) {
					notify(); // notify waitToFinish
				}

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
		private boolean isEmpty() {
			synchronized(this.logMessages) {
				return this.logMessages.isEmpty();
			}
		}
		private synchronized void waitToFinish() {
			try {
				while(!this.isEmpty() || this.getProcessing()) wait();
			} catch(InterruptedException ie) {ie.printStackTrace();}
		}
		private synchronized boolean getProcessing() {return this.processing;}
		private synchronized void setProcessing(boolean b) {this.processing=b;}
		
		private synchronized boolean stop() {
			boolean old = this.stopped;
			this.stopped = true;
			synchronized(this.logMessages) {
				this.logMessages.notify();
			}
			return !old;
		}
		private synchronized boolean isStopped() {
			return this.stopped;
		}
	}


	// Just a convenience container for a single checkpoint task
	private class CheckpointTask {
		final AbstractPaxosLogger logger; final String paxosID; 
		final short version; final int[] members; final int slot; 
		final Ballot ballot; final String state; final int gcSlot;
		CheckpointTask(AbstractPaxosLogger logger, String paxosID, short version, 
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
		public void checkpoint() {
			logger.putCheckpointState(paxosID, version, members, 
					slot, ballot, state, gcSlot);
		}
	}
	private class CCheckpointer implements Runnable {
		private final HashMap<String,CheckpointTask> checkpoints = new HashMap<String,CheckpointTask>();
		private boolean processing = false;
		private boolean stopped = false;

		private void enqueue(CheckpointTask newCP) {
			synchronized(checkpoints) {
				CheckpointTask oldCP = checkpoints.get(newCP.paxosID);
				if(oldCP==null || oldCP.slot < newCP.slot) {
					this.checkpoints.put(newCP.paxosID, newCP);
					this.checkpoints.notify();
				}
			}
		}
		/* A dequeue is explicitly needed to remove any pending checkpoint when a 
		 * paxos instance is killed. Otherwise, because checkpointing is an 
		 * asynchronous operation, a checkpoint can get created after a paxos
		 * instance has been stopped and all other state has been removed.
		 */
		private void dequeue(String paxosID) {
			synchronized(checkpoints) {
				checkpoints.remove(paxosID);
			}
		}
		private CheckpointTask dequeue() {
			synchronized(checkpoints) {
				CheckpointTask cp = null;
				for(Iterator<CheckpointTask> cpIter = checkpoints.values().iterator(); cpIter.hasNext();) {
					cp = cpIter.next();
					cpIter.remove();
					break;
				}
				return cp;
			}
		}
		private boolean isEmpty() {
			synchronized(this.checkpoints) {
				return this.checkpoints.isEmpty();
			}
		}
		private synchronized void waitToFinish() {
			try {
				while(!this.isEmpty() || this.getProcessing()) {
					this.wait();
				}
			} catch(InterruptedException ie) {ie.printStackTrace();}
		}
		private synchronized boolean getProcessing() {return this.processing;}
		private synchronized void setProcessing(boolean b) {this.processing=b;}

		public void run() {
			while(true) {
				synchronized(this.checkpoints) {
					try {
						while(this.checkpoints.isEmpty() && !isStopped()) this.checkpoints.wait();
						if(isStopped()) break;
					} catch(InterruptedException ie) {ie.printStackTrace();}
				}
				CheckpointTask checkpointTask = this.dequeue();
				assert(checkpointTask!=null);
				this.setProcessing(true);
				checkpointTask.checkpoint();
				this.setProcessing(false);
				synchronized(this) {this.notifyAll();}
			}
		}
		public synchronized boolean stop() {
			boolean old = this.stopped;
			this.stopped = true;
			synchronized(this.checkpoints) {
				this.checkpoints.notify();
			}
			return !old;
		}
		private synchronized boolean isStopped() {
			return this.stopped;
		}
	}

	/******************************** End of private utility classes ****************************/

	/*************************** Instrumentation methods below ************************/
}
