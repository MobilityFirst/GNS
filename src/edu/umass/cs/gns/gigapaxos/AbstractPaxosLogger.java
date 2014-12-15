package edu.umass.cs.gns.gigapaxos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gns.gigapaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PaxosPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PreparePacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.StatePacket;
import edu.umass.cs.gns.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gns.gigapaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gns.gigapaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gns.gigapaxos.paxosutil.MessagingTask;
import edu.umass.cs.gns.gigapaxos.paxosutil.Messenger;
import edu.umass.cs.gns.gigapaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gns.gigapaxos.paxosutil.SlotBallotState;
import edu.umass.cs.gns.util.DelayProfiler;

/**
 * @author V. Arun
 */

/*
 * This abstract class exists to make the logger pluggable. An example is a quick and dirty, fake,
 * memory-based logger DummyPaxosLogger that was implemented long back but it not maintained
 * anymore. Use DerbyPaxosLogger that extends this class for a more scalable, efficient, and
 * persistent logger.
 */
public abstract class AbstractPaxosLogger {
	protected final int myID; // protected coz the pluggable logger needs it
	protected final String logDirectory; // protected coz the pluggable logger needs it

	private boolean aboutToClose = false;

	private synchronized boolean isAboutToClose() {
		if (myID == 100 && this.aboutToClose)
			System.out.println("isAboutToClose check" + myID);
		return this.aboutToClose;
	}

	private synchronized void setAboutToClose() {
		this.aboutToClose = true;
	}

	private static ArrayList<AbstractPaxosLogger> instances = new ArrayList<AbstractPaxosLogger>();

	private final BatchLogger batchLogger;
	private final Messenger<?> messenger;
	private final CCheckpointer collapsingCheckpointer;

	private static Logger log = PaxosManager.getLogger();// Logger.getLogger(AbstractPaxosLogger.class.getName());

	protected AbstractPaxosLogger(int id, String logDir, Messenger<?> msgr) {
		this.myID = id;
		logDirectory = (logDir == null ? "." : logDir) + "/";
		this.messenger = msgr;
		this.batchLogger = new BatchLogger(this, this.messenger);
		(new Thread(this.batchLogger)).start();
		this.collapsingCheckpointer = new CCheckpointer();
		(new Thread(this.collapsingCheckpointer)).start();
		addLogger(this);
	}

	/************* Start of non-extensible methods **********************/
	// Logs a message **before** sending the reply message.
	public static final void logAndMessage(AbstractPaxosLogger logger,
			LogMessagingTask logMTask, Messenger<?> messenger)
			throws JSONException, IOException {
		if (logger.isAboutToClose())
			return;
		assert (logMTask != null);
		if (logMTask.logMsg != null) {
			messenger.send(logMTask); // no logging, just send right away
			return;
		}
		// else spawn a log-and-message task
		log.log(Level.FINE, "{0}{1}{2}{3}{4}{5}",
				new Object[] { "Node ", logger.myID, " logging ",
						(logMTask.logMsg.getType().getLabel()), ": ",
						logMTask.logMsg.toString() });
		PaxosPacket packet = logMTask.logMsg;
		assert (packet.getPaxosID() != null) : ("Null paxosID in " + packet);
		logger.batchLogger.enqueue(logMTask); // batchLogger will also send
	}

	// Will log and execute a decision. The former need not happen before the latter.
	public static final void logDecision(AbstractPaxosLogger logger,
			PValuePacket decision, PaxosInstanceStateMachine pism) {
		if (logger.isAboutToClose())
			return;

		logger.batchLogger.enqueue(new LogMessagingTask(decision));
	}

	// Designed to offload checkpointing to its own task so that the paxos instance can move on.
	public static final void checkpoint(AbstractPaxosLogger logger,
			String paxosID, short version, int[] members, int slot,
			Ballot ballot, String state, int gcSlot) {
		if (logger.isAboutToClose())
			return;

		CheckpointTask checkpointer = logger.new CheckpointTask(logger,
				paxosID, version, members, slot, ballot, state, gcSlot);
		logger.collapsingCheckpointer.enqueue(checkpointer);
	}

	/*
	 * Will replay logged messages from checkpoint onwards. Static because logger could actually be
	 * any implementation, e.g., DerbyPaxosLogger.
	 */
	public final static void rollForward(AbstractPaxosLogger logger,
			String paxosID, Messenger<?> messenger) {
		if (logger.isAboutToClose())
			return;

		ArrayList<PaxosPacket> loggedMessages = logger
				.getLoggedMessages(paxosID);
		if (loggedMessages == null) return;
		for (PaxosPacket paxosMsg : loggedMessages) {
			MessagingTask mtask = new MessagingTask(logger.myID,
					PaxosPacket.markRecovered(paxosMsg));
			try {
				log.log(Level.FINE, "{0}{1}{2}{3}", new Object[] { "Node ",
						logger.myID, " rolling forward ", mtask });
			messenger.send(mtask);
			} catch (IOException e) {
				log.severe("IOException encountered while replaying logged message "
						+ paxosMsg);
				e.printStackTrace();
			} catch (JSONException e) {
				log.severe("IOException encountered while replaying logged message "
						+ paxosMsg);
				e.printStackTrace();
			}
			// must continue, exceptions, warts, and all
		}
	}

	/*
	 * Needed in order to first remove the (at most one) pending checkpoint and then invoke the
	 * child's remove method.
	 */
	public static final boolean kill(AbstractPaxosLogger logger, String paxosID) {
		logger.collapsingCheckpointer.dequeue(paxosID);
		return logger.remove(paxosID);
	}

	public static final String listToString(ArrayList<PaxosPacket> list) {
		String s = "\n---------------BEGIN log------------------\n";
		int count = 0;
		int size = list.size();
		if (list != null) {
			for (PaxosPacket packet : list) {
				++count;
				if (size > 25) {
					if (!(count > 10 && count <= size - 10)) // for pretty printing
						s += (packet.getType()) + ":" + packet + "\n";
					else if (count == 11)
						s += "\n...(skipping " + (size - 20) + " entries)\n\n";
				}
			}
		}
		s += "---------------END log------------------";
		return s;
	}

	protected void stop() {
		this.batchLogger.stop();
		this.collapsingCheckpointer.stop();
	}

	protected void close() {
		this.setAboutToClose(); // stop accepting new log/checkpoint requests
		this.batchLogger.waitToFinish(); // wait for ongoing messages to be logged
		this.collapsingCheckpointer.waitToFinish(); // wait for ongoing checkpoints
		this.stop(); // stop the logger and checkpointer threads
		this.closeImpl(); // call close implementation
	}

	private static void addLogger(AbstractPaxosLogger logger) {
		synchronized (AbstractPaxosLogger.instances) {
			if (!AbstractPaxosLogger.instances.contains(logger)) {
				AbstractPaxosLogger.instances.add(logger);
			}
		}
	}

	/************* End of non-extensible methods **********************/

	/*************
	 * Start of extensible methods ************************ These methods can be implemented using a
	 * file, an embedded database, or anything else. Not all of the methods are independent nor do
	 * they all make sense for a generic, non-database logger.
	 * 
	 */

	// checkpointing methods
	public abstract String getCheckpointState(String paxosID);

	public abstract Ballot getCheckpointBallot(String paxosID);

	public abstract int getCheckpointSlot(String paxosID);

	public abstract SlotBallotState getSlotBallotState(String paxosID);

	public abstract SlotBallotState getSlotBallotState(String paxosID,
			short version); // useful to enforce version check upon recovery

	public abstract void putCheckpointState(String paxosID, short version,
			int[] group, int slot, Ballot ballot, String state, int gcSlot);

	public abstract StatePacket getStatePacket(String paxosID);

	// recovery methods
	public abstract RecoveryInfo getRecoveryInfo(String paxosID);

	public abstract boolean initiateReadCheckpoints(boolean b); // starts a cursor

	public abstract RecoveryInfo readNextCheckpoint(boolean b); // reads next checkpoint

	public abstract boolean initiateReadMessages(); // starts a cursor

	public abstract PaxosPacket readNextMessage(); // reads next checkpoint

	// close and cleanup methods
	public abstract void closeReadAll(); // closes the recovery cursor

	// public abstract void waitToFinish(); // waits for ongoing writes to finish

	public abstract void closeImpl(); // closes connections to the database

	public abstract boolean remove(String paxosID); // removes all paxosID state

	public abstract boolean removeAll(); // removes all paxos state (but keeps the database)

	// message logging methods
	public abstract boolean log(PaxosPacket packet); // logs a single PaxosPacket

	public abstract boolean logBatch(PaxosPacket[] packets); // batch logging

	public abstract ArrayList<PaxosPacket> getLoggedMessages(String paxosID);

	public abstract Map<Integer, PValuePacket> getLoggedAccepts(String paxosID,
			int firstSlot);

	public abstract ArrayList<PValuePacket> getLoggedDecisions(String paxosID,
			int minSlot, int maxSlot) throws JSONException;

	// pausing methods
	protected abstract boolean pause(String paxosID, String serialized);

	protected abstract HotRestoreInfo unpause(String paxosID);

	/**************** End of extensible methods ***********************/

	// A utility method with seemingly no other place to put
	public static int[] getSlotBallot(PaxosPacket packet) {
		int slot = -1;
		Ballot ballot = null;
		PValuePacket pvalue = null;
		switch (packet.getType()) {
		case PREPARE:
			PreparePacket prepare = (PreparePacket) packet;
			ballot = prepare.ballot;
			break;
		case ACCEPT:
		case DECISION:
			pvalue = (PValuePacket) packet;
			slot = pvalue.slot;
			ballot = pvalue.ballot;
			break;
		default:
			assert (false);
		}
		assert (ballot != null);
		int[] slotBallot = { slot, ballot.ballotNumber, ballot.coordinatorID };
		return slotBallot;
	}

	/*********************************** Private utility classes below ****************************/
	// Makes sure that message logging batches as much as possible.
	private class BatchLogger implements Runnable {
		private final AbstractPaxosLogger logger;
		private final Messenger<?> messenger;
		private final ArrayList<LogMessagingTask> logMessages = new ArrayList<LogMessagingTask>(); // sync
																									// object
		private boolean processing = false;
		private boolean stopped = false;

		BatchLogger(AbstractPaxosLogger lgr, Messenger<?> msgr) {
			this.logger = lgr;
			this.messenger = msgr;
		}

		public void run() {
			while (true) {
				while (this.isEmpty() && !isStopped())
					this.logMessagesWait();
				assert (!isEmpty() || isStopped());

				if (isEmpty() && isStopped())
					break;

				this.setProcessing(true);
				LogMessagingTask[] lmTasks = this.dequeueAll();
				PaxosPacket[] packets = new PaxosPacket[lmTasks.length];
				for (int i = 0; i < lmTasks.length; i++) {
					packets[i] = lmTasks[i].logMsg;
				}

				long t1 = System.currentTimeMillis();
				this.logger.logBatch(packets); // the main point of this class
				this.setProcessing(false);
				DelayProfiler.update("log", t1, packets.length);

				this.logMessagesNotify();

				for (LogMessagingTask lmTask : lmTasks) {
					try {
						this.messenger.send(lmTask);
					} catch (JSONException je) {
						log.severe("Logged message but could not send response: "
								+ lmTask);
						je.printStackTrace();
					} catch (IOException ioe) {
						log.severe("Logged message but could not send response: "
								+ lmTask);
						ioe.printStackTrace();
					}
				}
			}
			log.log(Level.FINE, "{0}{1}{2}", new Object[] {"batchLogger", myID, " thread stopping"});
		}

		private void logMessagesWait() {
			synchronized (this.logMessages) {
				try {
					this.logMessages.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		private void logMessagesNotify() {
			synchronized (this.logMessages) {
				this.logMessages.notifyAll();
			}
		}

		private void enqueue(LogMessagingTask lmTask) {
			synchronized (this.logMessages) {
				this.logMessages.add(lmTask);
				this.logMessages.notify();
			}

		}

		// There is no dequeue(), just dequeueAll()
		private LogMessagingTask[] dequeueAll() {
			synchronized (this.logMessages) {
				LogMessagingTask[] lmTasks = new LogMessagingTask[this.logMessages
						.size()];
				this.logMessages.toArray(lmTasks);
				this.logMessages.clear();
				return lmTasks;
			}
		}

		private boolean isEmpty() {
			synchronized (this.logMessages) {
				return this.logMessages.isEmpty();
			}
		}

		private void waitToFinish() {
			while (!this.isEmpty() || this.getProcessing())
				this.logMessagesWait();
		}

		private boolean getProcessing() {
			synchronized (this.logMessages) {
				return this.processing;
			}
		}

		private void setProcessing(boolean b) {
			synchronized (this.logMessages) {
				this.processing = b;
			}
		}

		private void stop() {
			synchronized (this.logMessages) {
				this.stopped = true;
				this.logMessages.notify();
			}
		}

		private boolean isStopped() {
			synchronized (this.logMessages) {
				return this.stopped;
			}
		}
	}

	// Just a convenience container for a single checkpoint task
	private class CheckpointTask {
		final AbstractPaxosLogger logger;
		final String paxosID;
		final short version;
		final int[] members;
		final int slot;
		final Ballot ballot;
		final String state;
		final int gcSlot;

		CheckpointTask(AbstractPaxosLogger logger, String paxosID,
				short version, int[] members, int slot, Ballot ballot,
				String state, int gcSlot) {
			this.logger = logger;
			this.paxosID = paxosID;
			this.version = version;
			this.members = members;
			this.slot = slot;
			this.ballot = ballot;
			this.state = state;
			this.gcSlot = gcSlot;
		}

		public void checkpoint() {
			logger.putCheckpointState(paxosID, version, members, slot, ballot,
					state, gcSlot);
		}
	}

	private class CCheckpointer implements Runnable {
		// used for synchronizing.
		private final HashMap<String, CheckpointTask> checkpoints = new HashMap<String, CheckpointTask>(); 
		private boolean processing = false;
		private boolean stopped = false;

		private void enqueue(CheckpointTask newCP) {
			synchronized (checkpoints) {
				CheckpointTask oldCP = checkpoints.get(newCP.paxosID);
				if (oldCP == null || oldCP.slot < newCP.slot) {
					this.checkpoints.put(newCP.paxosID, newCP);
					this.checkpoints.notify();
				}
			}
		}

		/*
		 * A dequeue is explicitly needed to remove any pending checkpoint when a paxos instance is
		 * killed. Otherwise, because checkpointing is an asynchronous operation, a checkpoint can
		 * get created after a paxos instance has been stopped and all other state has been removed.
		 */
		private void dequeue(String paxosID) {
			synchronized (checkpoints) {
				checkpoints.remove(paxosID);
			}
		}

		private CheckpointTask dequeue() {
			synchronized (checkpoints) {
				CheckpointTask cp = null;
				for (Iterator<CheckpointTask> cpIter = checkpoints.values()
						.iterator(); cpIter.hasNext();) {
					cp = cpIter.next();
					cpIter.remove();
					break;
				}
				return cp;
			}
		}

		private boolean isEmpty() {
			synchronized (this.checkpoints) {
				return this.checkpoints.isEmpty();
			}
		}

		private void checkpointsWait() {
			synchronized (this.checkpoints) {
				try {
					this.checkpoints.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		private void checkpointsNotify() {
			synchronized (this.checkpoints) {
				this.checkpoints.notifyAll();
			}
		}

		private void waitToFinish() {
			while (!this.isEmpty() || this.getProcessing())
				this.checkpointsWait();
		}

		private boolean getProcessing() {
			synchronized (this.checkpoints) {
				return this.processing;
			}
		}

		private void setProcessing(boolean b) {
			synchronized (this.checkpoints) {
				this.processing = b;
			}
		}

		public void run() {
			while (true) {
				while (this.isEmpty() && !isStopped())
					this.checkpointsWait();

				assert (!isEmpty() || isStopped());
				if (this.checkpoints.isEmpty() && isStopped())
					break;

				this.setProcessing(true);
				CheckpointTask checkpointTask = this.dequeue();
				assert (checkpointTask != null);
				checkpointTask.checkpoint();
				this.setProcessing(false);
				this.checkpointsNotify();
			}
			log.log(Level.FINE, "{0}{1}{2}", new Object[] {"CCheckpointer", myID, " thread stopping"});
		}

		public void stop() {
			synchronized (this.checkpoints) {
				this.stopped = true;
				this.checkpoints.notify();
			}
		}

		private boolean isStopped() {
			synchronized (this.checkpoints) {
				return this.stopped;
			}
		}
	}

	/******************************** End of private utility classes ****************************/

	public static void main(String[] args) {
		System.out
				.println("FAILURE: This class can not be unit-tested, but can be tested using DerbyPaxosLogger");
	}
}
