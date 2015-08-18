/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PreparePacket;
import edu.umass.cs.gigapaxos.paxospackets.StatePacket;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.ConsumerBatchTask;
import edu.umass.cs.gigapaxos.paxosutil.ConsumerTask;
import edu.umass.cs.gigapaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gigapaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.MessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.Messenger;
import edu.umass.cs.gigapaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gigapaxos.paxosutil.SlotBallotState;
import edu.umass.cs.gigapaxos.paxosutil.StringContainer;
import edu.umass.cs.utils.DelayProfiler;

/**
 * @author V. Arun
 * 
 *         <p>
 * 
 *         This abstract class exists to make the logger pluggable. An example
 *         is a quick and dirty, fake, memory-based logger DummyPaxosLogger that
 *         was implemented long back but has been deprecated now. Use
 *         DerbyPaxosLogger that extends this class for a more scalable,
 *         efficient, and persistent logger.
 */
public abstract class AbstractPaxosLogger {
	// protected coz the pluggable logger needs it
	protected final int myID;
	// protected coz the pluggable logger needs it.
	protected final String logDirectory;

	private boolean aboutToClose = false;

	private synchronized boolean isAboutToClose() {
		return this.aboutToClose;
	}

	private synchronized void setAboutToClose() {
		this.aboutToClose = true;
	}

	private static ArrayList<AbstractPaxosLogger> instances = new ArrayList<AbstractPaxosLogger>();

	private final BatchedLogger batchLogger;
	private final Messenger<?> messenger;
	private final Checkpointer collapsingCheckpointer;

	private static Logger log = Logger.getLogger(AbstractPaxosLogger.class
			.getName());

	protected AbstractPaxosLogger(int id, String logDir, Messenger<?> msgr) {
		this.myID = id;
		logDirectory = (logDir == null ? SQLPaxosLogger.LOG_DIRECTORY : logDir) + "/";
		this.messenger = msgr;
		(this.batchLogger = new BatchedLogger(
				new ArrayList<LogMessagingTask>(), this, this.messenger))
				.start(AbstractPaxosLogger.class.getSimpleName()+myID);
		;
		// checkpoint thread is not used and Checkpointer is deprecated
		this.collapsingCheckpointer = new Checkpointer(
				new HashMap<String, CheckpointTask>());//.start(AbstractPaxosLogger.class.getSimpleName()+myID);
		addLogger(this);
	}

	/* ************ Start of non-extensible methods ********************* */

	/**
	 * Logs a message **before** sending the reply message.
	 * 
	 * @param logger
	 *            The logger.
	 * @param logMTask
	 *            The logAndMessage task.
	 * @param messenger
	 *            The messenger that will send the message after logging.
	 * @throws JSONException
	 * @throws IOException
	 */
	protected static final void logAndMessage(AbstractPaxosLogger logger,
			LogMessagingTask logMTask//, Messenger<?> messenger
			)
			throws JSONException, IOException {
		// don't accept new work if about to close
		if (logger.isAboutToClose())
			return;
		assert (logMTask != null);
		// if no log message, just send right away
		if (logMTask.logMsg == null) {
			logger.messenger.send(logMTask);
			return;
		}
		// else spawn a log-and-message task
		PaxosPacket packet = logMTask.logMsg;
		log.log(Level.FINE, "{0}{1}{2}{3}{4}{5}", new Object[] { "Node ",
				logger.myID, " logging ", (packet.getType()), ": ", packet.getSummary(log.isLoggable(Level.FINE)) });
		assert (packet.getPaxosID() != null) : ("Null paxosID in " + packet);
		logger.batchLogger.enqueue(logMTask); // batchLogger will also send
	}

	// Will log and execute a decision. The former need not happen before the
	// latter.
	protected static final void logDecision(AbstractPaxosLogger logger,
			PValuePacket decision, PaxosInstanceStateMachine pism) {
		if (logger.isAboutToClose())
			return;

		logger.batchLogger.enqueue(new LogMessagingTask(decision));
	}

	/*
	 * FIXME: This method is unused. It was designed to offload checkpointing to
	 * its own task so that the paxos instance could move on. But there is no
	 * safe and efficient way for the instance to move on unless the checkpoint
	 * is complete, so we can not offload checkpoints to a worker after all.
	 */
	protected static final void checkpoint(AbstractPaxosLogger logger,
			String paxosID, int version, int[] members, int slot,
			Ballot ballot, String state, int gcSlot) {
		if (logger.isAboutToClose())
			return;

		CheckpointTask checkpointer = logger.new CheckpointTask(logger,
				paxosID, version, members, slot, ballot, state, gcSlot);
		logger.collapsingCheckpointer.enqueue(checkpointer);
	}

	/*
	 * Will replay logged messages from checkpoint onwards. Static because
	 * logger could actually be any implementation, e.g., DerbyPaxosLogger.
	 */
	protected final static void rollForward(AbstractPaxosLogger logger,
			String paxosID, Messenger<?> messenger) {
		if (logger.isAboutToClose())
			return;

		ArrayList<PaxosPacket> loggedMessages = logger
				.getLoggedMessages(paxosID);
		if (loggedMessages == null)
			return;
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
	 * Needed in order to first remove the (at most one) pending checkpoint and
	 * then invoke the child's remove method.
	 */
	protected static final boolean kill(AbstractPaxosLogger logger,
			String paxosID, int version) {
		logger.collapsingCheckpointer.dequeue(paxosID);
		// there isn't an easy way to remove pending log messages
		return logger.remove(paxosID, version);
	}

	protected static final String listToString(ArrayList<PaxosPacket> list) {
		String s = "\n---------------BEGIN log------------------\n";
		int count = 0;
		int size = list.size();
		if (list != null) {
			for (PaxosPacket packet : list) {
				++count;
				if (size > 25) {
					if (!(count > 10 && count <= size - 10)) // for pretty
																// printing
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
		this.batchLogger.waitToFinish(); // wait for ongoing messages to be
											// logged
		this.collapsingCheckpointer.waitToFinish(); // wait for ongoing
													// checkpoints
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

	/*
	 * Start of extensible methods. These methods can be implemented using a
	 * file, an embedded database, or anything else. Not all of the methods are
	 * independent nor do they all make sense for a generic, non-database
	 * logger.
	 */

	// checkpointing methods
	/**
	 * 
	 * @param paxosID
	 * @return Checkpointed state for paxosID.
	 */
	public abstract String getCheckpointState(String paxosID);

	/**
	 * @param paxosID
	 * @return Ballot corresponding to the latest (executed request in)
	 *         checkpoint.
	 */
	public abstract Ballot getCheckpointBallot(String paxosID);

	/**
	 * @param paxosID
	 * @return Slot corresponding to the most recent executed request in the
	 *         checkpoint.
	 */
	public abstract int getCheckpointSlot(String paxosID);

	/**
	 * @param paxosID
	 * @return Slot, ballot, and checkpoint state.
	 */
	public abstract SlotBallotState getSlotBallotState(String paxosID);

	/**
	 * 
	 * @param paxosID
	 * @param version
	 * @return Checks version and returns non-null only if the version matches.
	 */
	public abstract SlotBallotState getSlotBallotState(String paxosID,
			int version); // useful to enforce version check upon recovery

	/**
	 * 
	 * @param paxosID
	 * @param version
	 * @param group
	 * @param slot
	 * @param ballot
	 * @param state
	 * @param gcSlot
	 */
	public abstract void putCheckpointState(String paxosID, int version,
			Set<String> group, int slot, Ballot ballot, String state, int gcSlot);

	/**
	 * 
	 * @param paxosID
	 * @param version
	 * @param group
	 * @param slot
	 * @param ballot
	 * @param state
	 * @param gcSlot
	 * @param createTime
	 */
	public abstract void putCheckpointState(String paxosID, int version,
			Set<String> group, int slot, Ballot ballot, String state, int gcSlot,
			long createTime);

	/**
	 * 
	 * @param paxosID
	 * @return Checkpointed state.
	 */
	public abstract StatePacket getStatePacket(String paxosID);

	/**
	 * Copies the final state at the end of an epoch (or version) into a
	 * separate place. Useful for reconfiguring gigapaxos groups.
	 * 
	 * @param paxosID
	 * @param version
	 * @return True if copied successfully.
	 */
	public abstract boolean copyEpochFinalCheckpointState(String paxosID,
			int version);

	/**
	 * 
	 * @param paxosID
	 * @param version
	 * @return Final checkpoint state of the epoch {@code paxosID:version}.
	 */
	public abstract StringContainer getEpochFinalCheckpointState(
			String paxosID, int version);

	/**
	 * @param paxosID
	 * @return The most recent final state version (possibly null).
	 */
	public abstract Integer getEpochFinalCheckpointVersion(String paxosID);

	/**
	 * 
	 * @param paxosID
	 * @param version
	 * @return Returns true if the final epoch state exists and is successfully
	 *         deleted.
	 */
	public abstract boolean deleteEpochFinalCheckpointState(String paxosID,
			int version);

	// recovery methods
	/**
	 * 
	 * @param paxosID
	 * @return Returns recovery info retrieved from the checkpoint database.
	 */
	public abstract RecoveryInfo getRecoveryInfo(String paxosID);

	/**
	 * Prepares a cursor in the DB so that the caller can start reading
	 * checkpoints one at a time. Cursor-based checkpoint reading is useful
	 * under recovery when a very large number of checkpoints may have to be
	 * read and restored sequentially.
	 * 
	 * @param b
	 * @return Returns true if the cursor to start reading checkpoints is
	 *         successfully created.
	 */
	public abstract boolean initiateReadCheckpoints(boolean b); // starts a
																// cursor

	/**
	 * Reads the next checkpoint after the cursor has been created by
	 * {@link #initiateReadCheckpoints(boolean)}.
	 * 
	 * @param b
	 * @return The next retrieved checkpoint.
	 */
	public abstract RecoveryInfo readNextCheckpoint(boolean b); // reads next
																// checkpoint

	/**
	 * @return Starts a cursor for sequentially reading all log messages across
	 *         all paxos groups from the database in order to replay them. This
	 *         one-pass recovery is much faster than replaying log messages for
	 *         each paxos group one at a time.
	 */
	public abstract boolean initiateReadMessages(); // starts a cursor

	/**
	 * Reads the next log message after the cursor has been initialized by
	 * {@link #initiateReadMessages()}.
	 * 
	 * @return Returns the next log message.
	 */
	public abstract PaxosPacket readNextMessage();

	// close and cleanup methods
	/**
	 * Closes the recovery cursor.
	 */
	public abstract void closeReadAll();

	/**
	 * Closes all connections to the database. The database can not be used
	 * beyond this point.
	 */
	public abstract void closeImpl();

	/**
	 * Removes all state for the paxos group {@code paxosID}
	 * 
	 * @param paxosID
	 * @param version
	 *            TODO
	 * @return Returns true if state was succcessfully removed (or didn't exist
	 *         anyway).
	 */
	public abstract boolean remove(String paxosID, int version);

	/**
	 * 
	 * @return Removes all paxos state (but keeps the database itself).
	 */
	public abstract boolean removeAll();

	// message logging methods
	/**
	 * Logs a single {@link PaxosPacket}.
	 * 
	 * @param packet
	 * @return Returns true if logged successfully.
	 */
	public abstract boolean log(PaxosPacket packet);

	/**
	 * @param packets
	 * @return Returns true if the entire batch is logged successfully.
	 */
	public abstract boolean logBatch(PaxosPacket[] packets);

	/**
	 * Gets a list of all logged messages for the paxos group {@code paxosID}.
	 * Used by rollForward. We don't need to bother about the version here
	 * because non-version-compliant logged messages if any will be rejected by
	 * PaxosInstanceStateMachine anyway.
	 * 
	 * @param paxosID
	 * @return The list of logged messages for the paxos group {@code paxosID}.
	 */
	public abstract ArrayList<PaxosPacket> getLoggedMessages(String paxosID);

	/**
	 * We must supply the version here for safety.
	 * 
	 * @param paxosID
	 * @param version
	 * @param firstSlot
	 * @return A map of logged ACCEPTs indexed by their integer slot numbers.
	 */
	public abstract Map<Integer, PValuePacket> getLoggedAccepts(String paxosID,
			int version, int firstSlot);

	/**
	 * 
	 * We must supply the version here for safety.
	 * 
	 * @param paxosID
	 * @param version
	 * @param minSlot
	 * @param maxSlot
	 * @return Returns logged decisions for the paxos group {@code paxosID}.
	 * @throws JSONException
	 */
	public abstract ArrayList<PValuePacket> getLoggedDecisions(String paxosID,
			int version, int minSlot, int maxSlot) throws JSONException;

	// pausing methods
	protected abstract boolean pause(String paxosID, String serialized);

	protected abstract HotRestoreInfo unpause(String paxosID);

	/**************** End of extensible methods ***********************/

	/**
	 * @param packet
	 * @return The slot and ballot as a 3-integer array.
	 */
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

	/******************* Private utility classes below *********************/
	// Makes sure that message logging batches as much as possible.

	private class BatchedLogger extends ConsumerBatchTask<LogMessagingTask> {

		private final AbstractPaxosLogger logger;
		private final Messenger<?> messenger;
		private ArrayList<LogMessagingTask> logMessages = new ArrayList<LogMessagingTask>();

		BatchedLogger(ArrayList<LogMessagingTask> lock,
				AbstractPaxosLogger logger, Messenger<?> messenger) {
			super(lock, new LogMessagingTask[0]);
			this.logMessages = lock;
			this.logger = logger;
			this.messenger = messenger;
		}

		@Override
		public void enqueueImpl(LogMessagingTask task) {
			this.logMessages.add(task);
		}

		@Override
		public LogMessagingTask dequeueImpl() {
			throw new RuntimeException(this.getClass().getName()
					+ ".dequeueImpl() should not have been called");
		}

		@Override
		public void process(LogMessagingTask task) {
			throw new RuntimeException(this.getClass().getName()
					+ ".process() should not have been called");
		}

		@Override
		public void process(LogMessagingTask[] lmTasks) {
			PaxosPacket[] packets = new PaxosPacket[lmTasks.length];
			for (int i = 0; i < lmTasks.length; i++)
				packets[i] = lmTasks[i].logMsg;

			// first log
			long t1 = System.currentTimeMillis();
			boolean logged = this.logger.logBatch(packets);
			this.setProcessing(false);
			if (!logged)
				return;
			DelayProfiler.updateDelay("log", t1, packets.length);

			// then message if successfully logged
			{
				try {
					this.messenger.send(lmTasks);
				} catch (JSONException | IOException e) {
					log.severe("Logged message but could not send response: "
							+ lmTasks);
					e.printStackTrace();
				}
			}
		}
	}

	// FIXME: Unused convenience container for a single checkpoint task.
	@Deprecated
	private class CheckpointTask {
		final AbstractPaxosLogger logger;
		final String paxosID;
		final int version;
		final int[] members;
		final int slot;
		final Ballot ballot;
		final String state;
		final int gcSlot;

		CheckpointTask(AbstractPaxosLogger logger, String paxosID, int version,
				int[] members, int slot, Ballot ballot, String state, int gcSlot) {
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
			logger.putCheckpointState(paxosID, version, this.getDeprecatedGarbage(members), slot, ballot,
					state, gcSlot);
		}
		
		@Deprecated
		private Set<String> getDeprecatedGarbage(int[] members) {
			throw new RuntimeException("This method should never have been invoked");
		}
	}

	private class Checkpointer extends ConsumerTask<CheckpointTask> {
		// used for synchronizing.
		private final HashMap<String, CheckpointTask> checkpoints;

		Checkpointer(HashMap<String, CheckpointTask> lock) {
			super(lock);
			this.checkpoints = lock;
		}

		@Override
		public void enqueueImpl(CheckpointTask newCP) {
			CheckpointTask oldCP = checkpoints.get(newCP.paxosID);
			if (oldCP == null || oldCP.slot < newCP.slot) {
				this.checkpoints.put(newCP.paxosID, newCP);
			}
		}

		@Override
		public CheckpointTask dequeueImpl() {
			CheckpointTask cp = null;
			for (Iterator<CheckpointTask> cpIter = checkpoints.values()
					.iterator(); cpIter.hasNext();) {
				cp = cpIter.next();
				cpIter.remove();
				break;
			}
			return cp;
		}

		@Override
		public void process(CheckpointTask task) {
			assert (task != null);
			task.checkpoint();
		}

		// used to clear any pending checkpoint
		private void dequeue(String paxosID) {
			synchronized (checkpoints) {
				this.checkpoints.remove(paxosID);
			}
		}

	}

	/******************************** End of private utility classes ****************************/

	static class Main {
		public static void main(String[] args) {
			System.out
					.println("FAILURE: This class can not be unit-tested, but can be tested using DerbyPaxosLogger");
		}
	}
}
