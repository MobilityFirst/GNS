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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import org.json.JSONException;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.AcceptReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.BatchedAcceptReply;
import edu.umass.cs.gigapaxos.paxospackets.BatchedCommit;
import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.ConsumerTask;
import edu.umass.cs.gigapaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.MessagingTask;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class PaxosPacketBatcher extends ConsumerTask<PaxosPacket[]> {

	private final PaxosManager<?> paxosManager;
	private final HashMap<String, HashMap<Ballot, BatchedAcceptReply>> acceptReplies;
	private final HashMap<String, HashMap<Ballot, BatchedCommit>> commits;

	/**
	 * @param lock
	 * @param paxosManager
	 */
	private PaxosPacketBatcher(HashMapContainer lock,
			PaxosManager<?> paxosManager) {
		super(lock);
		this.acceptReplies = lock.acceptReplies;
		this.commits = lock.commits;
		this.paxosManager = paxosManager;
	}

	/**
	 * @param paxosManager
	 */
	public PaxosPacketBatcher(PaxosManager<?> paxosManager) {
		// we are using only one of the two structures to lock
		this(new HashMapContainer(), paxosManager);
	}

	// just to name the thread, otherwise super suffices
	public void start() {
		Thread me = (new Thread(this));
		me.setName(PaxosPacketBatcher.class.getSimpleName()
				+ this.paxosManager.getMyID());
		me.start();
	}

	@Override
	public void enqueueImpl(PaxosPacket[] tasks) {
		for (PaxosPacket task : tasks)
			switch (task.getType()) {
			case ACCEPT_REPLY:
				this.enqueueImpl((AcceptReplyPacket) task);
				break;
			case BATCHED_COMMIT:
				this.enqueueImpl((BatchedCommit) task);
				break;
			default:

			}
	}

	private boolean enqueueImpl(AcceptReplyPacket acceptReply) {
		if (!this.acceptReplies.containsKey(acceptReply.getPaxosID()))
			this.acceptReplies.put(acceptReply.getPaxosID(),
					new HashMap<Ballot, BatchedAcceptReply>());

		HashMap<Ballot, BatchedAcceptReply> arMap = this.acceptReplies
				.get(acceptReply.getPaxosID());
		boolean added = false;
		if (arMap.isEmpty())
			added = (arMap.put(acceptReply.ballot, new BatchedAcceptReply(
					acceptReply)) != null);
		else if (arMap.containsKey(acceptReply.ballot)) {
			added = arMap.get(acceptReply.ballot).addAcceptReply(acceptReply);
		} else {
			arMap.put(acceptReply.ballot, new BatchedAcceptReply(acceptReply));
		}

		return added;
	}

	private boolean enqueueImpl(BatchedCommit commit) {
		if (!this.commits.containsKey(commit.getPaxosID()))
			this.commits.put(commit.getPaxosID(),
					new HashMap<Ballot, BatchedCommit>());
		HashMap<Ballot, BatchedCommit> cMap = this.commits.get(commit
				.getPaxosID());
		boolean added = false;
		if (cMap.isEmpty())
			added = (cMap.put(commit.ballot, (commit)) != null);
		else if (cMap.containsKey(commit.ballot))
			added = cMap.get(commit.ballot).addBatchedCommit(commit);
		else
			added = (cMap.put(commit.ballot, (commit)) != null);

		return added;
	}

	@Override
	public PaxosPacket[] dequeueImpl() {

		PaxosPacket[] pkts = new PaxosPacket[2];
		pkts[0] = this.dequeueImplAR();
		pkts[1] = this.dequeueImplC();

		if (pkts[0] == null && pkts[1] != null)
			return pkts[1].toSingletonArray();
		else if (pkts[0] != null && pkts[1] == null)
			return pkts[0].toSingletonArray();

		return pkts;
	}

	private PaxosPacket dequeueImplAR() {
		BatchedAcceptReply batchedAR = null;
		if (!this.acceptReplies.isEmpty()) {
			Map<Ballot, BatchedAcceptReply> arMap = this.acceptReplies.values()
					.iterator().next();
			assert (!arMap.isEmpty());
			batchedAR = arMap.values().iterator().next();
			arMap.remove(batchedAR.ballot);
			if (arMap.isEmpty())
				this.acceptReplies.remove(batchedAR.getPaxosID());
		}
		return batchedAR;
	}

	public String toString() {
		return this.getClass().getSimpleName() + this.paxosManager.getMyID();
	}

	private PaxosPacket dequeueImplC() {
		BatchedCommit batchedCommit = null;
		if (!this.commits.isEmpty()) {
			Map<Ballot, BatchedCommit> cMap = this.commits.values().iterator()
					.next();
			assert (!cMap.isEmpty());
			batchedCommit = cMap.values().iterator().next();
			cMap.remove(batchedCommit.ballot);
			if (cMap.isEmpty())
				this.commits.remove(batchedCommit.getPaxosID());
		}
		return batchedCommit;

	}

	private void send(MessagingTask mtask) {
		try {
			PaxosManager
					.getLogger()
					.log(Level.FINE,
							"{0} sending batch {1}",
							new Object[] {
									this,
									(mtask.msgs[0] instanceof BatchedAcceptReply ? "batchedAR:"
											+ ((BatchedAcceptReply) mtask.msgs[0])
													.getAcceptedSlots().length
											: "batchedC:"
													+ ((BatchedCommit) mtask.msgs[0])
															.getCommittedSlots().length) });
			this.paxosManager.send(mtask, false, false);
		} catch (JSONException | IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(PaxosPacket[] tasks) {
		for (PaxosPacket task : tasks)
			if (task instanceof BatchedAcceptReply)
				this.send(new MessagingTask(
						((BatchedAcceptReply) task).ballot.coordinatorID, task));
			else if (task instanceof BatchedCommit)
				this.send(new MessagingTask(((BatchedCommit) task).getGroup(),
						task));
			else
				throw new RuntimeException("Should be impossible to get here");
	}

	private static boolean BATCHED_ACCEPT_REPLIES = Config
			.getGlobalBoolean(PC.BATCHED_ACCEPT_REPLIES);

	private static boolean BATCHED_COMMITS = Config
			.getGlobalBoolean(PC.BATCHED_COMMITS);

	private static boolean isUnbatchableDecision(MessagingTask mtask) {
		return !BATCHED_COMMITS
				&& mtask.msgs[0].getType() == PaxosPacketType.DECISION;
	}

	private static boolean isUnbatchableAcceptReplies(MessagingTask mtask) {
		return !BATCHED_ACCEPT_REPLIES
				&& mtask.msgs[0].getType() == PaxosPacketType.ACCEPT_REPLY;
	}

	/*
	 * Coalesces non-local messaging tasks and returns either the local part of
	 * the messaging task or the input mtask if unable to coalesce. The sender
	 * is expected to direct-send the returned messaging task. 
	 */
	private static final boolean SHORT_CIRCUIT_LOCAL = false;
	protected MessagingTask coalesce(MessagingTask mtask) {
		if (mtask == null || mtask.isEmptyMessaging()
				|| (allLocal(mtask) && SHORT_CIRCUIT_LOCAL)
				|| isUnbatchableAcceptReplies(mtask)
				|| isUnbatchableDecision(mtask))
			return mtask;

		MessagingTask nonLocal = MessagingTask.getNonLoopback(mtask,
				this.paxosManager.getMyID());
		if (nonLocal == null || nonLocal.isEmptyMessaging())
			return mtask;
		MessagingTask local = MessagingTask.getLoopback(mtask,
				this.paxosManager.getMyID());
		if(local == null || local.isEmptyMessaging()) ; // no-op

		boolean isAccReply = allAcceptReplies(mtask), isCommit = allCoalescableDecisions(mtask);
		if (!isAccReply && !isCommit)
			return mtask;

		if(SHORT_CIRCUIT_LOCAL) mtask = nonLocal;
		
		// use batched mtask as "local" NIO is worse for large requests
		if (isAccReply) {
			for (PaxosPacket acceptReply : mtask.msgs)
				this.enqueue(acceptReply.toSingletonArray());
		} else if (isCommit) {
			BatchedCommit batchedCommit = new BatchedCommit(
					(PValuePacket) mtask.msgs[0],
					Util.arrayToIntSet(mtask.recipients));
			// add rest into first, so index starts from 1
			for (int i = 1; i < mtask.msgs.length; i++) {
				if (batchedCommit.ballot
						.compareTo(((PValuePacket) mtask.msgs[i]).ballot) == 0)
					batchedCommit.addCommit((PValuePacket) mtask.msgs[i]);
				else {
					// can only add same ballot decisions
					this.enqueue(batchedCommit.toSingletonArray());
					batchedCommit = new BatchedCommit(
							(PValuePacket) mtask.msgs[i],
							Util.arrayToIntSet(mtask.recipients));
				}
			}
			this.enqueue(batchedCommit.toSingletonArray());
		}
		return SHORT_CIRCUIT_LOCAL ? local : null;  //local could still be null
	}

	private boolean allLocal(MessagingTask mtask) {
		for (int recipient : mtask.recipients)
			if (recipient != this.paxosManager.getMyID())
				return false;
		return true;
	}

	private static boolean allAcceptReplies(MessagingTask mtask) {
		for (PaxosPacket ppkt : mtask.msgs)
			if (!(ppkt instanceof AcceptReplyPacket))
				return false;
		return true;
	}

	private static boolean allCoalescableDecisions(MessagingTask mtask) {
		for (PaxosPacket ppkt : mtask.msgs)
			if (!(ppkt instanceof PValuePacket && ((PValuePacket) ppkt)
					.isCoalescable()))
				return false;
		return true;
	}

	// used by paxos manager to garbage collect decisions
	protected static Map<String, Integer> getMaxLoggedDecisionMap(
			MessagingTask[] mtasks) {
		HashMap<String, Integer> maxLoggedDecisionMap = new HashMap<String, Integer>();
		for (MessagingTask mtask : mtasks) {
			PValuePacket loggedDecision = null;
			if (mtask.isEmptyMessaging()
					&& mtask instanceof LogMessagingTask
					&& (loggedDecision = (PValuePacket) ((LogMessagingTask) mtask).logMsg) != null
					&& loggedDecision.getType()
							.equals(PaxosPacketType.DECISION)) {
				int loggedDecisionSlot = loggedDecision.slot;
				String paxosID = loggedDecision.getPaxosID();
				if (!maxLoggedDecisionMap.containsKey(paxosID))
					maxLoggedDecisionMap.put(paxosID, loggedDecisionSlot);
				else if (maxLoggedDecisionMap.get(paxosID) - loggedDecisionSlot < 0)
					maxLoggedDecisionMap.put(paxosID, loggedDecisionSlot);
			}
		}
		return maxLoggedDecisionMap;
	}

	// entire piece of stupidity needed only for the isEmpty() method
	static class HashMapContainer implements
			Collection<HashMap<String, HashMap<Ballot, ?>>> {
		private final HashMap<String, HashMap<Ballot, BatchedAcceptReply>> acceptReplies = new HashMap<String, HashMap<Ballot, BatchedAcceptReply>>();
		private final HashMap<String, HashMap<Ballot, BatchedCommit>> commits = new HashMap<String, HashMap<Ballot, BatchedCommit>>();

		@Override
		public int size() {
			return acceptReplies.size() + commits.size();
		}

		@Override
		public boolean isEmpty() {
			return size() == 0;
		}

		@Override
		public boolean contains(Object o) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public Iterator<HashMap<String, HashMap<Ballot, ?>>> iterator() {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public Object[] toArray() {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public <T> T[] toArray(T[] a) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public boolean add(HashMap<String, HashMap<Ballot, ?>> e) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public boolean remove(Object o) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public boolean addAll(
				Collection<? extends HashMap<String, HashMap<Ballot, ?>>> c) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public void clear() {
			throw new RuntimeException("Not implemented");
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		Ballot b1 = new Ballot(23, 456);
		Ballot b2 = new Ballot(23, 456);
		assert (b1.equals(b2));
		Set<Ballot> bset = new HashSet<Ballot>();
		bset.add(b1);
		assert (bset.contains(b1));
		assert (bset.contains(b2));
		bset.add(b2);
		assert (bset.size() == 1) : bset.size();
	}

}
