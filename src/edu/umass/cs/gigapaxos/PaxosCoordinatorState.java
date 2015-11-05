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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.AcceptPacket;
import edu.umass.cs.gigapaxos.paxospackets.AcceptReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PrepareReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.ProposalPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.WaitforUtility;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.NullIfEmptyMap;

/**
 * @author V. Arun
 * 
 *         This class manages paxos coordinator state. It has no public methods,
 *         only protected methods, as it is used *only* by PaxosCoordinator.
 * 
 *         An inactive PaxosCoordinatorState instance, i.e., when the
 *         corresponding node is a coordinator, but this paxos instance is not
 *         processing any requests uses about 50B plus an int each for each
 *         member of nodeSlotNumbers.
 * 
 *         Testing: It is unit-testable by running the main method.
 */
public class PaxosCoordinatorState extends PaxosCoordinator {
	private static final String NO_OP = RequestPacket.NO_OP;
	private static final String STOP = "STOP";
	private static final int PREPARE_TIMEOUT = 60000; // ms
	private static final int ACCEPT_TIMEOUT = 60000; // ms
	private static final double ACCEPT_RETRANSMISSION_BACKOFF_FACTOR = 1.5;
	private static final double PREPARE_RETRANSMISSION_BACKOFF_FACTOR = 1.5;

	private static final int RERUN_DELAY_THRESHOLD = 10000; // ms

	// final ballot, takes birth and dies with this PaxosCoordinatorState
	private final int myBallotNum;
	private final int myBallotCoord;
	// using two ints is 24 bytes less than Ballot

	// List of replicas who have accepted my ballot. Non-null only until the
	// coordinator becomes active.
	private WaitforUtility waitforMyBallot = null;

	/*
	 * List of proposals carried over from lower ballots to be re-proposed or
	 * committed in my ballot. Non-null only untul the coordinator becomes
	 * active.
	 */
	private NullIfEmptyMap<Integer, PValuePacket> carryoverProposals = new NullIfEmptyMap<Integer, PValuePacket>();

	/*
	 * List of proposals I am currently trying to push in my ballot as
	 * coordinator (or when I get the election majority to be a coordinator).
	 * Typically, this map is the only non-trivial space consumer when a paxos
	 * instance is actively processing requests. The size of this map depends on
	 * the rate of incoming requests and paxos throughput typically, and depends
	 * on the number of adopted proposals from lower ballots during coordinator
	 * changes. The latter in turn depends on how quickly a majority of
	 * instances commit a decision, as accepts below the majority committed
	 * frontier are garbage collected.
	 */
	private NullIfEmptyMap<Integer, ProposalStateAtCoordinator> myProposals = new NullIfEmptyMap<Integer, ProposalStateAtCoordinator>();

	private int nextProposalSlotNumber = 0; // next free slot number to propose

	//private static enum STATES {PREACTIVE, ACTIVE, RESIGNED};
	/*
	 * After this coordinator becomes active, it remains active until it no
	 * longer exists. That is, we never set active back to false again. Instead,
	 * this coordinator state is expunged and new coordinator state (for a
	 * different ballot) may get created in the future.
	 */
	private boolean active = false;

	/*
	 * This field is useful for garbage collection. It is maintained here so
	 * that we can piggyback on coordinator back-and-forth messages in order to
	 * disseminate the maximum slot cumulatively executed at a majority of
	 * acceptors, which is useful for garbage collecting accepted proposals at
	 * acceptors. It works in two phases as follows.
	 * 
	 * Before a coordinator becomes active, nodeSlotNumbers[i] maintains the
	 * slot number below which acceptor i has garbage collected accepted
	 * proposals. The acceptor maintains this slot in the field
	 * minCommittedFrontierSlot and sends it to the coordinator in its prepare
	 * reply. It is important for a coordinator to know while processing an
	 * acceptor's prepare reply from which slot the carryoverProposals reported
	 * by an acceptor *starts* so that the coordinator does not incorrectly
	 * assume an empty slot in the reported carryoverProposals for a no-op slot
	 * (a safety violation). Upon getting elected by getting non-preempting
	 * prepare replies from a majority of acceptors, the coordinator uses the
	 * maximum starting slot across this majority of replicas as the first slot
	 * that it will try to get accepted in its ballot.
	 * 
	 * After becoming active, nodeSlotNumbers[i] maintains the slot up to which
	 * acceptor i has cumulatively executed requests. Acceptors provide this
	 * information to the coordinator through accept reply messages. Based on
	 * this information in nodeSlotNumbers, the active coordinator in its accept
	 * messages tells acceptors the slot number up to which a majority of
	 * replicas have cumulatively executed requests. The acceptor updates this
	 * information in its minCommittedFrontierSlot field and garbage collects
	 * accepted proposals with lower slot numbers, and uses this field to report
	 * to the next coordinator as in the pre-active phase described above.
	 * 
	 * Notes: (1) We don't bother keeping track of members here because that is
	 * sorted and final in the paxos instance and is always passed as an
	 * argument when needed here. (2) All of this garbage collection is
	 * referring to an acceptor's acceptedProposals; prepares and decisions are
	 * garbage collected simply below the checkpoint.
	 */
	private int[] nodeSlotNumbers = null;

	private static Logger log = PaxosManager.getLogger();

	// Used in myProposals map above and nowhere else
	private class ProposalStateAtCoordinator {
		final PValuePacket pValuePacket;
		final WaitforUtility waitfor;

		ProposalStateAtCoordinator(int[] members, PValuePacket pvalue) {
			this.pValuePacket = new PValuePacket(new Ballot(myBallotNum,
					myBallotCoord), pvalue);// pvalue;
			this.waitfor = new WaitforUtility(members);
		}

		public String toString() {
			return this.pValuePacket + this.waitfor.toString();
		}
	}

	/*
	 * Coordinator state is born into the ballot myBallot. myBallot can never
	 * change.
	 */
	PaxosCoordinatorState(int bnum, int coord, int slot, int[] members,
			PaxosCoordinatorState prev) {
		this.myBallotNum = bnum;
		this.myBallotCoord = coord;
		this.nextProposalSlotNumber = slot;
		this.nodeSlotNumbers = new int[members.length];
		for (int i = 0; i < members.length; i++)
			this.nodeSlotNumbers[i] = -1;
		if (prev != null && !prev.isActive() && !prev.myProposals.isEmpty())
			// wasteful to drop these preactives
			this.copyOverPrevious(prev.myProposals,
					prev.nextProposalSlotNumber, members);
		//this.pcs = this;
	}

	private void copyOverPrevious(
			NullIfEmptyMap<Integer, ProposalStateAtCoordinator> prev,
			int nextSlot, int[] members) {
		String s = "";
		String paxosID = null;
		for (ProposalStateAtCoordinator psac : prev.values()) {
			PValuePacket prevProp = psac.pValuePacket;
			PValuePacket curProp = new PValuePacket(new Ballot(
					this.myBallotNum, this.myBallotCoord), prevProp);
			s = s + prevProp.slot;
			paxosID = prevProp.getPaxosID();
			this.myProposals.put(prevProp.slot, new ProposalStateAtCoordinator(
					members, curProp));
		}
		log.log(Level.FINE, "Coordinator {0}, {1} copying over slots [{2}]",
				new Object[] { this.myBallotCoord, paxosID, s });
		this.nextProposalSlotNumber = nextSlot;
	}

	protected void setNodeSlots(int[] slots) {
		this.nodeSlotNumbers = slots;
	}

	/*
	 * Phase1a Event: A call to try to become coordinator. Action: If I am not
	 * already a coordinator, initiate a waitfor to start receiving prepare
	 * replies.
	 * 
	 * Return: The ballot. The caller will send out the ballot to all nodes.
	 */
	protected synchronized Ballot prepare(int[] members) {
		if (this.active == true)
			return null; // I am already an active coordinator
		if (this.waitforMyBallot == null)
			this.waitforMyBallot = new WaitforUtility(members);
		return new Ballot(this.myBallotNum, this.myBallotCoord);
	}

	/*
	 * Phase2a Event: A call to propose a request with any available slot
	 * number. Action: Assign the next available slot number and add it to the
	 * list of proposals I am pushing for. Actually send it out by invoking
	 * initCommander only if I am active as a coordinator. Otherwise, I am still
	 * in the process of receiving pvalues accepted by other nodes in lower
	 * ballots that might supersede my proposals.
	 * 
	 * Return: An AcceptPacket if any that will be sent out by the caller to
	 * other nodes so as to actually propose this request.
	 */
	private static final boolean EXECUTE_UPON_ACCEPT = Config.getGlobalBoolean(PC.EXECUTE_UPON_ACCEPT);
	protected synchronized AcceptPacket propose(int[] members,
			RequestPacket request) {
		if (this.myProposals.containsKey(this.nextProposalSlotNumber - 1) &&
		// no point enqueuing anything after stop
				this.myProposals.get(this.nextProposalSlotNumber - 1).pValuePacket
						.isStopRequest())
			return null;
		AcceptPacket acceptPacket = null;
		Ballot myBallot = new Ballot(this.myBallotNum, this.myBallotCoord);
		PValuePacket pvalue = new PValuePacket(myBallot, new ProposalPacket(
				this.nextProposalSlotNumber++, request));
		// nextSlot should always be free
		assert (!this.myProposals.containsKey(pvalue.slot));
		if(!EXECUTE_UPON_ACCEPT) // only for testing
			this.myProposals.put(pvalue.slot, new ProposalStateAtCoordinator(
				members, pvalue));
		log.log(Level.FINE, "Node {0} inserted proposal {1}",
				new Object[] { myBallot.coordinatorID,
						pvalue.getSummary(log.isLoggable(Level.FINE)) });
		if (this.isActive()) {
			acceptPacket = this.initCommander(members, pvalue);
		} else {
			log.log(Level.FINE, "Coordinator at node {0} is not active",
					new Object[] { myBallot.coordinatorID, });
			/*
			 * Got to wait till view change is complete as proposals for a slot
			 * can change in the process. Do nothing for now.
			 */
		}
		return acceptPacket;
	}

	/*********************** Start of Phase1b methods ************************/

	/*
	 * Phase1b If I receive a prepare reply with a higher ballot, I am
	 * guaranteed to lose the election, so concede defeat already.
	 */
	protected synchronized boolean isPreemptable(PrepareReplyPacket prepareReply) {
		boolean preempt = false;
		if (prepareReply.ballot.compareTo(new Ballot(this.myBallotNum,
				this.myBallotCoord)) > 0) {
			preempt = true;
		}
		return preempt;
	}

	/*
	 * Phase1b Event: Received a prepare reply for my ballot or lower. Return:
	 * true if prepare reply should be ignored. A prepare message is sent by a
	 * coordinator to initiate a ballot. The coordinator waits for prepare
	 * replies containing its proposed ballot. Lower ballots can be ignored.
	 * Higher ballots mean that I should resign.
	 */
	private synchronized boolean canIgnorePrepareReply(
			PrepareReplyPacket prepareReply, int[] members) {
		// I am not even trying to be a coordinator.
		if (this.waitforMyBallot == null)
			return true;

		/*
		 * This should not usually happen. Why did the replica bother to send me
		 * a lower ballot in the first place? It can happen is some replica is
		 * recovering and replaying logged messages with networking turned on.
		 */
		else if (prepareReply.ballot.compareTo(new Ballot(this.myBallotNum,
				this.myBallotCoord)) < 0) {
			log.log(Level.FINE,
					"Node{0} received outdated prepare reply, "
							+ "which should normally happen only during recovery: {1}): ",
					new Object[] { this.myBallotCoord,
							prepareReply.getSummary(log.isLoggable(Level.FINE)) });
			return true;
		}

		// The replica already answered my ballot request.
		else if (!Util.contains(prepareReply.acceptor, members)
				|| this.waitforMyBallot.alreadyHeardFrom(prepareReply.acceptor))
			return true;

		return false;
	}

	/*
	 * Phase1b Event: Received prepare reply for my ballot. Action: Update
	 * waitfor to check for majority in ballot.
	 * 
	 * Return: true if my ballot has been accepted by a majority.
	 * 
	 * Note: If accepted by a majority, we next need to do some nontrivial
	 * processing of pvalues received in these prepare replies. The caller will
	 * next invoke combinePValuesOntoProposals.
	 */
	protected synchronized boolean isPrepareAcceptedByMajority(
			PrepareReplyPacket prepareReply, int[] members) {
		if (this.canIgnorePrepareReply(prepareReply, members)) {
			log.log(Level.FINE, "Node {0} ignoring prepare reply",
					new Object[] { this.myBallotCoord });
			return false;
		}
		// isPreemptable and canIgnorePrepareReply should have been called
		// already, hence the assert
		assert (prepareReply.ballot.compareTo(new Ballot(this.myBallotNum,
				this.myBallotCoord)) == 0) : "prepareReply.ballot = "
				+ prepareReply.ballot + ", myBallot = " + this.myBallotNum
				+ ":" + this.myBallotCoord;

		// useful in combinePValuesOntoProposals
		recordSlotNumber(members, prepareReply);

		boolean acceptedByMajority = false;
		for (PValuePacket pvalue : prepareReply.accepted.values()) {
			int curSlot = pvalue.slot;
			/*
			 * The latter part of the disjunction below is because some
			 * replica(s) may have accepted a different proposal for curSlot in
			 * a higher ballot, so we should choose the proposal accepted for a
			 * slot in the highest ballot.
			 */
			PValuePacket existing = this.carryoverProposals.get(curSlot);
			if (existing == null
					|| pvalue.ballot.compareTo(existing.ballot) > 0) {
				assert (pvalue.ballot.compareTo(this.getBallot()) <= 0) : pvalue
						+ " > " + this.getBallotStr();
				if (existing != null
						&& pvalue.ballot.compareTo(existing.ballot) > 0)
					log.log(Level.INFO, "{0} dropping overwritten request {1}",
							new Object[] { this, existing.getSummary() });
				this.carryoverProposals.put(pvalue.slot, pvalue);
			} else if (pvalue.ballot.compareTo(existing.ballot) == 0) {
				assert (pvalue.requestValue.equals(existing.requestValue));
			}
		}
		waitforMyBallot.updateHeardFrom(prepareReply.acceptor);
		log.log(Level.FINEST, "Waitfor = {0}", new Object[] {
				waitforMyBallot });
		if (this.waitforMyBallot.heardFromMajority()) {
			acceptedByMajority = true;
			log.log(Level.FINE,
					"{0} coordinator {1} acquired PREPARE majority and about to conduct view change; "
							+ "accepted_pvalues_min_slots = {2}",
					new Object[] { prepareReply.getPaxosID(),
							Ballot.getBallotString(myBallotNum, myBallotCoord),
							getString(this.nodeSlotNumbers) });
		}
		return acceptedByMajority;
	}

	/*
	 * Phase1b Called after the above method by caller. We could also call it
	 * from the above method instead, but having the caller invoke these methods
	 * makes them more readable.
	 * 
	 * This method combines the received pvalues with existing proposals so that
	 * the former supersedes the latter. This completes the
	 * "proposals*pmax(pvals)" step. The pmax() was already being done as
	 * pvalues were being received.
	 */
	protected synchronized void combinePValuesOntoProposals(int[] members) {
		if (this.carryoverProposals.isEmpty())
			return; // no need to process stop requests either

		int maxCarryoverSlot = getMaxPValueSlot(this.carryoverProposals);
		int maxMinCarryoverSlot = this.getMaxMinCarryoverSlot();

		/*
		 * Combine carryoverProposals with myProposals prioritizing the former
		 * and selecting no-ops for slots for which neither contain a value.
		 */
		NullIfEmptyMap<Integer, ProposalStateAtCoordinator> preActives = this.myProposals;
		this.myProposals = new NullIfEmptyMap<Integer, ProposalStateAtCoordinator>();
		for (int curSlot = maxMinCarryoverSlot; curSlot - maxCarryoverSlot <= 0; curSlot++) { // wrapround-arithmetic
			// received pvalues dominate pre-active proposals
			if (this.carryoverProposals.containsKey(curSlot)) {
				this.myProposals.put(curSlot, new ProposalStateAtCoordinator(
						members, this.carryoverProposals.get(curSlot)));
			} else if (!preActives.containsKey(curSlot)) { // no-op if neither
															// received nor
															// pre-active
				this.myProposals.put(curSlot, new ProposalStateAtCoordinator(
						members, makeNoopPValue(curSlot, null)));
			} else if (preActives.containsKey(curSlot)) { // stick with
															// pre-active
				this.myProposals.put(curSlot, preActives.get(curSlot));
				preActives.remove(curSlot);
			}
		}
		/*
		 * The next free slot number to use for proposals should be the maximum
		 * of the maximum slot in received pvalues, and the
		 * nextProposalSlotNumber that reflects the highest locally proposed
		 * slot number.
		 */
		this.nextProposalSlotNumber = maxCarryoverSlot + 1;
		assert (noGaps(maxMinCarryoverSlot, this.nextProposalSlotNumber,
				this.myProposals)); // gaps means state machine will be stuck

		this.reproposePreemptedProposals(preActives, members);

		// need to ensure that a regular request never follows a stop
		processStop(members);
	}

	/*
	 * Phase1b Utility method invoked in combinePValuesOntoProposals to
	 * repropose slot numbers below maxMinCarryoverSlot with slot numbers >=
	 * nextProposalSlotNumber. We could also safely just drop these requests,
	 * but why be so heartless.
	 */
	private synchronized void reproposePreemptedProposals(
			NullIfEmptyMap<Integer, ProposalStateAtCoordinator> preempted,
			int[] members) {
		for (ProposalStateAtCoordinator psac : preempted.values()) {
			AcceptPacket accept = this.propose(members,
					(RequestPacket) psac.pValuePacket);
			assert (accept == null); // because we are not active yet
		}
	}

	/*
	 * Phase1b Utility method to handle stop requests correctly after combining
	 * pvalues on to proposals. The goal is to make sure that there is no
	 * request after a stop request. If there is a request after a stop in a
	 * higher ballot, the stop has to be turned into a noop. If there is a
	 * request after a stop in a lower ballot, the request should be turned into
	 * a stop.
	 */
	private synchronized void processStop(int[] members) {
		ArrayList<ProposalStateAtCoordinator> modified = new ArrayList<ProposalStateAtCoordinator>();
		boolean stopExists = false;
		for (ProposalStateAtCoordinator psac1 : this.myProposals.values()) {
			if (!psac1.pValuePacket.isStopRequest())
				continue;
			else
				stopExists = true;
			for (ProposalStateAtCoordinator psac2 : this.myProposals.values()) {
				if (psac1.pValuePacket.isStopRequest() && // 1 is stop
						// other is not stop
						!psac2.pValuePacket.isStopRequest() &&
						// other is not noop
						!psac2.pValuePacket.requestValue.equals(NO_OP) &&
						// other has a higher slot
						psac1.pValuePacket.slot - psac2.pValuePacket.slot < 0) {
					// stop ballot > other ballot
					if (psac1.pValuePacket.ballot
							.compareTo(psac2.pValuePacket.ballot) > 0) {
						// convert request (psac2) to stop (psac1)
						int reqSlot = psac2.pValuePacket.slot;
						log.log(Level.FINE, "Converting {0} to stop", new Object[] {
								psac2.pValuePacket.slot,
								});
						ProposalStateAtCoordinator psac2ToStop = new ProposalStateAtCoordinator(
								members, new PValuePacket(new Ballot(
										this.myBallotNum, this.myBallotCoord),
										new ProposalPacket(reqSlot,
												psac1.pValuePacket)));
						modified.add(psac2ToStop);
					} else if
					// stop ballot < other ballot
					(psac1.pValuePacket.ballot
							.compareTo(psac2.pValuePacket.ballot) < 0) {
						// convert stop (psac1) to noop (psac1)
						assert (psac1.pValuePacket.isStopRequest());
						log.log(Level.FINE, "Converting {0} to noop", new Object[] {
								psac1.pValuePacket.slot,
								 });
						PValuePacket noopPValue = this
								.makeNoopPValue(psac1.pValuePacket);
						psac1 = new ProposalStateAtCoordinator(members,
								noopPValue);
						// could "continue" here as psac1 is not stop anymore
						modified.add(psac1);
					} else {
						assert (false) : Util.suicide("YIKES! Coordinator "
								+ psac1.pValuePacket.ballot.coordinatorID
								+ " proposed a regular request "
								+ psac2.pValuePacket.slot + " after a stop "
								+ psac1.pValuePacket.slot + "\n"
								+ psac1.pValuePacket + "\n"
								+ psac2.pValuePacket
								+ "\nThis should be reported as a bug.");
					}
				}
			}
		}
		for (ProposalStateAtCoordinator psac : modified)
			this.myProposals.put(psac.pValuePacket.slot, psac);
		if (stopExists
				&& !this.myProposals.get(this.nextProposalSlotNumber - 1).pValuePacket
						.isStopRequest()) {
			this.propose(members, new RequestPacket(0, STOP, true));
		}
	}

	/*
	 * Phase1b The second last step of Phase1b. The caller invokes this method
	 * after the above method to spawn commanders for all updated local
	 * proposals.
	 * 
	 * Return: The list of AcceptPackets that the caller will actually send out
	 * in order to propose these proposals.
	 * 
	 * The view change is complete when the caller invokes setCoordinatorActive
	 * after this method.
	 */
	protected synchronized ArrayList<AcceptPacket> spawnCommandersForProposals() {
		if (this.active)
			return null; // ensures called only once

		ArrayList<AcceptPacket> acceptPacketList = null;
		for (ProposalStateAtCoordinator pstate : this.myProposals.values()) {
			if (acceptPacketList == null)
				acceptPacketList = new ArrayList<AcceptPacket>();
			acceptPacketList.add(this.initCommander(
					pstate.waitfor.getMembers(), pstate.pValuePacket));
		}

		// ensures called only once as active is never set to false
		this.active = true;
		return acceptPacketList;
	}

	/*
	 * Phase1b Phase1b is complete. I am active. I will remain active until I am
	 * destroyed and my ballot is forgotten forever.
	 */
	protected synchronized void setCoordinatorActive() {
		this.active = true;
		/*
		 * The two structures below have no more use. They hardly take up any
		 * space, especially coz the latter is a NullIfEmptyMap, but why bother
		 * to even keep that. Plus it serves as an implicit assert(false) if any
		 * code tries to access these structures here onwards.
		 */
		this.waitforMyBallot = null;
		this.carryoverProposals = null;
	}

	/*********************** End of Phase1b methods ************************/

	/*********************** Start of Phase2b methods ************************/
	/*
	 * Phase2b Event: Received an accept reply (phase2b) for a proposal in the
	 * current ballot. Action: Update waitfor to check for a majority. Return:
	 * the proposal if a majority of accept replies have been received.
	 */
	protected synchronized PValuePacket handleAcceptReplyMyBallot(
			int[] members, AcceptReplyPacket acceptReply) {
		assert ((acceptReply.ballot.compareTo(new Ballot(this.myBallotNum,
				this.myBallotCoord)) == 0));
		assert (!this.myProposals.containsKey(acceptReply.slotNumber) || this.myProposals
				.get(acceptReply.slotNumber).waitfor
				.contains(acceptReply.acceptor)) : "ACCEPT = "
				+ this.myProposals.get(acceptReply.slotNumber)
				+ "; ACCEPT_REPLY = " + acceptReply;
		// current ballot, makes sense to process now
		recordSlotNumber(members, acceptReply); // useful for garbage collection

		boolean acceptedByMajority = false;
		ProposalStateAtCoordinator pstate = this.myProposals
				.get(acceptReply.slotNumber);
		WaitforUtility waitfor = null;
		PValuePacket decision = null;
		/*
		 * If I proposed it and it is not yet preempted or committed, then I
		 * must have some waitfor state for it.
		 */
		if (pstate != null && ((waitfor = pstate.waitfor) != null)) {
			waitfor.updateHeardFrom(acceptReply.acceptor);
			log.log(Level.FINEST,
					"Node {0} updated waitfor to: {1} for {2}",
					new Object[] {
							this.myBallotCoord,
							waitfor,
							pstate.pValuePacket.getSummary(log
									.isLoggable(Level.FINEST)) });
			if (waitfor.heardFromMajority()) {
				// phase2b success
				acceptedByMajority = true;
				decision = (pstate.pValuePacket
						.makeDecision(getMajorityCommittedSlot()));
				log.log(Level.FINE, "{0} decided {1}", new Object[] { this,
						decision.getSummary(log.isLoggable(Level.FINE)) });
				assert (!decision.isRecovery());
				this.myProposals.remove(decision.slot);
			} else
				pstate.pValuePacket.addDebugInfo("r");
		}
		return (acceptedByMajority ? decision : null);
	}

	/*
	 * Phase2b Event: Received an accept reply (phase2b) for a higher ballot.
	 * Action: Relinquish this proposal.
	 * 
	 * Return: true if it's time to resign, i.e., no more outstanding proposals
	 * are left, else false.
	 * 
	 * Technically, the coordinator could also simply resign when any higher
	 * ballot than myBallot for any proposal is observed, as the rival
	 * coordinator(s) pushing the higher ballot or beyond will do the job. But,
	 * in general, receiving an accept reply with higherBallot for (myBallot,
	 * slot_x, proposal_x), where higherBallot > myBallot, does not imply that
	 * this coordinator can not successfully complete (myBallot, slot_y,
	 * proposal_y) for some other slot y (including y>x). Indeed, it might get a
	 * majority of accept replies anyway even for y=x. But we choose to
	 * relinquish this specific slot x, and resign overall as coordinator if
	 * there are no outstanding proposals left, when a higher ballot in an
	 * accept reply for x is received.
	 */
	protected synchronized PValuePacket handleAcceptReplyHigherBallot(
			AcceptReplyPacket acceptReply) {
		assert (!this.myProposals.containsKey(acceptReply.slotNumber) || this.myProposals
				.get(acceptReply.slotNumber).waitfor
				.contains(acceptReply.acceptor));
		// Stop coordinating this specific proposal.
		ProposalStateAtCoordinator psac = this.myProposals
				.remove(acceptReply.slotNumber);
		PValuePacket preempted = (psac != null ? psac.pValuePacket.preempt()
				: null);
		assert (preempted == null || preempted.ballot
				.compareTo(acceptReply.ballot) < 0) : preempted + " >= "
				+ acceptReply;
		return preempted;
	}

	protected synchronized boolean preemptedFully() {
		// If no outstanding proposals and received higher ballot, resign as
		// coordinator.
		if (this.myProposals.isEmpty())
			return true;
		return false;
	}

	/*********************** End of Phase2b methods ************************/
	protected Ballot getBallot() {
		return new Ballot(this.myBallotNum, this.myBallotCoord);
	}

	protected String getBallotStr() {
		return Ballot.getBallotString(this.myBallotNum, this.myBallotCoord);
	}

	protected int getBallotCoord() {
		return this.myBallotCoord;
	}

	protected int getBallotNum() {
		return this.myBallotNum;
	}

	protected synchronized int getNextProposalSlot() {
		return this.nextProposalSlotNumber;
	}

	protected synchronized int[] getNodeSlots() {
		return this.nodeSlotNumbers;
	}

	protected synchronized boolean isActive() {
		return this.active;
	}

	// checks and increments retransmission count
	protected synchronized boolean testAndSetWaitingTooLong() {
		if (!this.isActive() // periodic retransmission
				&& this.waitforMyBallot.waitTime() > PREPARE_TIMEOUT
						* Math.pow(PREPARE_RETRANSMISSION_BACKOFF_FACTOR,
								this.waitforMyBallot.getRetransmissionCount())) {
			this.waitforMyBallot.incrRetransmissonCount(true);
			if(Util.oneIn(10)) log.info(DelayProfiler.getStats());
			return true;
		}
		return false;
	}

	// checks and returns just whether it's been waiting for too long
	protected synchronized boolean testAndSetWaitingTooLong(int slot) {
		ProposalStateAtCoordinator psac = this.myProposals.get(slot);
		if (this.isActive()
				&& psac != null
				// exponential backoff
				&& psac.waitfor.totalWaitTime() > ACCEPT_TIMEOUT
						* Math.pow(ACCEPT_RETRANSMISSION_BACKOFF_FACTOR,
								psac.waitfor.getRetransmissionCount())) {
			psac.waitfor.incrRetransmissonCount(true);
			return true;
		}
		return false;
	}

	protected synchronized boolean isCommandering(int slot) {
		return (this.isActive() && this.myProposals.containsKey(slot)) ? true
				: false;
	}

	protected synchronized AcceptPacket reInitCommander(int slot) {
		return // this.testAndSetWaitingTooLong(slot) ?
		this.initCommander(this.myProposals.get(slot));
		// : null;
	}

	protected boolean ranRecently() {
		WaitforUtility waitfor = this.waitforMyBallot;
		return waitfor != null
				&& waitfor.totalWaitTime() < RERUN_DELAY_THRESHOLD;
	}

	protected synchronized boolean caughtUp() {
		return //this.isActive() && 
				this.myProposals.isEmpty();
	}

	/*
	 * This method returns proposals received by this coordinator before it
	 * actually became active. If preempted, it is nice to pass them over to the
	 * next coordinator. Not necessary for safety.
	 */
	protected synchronized ArrayList<ProposalPacket> getPreActiveProposals() {
		Collection<ProposalStateAtCoordinator> preActiveSet = this.myProposals
				.values();
		ArrayList<ProposalPacket> preActiveProposals = new ArrayList<ProposalPacket>();
		for (ProposalStateAtCoordinator psac : preActiveSet) {
			preActiveProposals.add(psac.pValuePacket);
		}
		return preActiveProposals;
	}

	/****************************** Start of non-public/protected methods ******************************/

	/*
	 * Record the acceptor GC slot number specified in the prepare reply.
	 * Acceptors remove accepted proposals at or below the GC slot number, i.e.,
	 * the maximum slot number that has been cumulatively executed by a majority
	 * of replicas.
	 */
	private synchronized void recordSlotNumber(int[] members,
			PrepareReplyPacket preply) {
		assert (this.nodeSlotNumbers != null);
		boolean updated = false;
		for (int i = 0; i < members.length; i++) {
			if (members[i] == preply.acceptor) {
				if (this.nodeSlotNumbers[i] - preply.getMinSlot() < 0) {
					this.nodeSlotNumbers[i] = preply.getMinSlot();
					updated = true;
				}
			}
		}
		if (updated)
			log.log(Level.FINEST, "Node {0} updated nodeSlotNumbers on a prepare reply {1}",
					new Object[] {this.myBallotCoord,
							getString(nodeSlotNumbers) });
	}

	/*
	 * Record the cumulative committed slot number in the accept reply. After
	 * becoming active, the coordinator will disseminate the slot number
	 * cumulatively committed by a majority of acceptors.
	 */
	private synchronized void recordSlotNumber(int[] members,
			AcceptReplyPacket acceptReply) {
		assert (this.nodeSlotNumbers != null);
		boolean updated = false;
		for (int i = 0; i < members.length; i++) {
			if (members[i] == acceptReply.acceptor) {
				if (this.nodeSlotNumbers[i] < acceptReply.maxCheckpointedSlot) {
					this.nodeSlotNumbers[i] = acceptReply.maxCheckpointedSlot;
					updated = true;
				}
			}
		}
		if (updated)
			log.log(Level.FINE, "Node {0} updaed nodeSlotNumbers on an accept reply {1}",
					new Object[] { this.myBallotCoord,
							getString(nodeSlotNumbers) });
	}

	private synchronized AcceptPacket initCommander(int[] members,
			PValuePacket pvalue) {
		return this.initCommander(new ProposalStateAtCoordinator(members,
				pvalue));
	}
	
	private Object getString(final int[] array) {
		return new Object() {
			public String toString() {
				return Arrays.toString(array);
			}
		};
	}

	private synchronized AcceptPacket initCommander(
			ProposalStateAtCoordinator pstate) {
		AcceptPacket acceptPacket = new AcceptPacket(this.myBallotCoord,
				pstate.pValuePacket, getMajorityCommittedSlot());
		//pstate.waitfor.incrRetransmissonCount();
		log.log(Level.FINE,
				"Node {0} intCommandering {1}; nodeSlotNumbers = {2}",
				new Object[] { this.myBallotCoord, acceptPacket.getSummary(log.isLoggable(Level.FINE)),
						getString(nodeSlotNumbers) });
		return acceptPacket;
	}

	/*********************
	 * Start of stand-alone'ish utility methods ************************* These
	 * private methods are not synchronized even though some of them take
	 * synchronization requiring structures as arguments because the callers
	 * above are *all* synchronized.
	 */
	protected int getMajorityCommittedSlot() {
		return this.getMedianMinus(this.nodeSlotNumbers);
	}

	/*
	 * Gets the median (just before median) value with odd (even) number of
	 * members
	 */
	private int getMedianMinus(int[] array) {
		int[] copy = new int[this.nodeSlotNumbers.length];
		System.arraycopy(this.nodeSlotNumbers, 0, copy, 0,
				this.nodeSlotNumbers.length);
		Arrays.sort(copy);
		int medianMinus = copy.length % 2 == 0 ? copy.length / 2 - 1
				: copy.length / 2; // highest index <= median index
		return copy[medianMinus];
	}

	private boolean noGaps(int x, int y,
			NullIfEmptyMap<Integer, ProposalStateAtCoordinator> map) {
		for (int i = x; i - y < 0; i++) { // wraparound-arithmetic
			if (map.get(i) == null)
				return false;
		}
		return true;
	}

	private PValuePacket makeNoopPValue(int curSlot, PValuePacket pvalue) {
		ProposalPacket proposalPacket = null;
		if (pvalue != null)
			proposalPacket = new ProposalPacket(curSlot, pvalue.makeNoop());
		else
			// actual noop (as opposed to RequestPacket converted noop)
			proposalPacket = new ProposalPacket(curSlot, new RequestPacket(
					0, NO_OP, false).setEntryReplica(this.myBallotCoord));
		PValuePacket noop = new PValuePacket(new Ballot(this.myBallotNum,
				this.myBallotCoord), proposalPacket);
		return noop.makeDecision(this.getMajorityCommittedSlot());
	}

	private PValuePacket makeNoopPValue(PValuePacket pvalue) {
		return this.makeNoopPValue(pvalue.slot, pvalue);
	}

	private int getMaxPValueSlot(NullIfEmptyMap<Integer, PValuePacket> pvalues) {
		Integer maxSlot = null; // this is maximum slot for which some adopted
		// (=in-progress) request has been found
		for (int cur : pvalues.keySet()) {
			if (maxSlot == null)
				maxSlot = cur;
			if (cur - maxSlot > 0) // wraparound-arithmetic
				maxSlot = cur;
		}
		assert(maxSlot != null);
		return maxSlot;
	}

	/*
	 * Before a coordinator becomes active, max(nodeSlotNumbers) is the maximum
	 * of the minimum slot numbers including and above which replicas sent
	 * carryover proposals.
	 */
	private int getMaxMinCarryoverSlot() {
		Integer maxSlot = null;// Integer.MIN_VALUE;
		for (int i = 0; i < this.nodeSlotNumbers.length; i++) {
			if (maxSlot == null)
				maxSlot = this.nodeSlotNumbers[i];
			if (this.nodeSlotNumbers[i] - maxSlot > 0) // wraparound-arithmetic
				maxSlot = this.nodeSlotNumbers[i];
		}
		assert (maxSlot != null);
		return maxSlot;
	}

	/********************* End of stand-alone utility methods *************************/

	/********************** Start of testing methods ****************************/
	// no synchronized methods should be used here
	public String toString() {
		return "["
				+ this.myBallotNum
				+ ":"
				+ this.myBallotCoord
				+ ", "
				+ this.nextProposalSlotNumber
				+ ", "
				+ (this.active ? "active" : "inactive")
				+ (this.carryoverProposals != null
						&& !this.carryoverProposals.isEmpty() ? ", |adopted|="
						+ this.carryoverProposals.size() : "")
				+ (!this.myProposals.isEmpty() ? ", |myProposals|="
						+ this.myProposals.size() : "")
				+ (!this.active
						&& this.waitforMyBallot.getRetransmissionCount() >= 5 ? ", retries="
						+ this.waitforMyBallot.getRetransmissionCount()
						: "") + "]";
	}

	private String printState() {
		String s = "[\n  carryoverProposals = ";
		if (this.carryoverProposals == null
				|| this.carryoverProposals.isEmpty())
			s += "[]\n";
		else {
			s += "\n";
			for (PValuePacket pvalue : this.carryoverProposals.values()) {
				s += "  " + pvalue + "\n";
			}
		}
		s += printMyProposals();
		s += "  nodeSlotNumbers=[ ";
		for (int i = 0; i < this.nodeSlotNumbers.length; i++)
			s += this.nodeSlotNumbers[i] + " ";
		s += " ]\n]";
		return s;
	}

	private String printMyProposals() {
		String s = "  myProposals = \n";
		for (ProposalStateAtCoordinator psac : this.myProposals.values()) {
			s += psac.pValuePacket + "\n";
		}

		return s;
	}

	protected void testingInitCoord(int load) {
		// this.testingInitInstance(load);
		this.myProposals = new NullIfEmptyMap<Integer, ProposalStateAtCoordinator>();
		int[] group = { 21, 32, 32, 91, 14 };
		for (int i = 0; i < load; i++) {
			this.myProposals.put(25 + i, new ProposalStateAtCoordinator(group,
					new PValuePacket(new Ballot(this.myBallotNum,
							this.myBallotCoord), new ProposalPacket(45 + i,
							new RequestPacket("hello39" + i, false)))));
		}
	}

	private PValuePacket createCarryover(RequestPacket req, int slot,
			int ballot, int coord) {
		ProposalPacket prop = new ProposalPacket(slot, req);
		PValuePacket pvalue = new PValuePacket(new Ballot(ballot, coord), prop);
		return pvalue;
	}

	protected synchronized boolean isPresent(int slot) {
		return this.myProposals.containsKey(slot) || !this.isActive();
	}

	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		int myID = 21;
		int ballotnum = 2;
		int numMembers = 43;
		int[] members = new int[numMembers];
		members[0] = myID;
		for (int i = 1; i < members.length; i++) {
			members[i] = members[i - 1] + 1 + (int) (Math.random() * 10);
		}
		for (int i = 0; i < members.length - 1; i++)
			assert (members[i] < members[i + 1]);

		PaxosCoordinatorState pcs = new PaxosCoordinatorState(ballotnum, myID,
				0, members, null);
		System.out.println("Created PaxosCoordinatorState");
		int numReqs = 100;
		RequestPacket[] reqs = new RequestPacket[numReqs];

		for (int i = 0; i < numReqs; i++) {
			reqs[i] = new RequestPacket("req" + i, false);
		}
		RequestPacket stop = new RequestPacket("STOP_REQUEST", true);

		// Test preactive propose() returns null, and pcs is not active and not
		// empty
		assert (pcs.propose(members, reqs[0]) == null && !pcs.isActive() && !pcs.myProposals
				.isEmpty());
		assert (pcs.propose(members, stop) == null && !pcs.isActive() && !pcs.myProposals
				.isEmpty());
		assert (pcs.propose(members, reqs[9]) == null && !pcs.isActive() && !pcs.myProposals
				.isEmpty());

		// Test preactive proposals are inserted correctly
		boolean inserted = false;
		ArrayList<ProposalPacket> preactives = pcs.getPreActiveProposals();
		for (ProposalPacket prop : preactives)
			if (prop.requestID == reqs[0].requestID)
				inserted = true;
		assert (inserted);
		for (ProposalPacket prop : preactives)
			if (prop.requestID == stop.requestID)
				inserted = true;
		assert (inserted);
		for (ProposalPacket prop : preactives)
			if (prop.requestID == reqs[9].requestID)
				inserted = true;
		assert (inserted);

		// Test prepare replies are ignored before prepare has been sent
		PrepareReplyPacket preply = new PrepareReplyPacket(10, new Ballot(29,
				42), null, -1);
		assert (pcs.waitforMyBallot == null); // prepare not sent yet
		assert (pcs.canIgnorePrepareReply(preply, members));

		// Test prepare ballot is set correctly
		assert (pcs.waitforMyBallot == null);
		Ballot ballot = pcs.prepare(members);
		assert (pcs.waitforMyBallot != null);
		assert (ballot.ballotNumber == ballotnum && ballot.coordinatorID == myID);

		// Test lower ballot prepare replies are ignored
		preply = new PrepareReplyPacket(10, new Ballot(ballot.ballotNumber - 1,
				ballot.coordinatorID), null, -1);
		assert (pcs.canIgnorePrepareReply(preply, members));
		preply = new PrepareReplyPacket(10, new Ballot(ballot.ballotNumber,
				ballot.coordinatorID - 1), null, -1);
		assert (pcs.canIgnorePrepareReply(preply, members));
		// Do not test !canIgnorePrepareReply yet

		// Test not active until enough correct prepare replies are received.
		assert (!pcs.isActive());

		Ballot preplyBallot = new Ballot(ballot.ballotNumber,
				ballot.coordinatorID);
		preply = new PrepareReplyPacket(10, preplyBallot, null, -1);
		assert (!Util.contains(10, members));
		assert (pcs.canIgnorePrepareReply(preply, members)); // coz 10 is not in
																// members

		int numPValues = 100;
		PValuePacket[] pvalues = new PValuePacket[numPValues];

		System.out.println(pcs.printState());
		int maxMinSlot = 7;

		// prepare reply by members[2]
		pvalues[0] = pcs.createCarryover(reqs[0], 2, ballot.ballotNumber - 1,
				ballot.coordinatorID - 1);
		HashMap<Integer, PValuePacket> accepted = new HashMap<Integer, PValuePacket>();
		accepted.put(pvalues[0].slot, pvalues[0]);
		System.out.println("Combining " + accepted);
		preply = new PrepareReplyPacket(members[2], preplyBallot, accepted, -1);
		assert (!pcs.canIgnorePrepareReply(preply, members)); // finally a
																// legitimate
																// reply
		assert (!pcs.isPrepareAcceptedByMajority(preply, members) && pcs.waitforMyBallot
				.alreadyHeardFrom(members[2]));
		assert (pcs.canIgnorePrepareReply(preply, members)); // no duplicates
		System.out.println(pcs.printState());

		// prepare reply by members[0]
		pvalues[1] = pcs.createCarryover(reqs[1], 2, ballot.ballotNumber - 1,
				ballot.coordinatorID);
		pvalues[2] = pcs.createCarryover(reqs[2], 6, ballot.ballotNumber - 1,
				ballot.coordinatorID);
		accepted.put(pvalues[1].slot, pvalues[1]);
		accepted.put(pvalues[2].slot, pvalues[2]);
		assert (pvalues[0].ballot.compareTo(pvalues[1].ballot) < 0);
		System.out.println("Combining " + accepted);
		preply = new PrepareReplyPacket(members[0], preplyBallot, accepted, -1);
		assert (!pcs.isPrepareAcceptedByMajority(preply, members) && pcs
				.canIgnorePrepareReply(preply, members)); // legitimate
															// self-reply
															// followed by
															// duplicate

		// prepare by members[4] including stop
		reqs[3] = new RequestPacket(reqs[3].requestValue,
				false);
		pvalues[3] = pcs.createCarryover(reqs[3], 7, ballot.ballotNumber - 1,
				ballot.coordinatorID);
		pvalues[4] = pcs.createCarryover(reqs[4], 8, ballot.ballotNumber - 1,
				ballot.coordinatorID + 1);
		pvalues[5] = pcs.createCarryover(reqs[5], 9, ballot.ballotNumber - 1,
				ballot.coordinatorID - 1);
		accepted.clear();
		accepted.put(pvalues[3].slot, pvalues[3]);
		accepted.put(pvalues[4].slot, pvalues[4]);
		accepted.put(pvalues[5].slot, pvalues[5]);
		preply = new PrepareReplyPacket(members[4], preplyBallot, accepted, -1);
		assert (!pcs.isPrepareAcceptedByMajority(preply, members) && pcs
				.canIgnorePrepareReply(preply, members));

		for (int i = 0; i < members.length; i += 2) { // members 0, 2, 4, ...
			preply = new PrepareReplyPacket(members[i], preplyBallot, null, -1);
			assert (!pcs.isPrepareAcceptedByMajority(preply, members) || (i == members.length - 1 && (members.length % 2 == 1)));
		}
		if (members.length % 2 == 0) { // one more if even group size
			preply = new PrepareReplyPacket(members[members.length - 1],
					preplyBallot, null, -1);
			assert (pcs.isPrepareAcceptedByMajority(preply, members));
		}
		assert (pcs.waitforMyBallot.heardFromMajority());
		assert (!pcs.myProposals.isEmpty());
		assert (!pcs.carryoverProposals.isEmpty());
		System.out.println(pcs.printState());

		pcs.combinePValuesOntoProposals(members);
		pcs.setCoordinatorActive();
		System.out.println(pcs.printState());

		boolean stopped = false;
		Set<Integer> slots = pcs.myProposals.keySet();
		SortedSet<Integer> sortedSlots = new TreeSet<Integer>(slots);

		for (int slot : sortedSlots) { // slots in sorted order
			assert (slot >= maxMinSlot);
			if (pcs.myProposals.get(slot).pValuePacket.isStopRequest())
				stopped = true;
			// stops are only followed by stops
			if (stopped)
				assert (pcs.myProposals.get(slot).pValuePacket.isStopRequest()) : "slot "
						+ slot;
		}

		pcs.spawnCommandersForProposals();
		pcs.setCoordinatorActive();
		assert (pcs.carryoverProposals == null);
		assert (pcs.waitforMyBallot == null);

		System.out.println("Testing accept replies");
		AcceptReplyPacket[] areplies = new AcceptReplyPacket[members.length];
		for (int i : new ArrayList<Integer>(pcs.myProposals.keySet())) {
			PValuePacket pvalue = pcs.myProposals.get(i).pValuePacket;
			assert (pvalue != null);
			assert (!pcs.preemptedFully());
			for (int j = 0; j < members.length; j++) {
				PValuePacket decision = null;
				if (Math.random() > 0.99) { // Note: a single member's accept
											// reply is enough to preempt
					Ballot b = new Ballot(pcs.myBallotNum + 1,
							pcs.myBallotCoord);
					areplies[j] = new AcceptReplyPacket(members[j], b,
							pvalue.slot, -1);
					decision = pcs.handleAcceptReplyHigherBallot(areplies[j]);
					assert (decision == null || decision.getType() == PaxosPacketType.PREEMPTED);
				} else {
					areplies[j] = new AcceptReplyPacket(members[j], new Ballot(
							pcs.myBallotNum, pcs.myBallotCoord), pvalue.slot,
							-1);
					decision = pcs.handleAcceptReplyMyBallot(members,
							areplies[j]);
					assert (decision == null || decision.getType() == PaxosPacketType.DECISION);
				}
				if (decision != null) {
					if (decision.getType() == PaxosPacketType.DECISION) {
						System.out.println("Accepted by majority decision: "
								+ decision);
						assert (!pcs.myProposals.containsKey(decision.slot));
					} else if (decision.getType() == PaxosPacketType.PREEMPTED) {
						System.out.println("PREEMPTED proposal: " + decision);
						assert (!pcs.myProposals.containsKey(decision.slot));
					}
				}
			}
		}
		assert (pcs.myProposals.isEmpty());

		System.out
				.println("\nSUCCESS! TBD: Only minimally tested. Not stress tested under concurrency.");
	}
}
