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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.gigapaxos.paxospackets.AcceptPacket;
import edu.umass.cs.gigapaxos.paxospackets.AcceptReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PrepareReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.ProposalPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.HotRestoreInfo;

/**
 * @author V. Arun
 */

/*
 * This class is a paxos coordinator. It is really a shell to wrap
 * PaxosCoordinator state and exists for the following reasons: (1) it allows
 * multiple operations over PaxosCoordinatorState to be atomically; (2) it hides
 * from PaxosInstanceStateMachine any synchronization issues wrt
 * PaxosCoordinatorState (just like with PaxosAcceptorState); (3) it keeps the
 * code in PaxosCoordinator and PaxosCoordinatorState more readable and closer
 * to Rensesse's Paxos Made Moderately Complex pseudocode; (4) it uses little
 * empty space for the coordinator when a paxos instance is not a coordinator.
 * The alternative would be to nullify PaxosCoordinatorState when it is not the
 * coordinator (making synchronization uglier without the shell) or let it use
 * empty space. Space is only a minor reason as the PaxosCoordinatorState
 * internally uses optimized structures because of which empty space adds only a
 * few tens of bytes and still keeps the *total* space used by an inactive paxos
 * instance under 250B. An active coordinator or acceptor on the other hand
 * could easily use 1000B or more.
 * 
 * Notes: (1) This class itself has no state other than the
 * PaxosCoordinatorState pointer. (2) Most methods require synchronization for
 * atomicity even though PaxosCoordinatoState is internally synchronized.
 * 
 * Testability: This class is not unit-testable as it is just as shell for
 * PaxosCoordinatorState. Both PaxosManager and TESTPaxosMain test this class
 * along with PaxosInstanceStateMachine.
 */
public class PaxosCoordinator {
	private PaxosCoordinatorState pcs = null; // This gets recreated for each
												// ballot.

	private static Logger log = // PaxosManager.getLogger();//
	Logger.getLogger(PaxosCoordinator.class.getName());

	/*
	 * Come to exist if nonexistent. Called by PaxosInstanceStateMachine
	 */
	protected synchronized Ballot makeCoordinator(int bnum, int coord,
			int[] members, int slot, boolean recovery) {
		boolean sendPrepare = false;
		if (pcs == null || (pcs.getBallot().compareTo(bnum, coord)) < 0) {
			pcs = new PaxosCoordinatorState(bnum, coord, slot, members, pcs);
			if (bnum == 0 || recovery)
				pcs.setCoordinatorActive(); // Initial coordinator status
											// assumed, not explicitly prepared.
			else
				sendPrepare = true;
		} else if (pcs != null && (pcs.getBallot().compareTo(bnum, coord)) == 0
				&& !pcs.isActive())
			sendPrepare = true; // resend prepare

		// for ballotnum>0, must explicitly prepare
		return sendPrepare ? pcs.prepare(members) : null; 
	}

	protected synchronized Ballot remakeCoordinator(int[] members) {
		return (this.exists() && !this.isActive() ? pcs.prepare(members) : null);
	}

	protected synchronized Ballot hotRestore(HotRestoreInfo hri) {
		if (hri.coordBallot == null)
			return null;
		Ballot ballot = makeCoordinator(hri.coordBallot.ballotNumber,
				hri.coordBallot.coordinatorID, hri.members,
				hri.nextProposalSlot, true);
		assert (this.isActive());
		this.pcs.setNodeSlots(hri.nodeSlots);
		return ballot;
	}

	/*
	 * Cease to exist. Internally called when preempted.
	 */
	private synchronized ArrayList<ProposalPacket> resignAsCoordinator() {
		ArrayList<ProposalPacket> preActiveProposals = null;
		if (this.exists() && !pcs.isActive()) {
			preActiveProposals = pcs.getPreActiveProposals();
			log.log(Level.FINE,
					"Coordinator {0} resigning; preActiveproposals = ",
					new Object[] { this.pcs.getBallot(), preActiveProposals });
		}

		pcs = null; // main point of this method.

		// pass these to the coordinator to whom you deferred
		return preActiveProposals; 
	}

	/*
	 * Returns true if pcs!=null. Existence is different from the coordinator
	 * being active (=pcs.active==true).
	 */
	protected synchronized boolean exists() {
		return pcs != null;
	}

	protected synchronized boolean exists(Ballot b) {
		return pcs != null && pcs.getBallot().compareTo(b) >= 0;
	}

	/*
	 * Allows PaxosInstanceStateMachine to forcibly call resignAsCoordinator().
	 * We don't care about the return value (preActives) here.
	 */
	protected synchronized void forceStop() {
		this.resignAsCoordinator();
	}

	/*
	 * Phase2a
	 */
	protected synchronized AcceptPacket propose(int[] groupMembers,
			RequestPacket req) {
		if (!this.exists())
			log.severe("Coordinator resigned after check, DROPPING request: "
					+ req.getSummary());
		return this.exists() ? this.pcs.propose(groupMembers, req) : null;
	}

	/*
	 * Phase2b
	 */
	protected synchronized PValuePacket handleAcceptReply(int[] members,
			AcceptReplyPacket acceptReply) {
		if (!this.exists() || !this.isActive())
			return null;

		PValuePacket committedPValue = null;
		PValuePacket preemptedPValue = null;
		if (acceptReply.ballot.compareTo(this.pcs.getBallot()) > 0) {
			if ((preemptedPValue = pcs
					.handleAcceptReplyHigherBallot(acceptReply)) != null) {
				/*
				 * Can ignore return value of preActiveProposals below as
				 * handleAcceptReplyHigherBallot returns true only if there are
				 * no proposals at this coordinator.
				 */
				log.log(Level.FINE,
						"Coordinator {0} PREEMPTED {1}:request#{2}",
						new Object[] { this.pcs.getBallot(),
								acceptReply.getPaxosID(),
								acceptReply.slotNumber });
				assert (preemptedPValue.ballot.compareTo(acceptReply.ballot) < 0);
				if (pcs.preemptedFully()) {
					log.log(Level.FINE,
							"Coordinator {0} preempted fully, about to resign",
							new Object[] { this.pcs.getBallot() });
					resignAsCoordinator();
				}
			}
		} else if (acceptReply.ballot.compareTo(pcs.getBallot()) == 0) {
			committedPValue = pcs.handleAcceptReplyMyBallot(members,
					acceptReply);
		} else
			log.log(Level.INFO,
					"{0}:{1} acceptor {1} is replying to a lower ballot proposal: {2} < {3} : {4}",
					new Object[] { acceptReply.getPaxosID(),
							acceptReply.getVersion(), acceptReply.acceptor,
							acceptReply.ballot, this.pcs.getBallotStr(),
							acceptReply });
		// both could be null too
		return committedPValue != null ? committedPValue : preemptedPValue;
	}

	/*
	 * Phase1a Propose ballot.
	 */
	protected synchronized Ballot prepare(int[] members) {
		return this.pcs.prepare(members);
	}

	/*
	 * Phase1b Event: Received a prepare reply message. Action: Resign if reply
	 * contains higher ballot as we are guaranteed to lose the election. Else if
	 * this prepare reply constitutes a majority, combine received pvalues onto
	 * existing proposals, spawn commanders for all proposals, and set
	 * coordinator to active state.
	 * 
	 * Return: The set of accept messages (phase2a) to be sent out corresponding
	 * to spawned commanders.
	 */
	protected synchronized ArrayList<AcceptPacket> handlePrepareReply(
			PrepareReplyPacket prepareReply, int[] members) {
		if (!this.exists())
			return null;
		ArrayList<AcceptPacket> acceptPacketList = null;

		if (this.pcs.isPrepareAcceptedByMajority(prepareReply, members)) {
			// ******ensures this block is called exactly once
			assert (!this.pcs.isActive());
			// okay even for multiple threads to call in parallel
			this.pcs.combinePValuesOntoProposals(members);
			// should be called only once, o/w conflicts possible
			acceptPacketList = this.pcs.spawnCommandersForProposals();
			this.pcs.setCoordinatorActive();
			// *****ensures this block is called exactly once
		} // "synchronized" method ensures that this else block is called
			// atomically

		return (acceptPacketList);
	}

	/*
	 * Phase1b Received prepare reply with higher ballot.
	 */
	protected synchronized ArrayList<ProposalPacket> getPreActivesIfPreempted(
			PrepareReplyPacket prepareReply, int[] members) {
		return (this.exists() && !this.isActive() && this.pcs
				.isPreemptable(prepareReply)) ? this.resignAsCoordinator()
				: null;
	}

	protected synchronized boolean waitingTooLong() {
		return this.exists() && this.pcs.testAndSetWaitingTooLong() ? true
				: false;
	}

	protected synchronized boolean waitingTooLong(int slot) {
		return (this.exists() && this.isCommandering(slot) && this.pcs
				.testAndSetWaitingTooLong(slot)) ? true : false;
	}

	protected synchronized boolean isCommandering(int slot) {
		return (this.isActive() && this.pcs.isCommandering(slot)) ? true
				: false;
	}

	protected synchronized AcceptPacket reissueAcceptIfWaitingTooLong(int slot) {
		return (this.isActive() && this.waitingTooLong(slot)) ? this.pcs
				.reInitCommander(slot) : null;
	}

	protected boolean ranRecently() {
		PaxosCoordinatorState coordState = this.pcs;
		return (coordState != null && coordState.ranRecently());
	}

	protected int getMajorityCommittedSlot() {
		return (this.isActive() ? this.pcs.getMajorityCommittedSlot() : -1);
	}

	protected synchronized boolean caughtUp() {
		return !this.exists() || this.pcs.caughtUp();
	}

	protected synchronized int getNextProposalSlot() {
		return this.exists() ? this.pcs.getNextProposalSlot() : -1;
	}

	protected synchronized int[] getNodeSlots() {
		return this.exists() ? this.pcs.getNodeSlots() : null;
	}

	// unsynchronized to prevent logging console deadlocks
	protected String getBallotStr() {
		PaxosCoordinatorState coordState = this.pcs;
		return coordState != null ? coordState.getBallotStr() : null;
	}

	// unsynchronized to prevent logging console deadlocks
	protected Ballot getBallot() {
		PaxosCoordinatorState coordState = this.pcs;
		return coordState != null ? coordState.getBallot() : null;
	}

	// FIXME: needs to be synchronized 
	protected boolean isActive() {
		return this.pcs != null && this.pcs.isActive();
	}

	public String toString() {
		return "{Coordinator=" + (pcs != null ? this.pcs.toString() : null)
				+ "}";
	}

	protected void testingInitCoord(int load) {
		if (pcs != null)
			pcs.testingInitCoord(load);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out
				.println("FAILURE: This is an untested shell class for PaxosCoordinatorState that is tested. "
						+ "Try running PaxosManager's tests for now.");
	}
}
