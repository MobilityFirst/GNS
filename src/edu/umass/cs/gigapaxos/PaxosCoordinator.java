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
public abstract class PaxosCoordinator {

	private static Logger log = (PaxosManager.getLogger());

	protected static PaxosCoordinator makeCoordinator(PaxosCoordinator c, int bnum,
			int coord, int[] members, int slot, boolean recovery) {
		boolean sendPrepare = false;
		if (c == null || c.getPCS() == null
				|| (c.getPCS().getBallot().compareTo(bnum, coord)) < 0) {
			if(c==null) 
				c = new PaxosCoordinatorState(bnum, coord, slot, members, null);
			if (bnum == 0 || recovery)
				// initial coordinator status assumed, not explicitly prepared.
				c.getPCS().setCoordinatorActive();
			else
				sendPrepare = true
				;
		} 
		else if (c.getPCS() != null && (c.getBallot().compareTo(bnum, coord)) == 0
				&& !c.isActive())
			sendPrepare = true
			; // resend prepare
		assert(c!=null);
		if(sendPrepare) c.getPCS().prepare(members);
		// for ballotnum>0, must explicitly prepare
		return c;
	}

	protected static PaxosCoordinator createCoordinator(int bnum,
			int coord, int[] members, int slot, boolean recovery) {
		PaxosCoordinatorState c = new PaxosCoordinatorState(bnum, coord,
				slot, members, null);
		if (bnum == 0 || recovery)
			// initial coordinator status assumed, not explicitly prepared.
			c.setCoordinatorActive();
		return c;
	}
	
	protected static Ballot remakeCoordinator(PaxosCoordinator c, int[] members) {
		return c!=null ? c.prepare(members) : null;
	}

	protected abstract void setNodeSlots(int[] nodeSlots);
	protected static PaxosCoordinator hotRestore(PaxosCoordinator c, HotRestoreInfo hri) {
		if (hri.coordBallot == null)
			return null;
		PaxosCoordinator coordinator = makeCoordinator(c, hri.coordBallot.ballotNumber,
				hri.coordBallot.coordinatorID, hri.members,
				hri.nextProposalSlot, true);
		assert (coordinator.isActive());
		coordinator.getPCS().setNodeSlots(hri.nodeSlots);
		return coordinator;
	}

	/*
	 * Cease to exist. Internally called when preempted.
	 */
	private synchronized ArrayList<ProposalPacket> resignAsCoordinator() {
		ArrayList<ProposalPacket> preActiveProposals = null;
		if (this.exists() && !getPCS().isActive()) {
			preActiveProposals = getPCS().getPreActiveProposals();
			log.log(Level.FINE,
					"Coordinator {0} resigning; preActiveproposals = ",
					new Object[] { this.getPCS().getBallot(), preActiveProposals });
		}

		// pass these to the coordinator to whom you deferred
		return preActiveProposals; 
	}

	protected abstract ArrayList<ProposalPacket> getPreActiveProposals();

	/*
	 * Returns true if pcs!=null. Existence is different from the coordinator
	 * being active (=pcs.active==true).
	 */
	private synchronized boolean exists() {
		return true;//pcs != null;
	}
	protected static boolean exists(PaxosCoordinator c) {
		return c!=null;
	}

	private synchronized boolean exists(Ballot b) {
		return getPCS() != null && getPCS().getBallot().compareTo(b) >= 0;
	}

	protected static boolean exists(PaxosCoordinator c, Ballot b) {
		return c!=null ? c.exists(b) : false;
	}

	/*
	 * Allows PaxosInstanceStateMachine to forcibly call resignAsCoordinator().
	 * We don't care about the return value (preActives) here.
	 */
	protected static void forceStop(PaxosCoordinator coordinator) {
		if (coordinator != null)
			synchronized (coordinator) {
				coordinator.resignAsCoordinator();
			}
	}

	/*
	 * Phase2a
	 */
	protected abstract AcceptPacket propose(int[] members,
			RequestPacket request);


	protected static AcceptPacket propose(PaxosCoordinator c, int[] groupMembers,
			RequestPacket req) {
		return c!=null ? c.propose(groupMembers, req) : null;
	}
	protected static PValuePacket handleAcceptReply(PaxosCoordinator c, int[] members,
			AcceptReplyPacket acceptReply) {
		return c!=null ? c.handleAcceptReply(members, acceptReply) : null;
	}
	
	private PaxosCoordinator getPCS() {
		assert(this instanceof PaxosCoordinatorState);
		return this;
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
		if (acceptReply.ballot.compareTo(this.getPCS().getBallot()) > 0) {
			if ((preemptedPValue = getPCS()
					.handleAcceptReplyHigherBallot(acceptReply)) != null) {
				/*
				 * Can ignore return value of preActiveProposals below as
				 * handleAcceptReplyHigherBallot returns true only if there are
				 * no proposals at this coordinator.
				 */
				log.log(Level.FINE,
						"Coordinator {0} PREEMPTED {1}:request#{2}",
						new Object[] { this.getPCS().getBallot(),
								acceptReply.getPaxosID(),
								acceptReply.slotNumber });
				assert (preemptedPValue.ballot.compareTo(acceptReply.ballot) < 0);
				if (getPCS().preemptedFully()) {
					log.log(Level.FINE,
							"Coordinator {0} preempted fully, about to resign",
							new Object[] { this.getPCS().getBallot() });
					resignAsCoordinator();
				}
			}
		} else if (acceptReply.ballot.compareTo(getPCS().getBallot()) == 0) {
			committedPValue = getPCS().handleAcceptReplyMyBallot(members,
					acceptReply);
		} else
			log.log(Level.FINE,
					"{0}:{1} acceptor {2} is replying to a lower ballot proposal: {3} < {4} : {5}",
					new Object[] { acceptReply.getPaxosID(),
							acceptReply.getVersion(), acceptReply.acceptor,
							acceptReply.ballot, this.getPCS().getBallotStr(),
							acceptReply });
		// both could be null too
		return committedPValue != null ? committedPValue : preemptedPValue;
	}

	protected abstract boolean preemptedFully();

	protected abstract PValuePacket handleAcceptReplyMyBallot(int[] members,
			AcceptReplyPacket acceptReply);

	protected abstract PValuePacket handleAcceptReplyHigherBallot(
			AcceptReplyPacket acceptReply);

	/*
	 * Phase1a Propose ballot.
	 */
	protected abstract Ballot prepare(int[] members);
	

	protected static ArrayList<AcceptPacket> handlePrepareReply(
			PaxosCoordinator c, PrepareReplyPacket prepareReply, int[] members) {
		return c != null ? c.handlePrepareReply(prepareReply, members) : null;
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

		if (this.getPCS().isPrepareAcceptedByMajority(prepareReply, members)) {
			// ******ensures this block is called exactly once
			assert (!this.getPCS().isActive());
			// okay even for multiple threads to call in parallel
			this.getPCS().combinePValuesOntoProposals(members);
			// should be called only once, o/w conflicts possible
			acceptPacketList = this.getPCS().spawnCommandersForProposals();
			this.getPCS().setCoordinatorActive();
			// *****ensures this block is called exactly once
		} 

		return (acceptPacketList);
	}

	protected abstract void setCoordinatorActive();

	protected abstract ArrayList<AcceptPacket> spawnCommandersForProposals() ;
	protected abstract void combinePValuesOntoProposals(int[] members);

	protected abstract boolean isPrepareAcceptedByMajority(
			PrepareReplyPacket prepareReply, int[] members);

	/*
	 * Phase1b Received prepare reply with higher ballot.
	 */
	protected abstract boolean isPreemptable(PrepareReplyPacket prepareReply);
	private synchronized ArrayList<ProposalPacket> getPreActivesIfPreempted(
			PrepareReplyPacket prepareReply, int[] members) {
		return (this.exists() && !this.isActive() && this.getPCS()
				.isPreemptable(prepareReply)) ? this.resignAsCoordinator()
				: null;
	}
	protected static ArrayList<ProposalPacket> getPreActivesIfPreempted(PaxosCoordinator c,
			PrepareReplyPacket prepareReply, int[] members) {
		return c!=null ? c.getPreActivesIfPreempted(prepareReply, members) : null;
	}

	protected abstract boolean testAndSetWaitingTooLong();
	private synchronized boolean waitingTooLong() {
		return this.exists() && this.getPCS().testAndSetWaitingTooLong() ? true
				: false;
	}
	
	protected static boolean waitingTooLong(PaxosCoordinator c) {
		return c != null ? c.waitingTooLong() : false;
	}

	protected abstract boolean testAndSetWaitingTooLong(int slot);
	private synchronized boolean waitingTooLong(int slot) {
		return (this.exists() && this.isCommandering(slot) && this.getPCS()
				.testAndSetWaitingTooLong(slot)) ? true : false;
	}

	protected abstract boolean isCommandering(int slot);

	protected abstract AcceptPacket reInitCommander(int slot);
	private synchronized AcceptPacket reissueAcceptIfWaitingTooLong(int slot) {
		return (this.isActive() && this.waitingTooLong(slot)) ? this.getPCS()
				.reInitCommander(slot) : null;
	}

	protected static AcceptPacket reissueAcceptIfWaitingTooLong(
			PaxosCoordinator c, int slot) {
		return c != null ? c.reissueAcceptIfWaitingTooLong(slot) : null;
	}

	protected abstract boolean ranRecently();
	
	
	protected static boolean ranRecently(PaxosCoordinator c) {
		return c!=null ? c.ranRecently() : false;
	}

	protected abstract int getMajorityCommittedSlot();
	
	protected static int getMajorityCommittedSlot(PaxosCoordinator c) {
		return c!=null ? c.getMajorityCommittedSlot() : -1;
	}

	protected abstract boolean caughtUp();
	
	protected static boolean caughtUp(PaxosCoordinator c) {
		return c==null || !c.exists() || c.caughtUp();
	}

	protected abstract int getNextProposalSlot();
	
	protected static int getNextProposalSlot(PaxosCoordinator c) {
		return c!=null ? c.getNextProposalSlot() : -1;
	}

	protected abstract int[] getNodeSlots();
	
	protected static int[] getNodeSlots(PaxosCoordinator c) {
		return c!=null ? c.getNodeSlots() : null;
	}


	abstract String getBallotStr();
	
	// unsynchronized to prevent logging console deadlocks
	protected static String getBallotStr(PaxosCoordinator c) {
		return c!=null ? c.getBallotStr() : null;
	}


	abstract Ballot getBallot();

	// unsynchronized to prevent logging console deadlocks
	protected static Ballot getBallot(PaxosCoordinator c) {
		return c!=null ? c.getBallot() : null;
	}

	protected abstract boolean isActive();
	
	
	protected static boolean isActive(PaxosCoordinator c) {
		return c!=null ? c.isActive() : false;
	}

	public String toString() {
		return "{Coordinator=" + (getPCS() != null ? this.getPCS().toString() : null)
				+ "}";
	}

	protected void testingInitCoord(int load) {
		if (getPCS() != null)
			getPCS().testingInitCoord(load);
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