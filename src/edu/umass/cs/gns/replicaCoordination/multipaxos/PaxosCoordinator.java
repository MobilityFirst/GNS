package edu.umass.cs.gns.replicaCoordination.multipaxos;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import java.util.ArrayList;
import java.util.logging.Logger;

import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptReplyPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PrepareReplyPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.HotRestoreInfo;

/**
@author V. Arun
 */

/* This class is a paxos coordinator. It is really a shell to 
 * wrap PaxosCoordinator state and exists for the 
 * following reasons: (1) it allows multiple operations over
 * PaxosCoordinatorState to be atomically; (2) it hides from 
 * PaxosInstanceStateMachine any synchronization issues wrt 
 * PaxosCoordinatorState (just like with PaxosAcceptorState); 
 * (3) it keeps the code in PaxosCoordinator and 
 * PaxosCoordinatorState more readable and closer to Rensesse's 
 * Paxos Made Moderately Complex pseudocode; (4) it uses little
 * empty space for the coordinator when a paxos instance is not
 * a coordinator. The alternative would be to nullify 
 * PaxosCoordinatorState when it is not the coordinator (making
 * synchronization uglier without the shell) or let it use empty 
 * space. Space is only a minor reason as the PaxosCoordinatorState 
 * internally uses optimized structures because of which empty
 * space adds only a few tens of bytes and still keeps the *total*
 * space used by an inactive paxos instance under 250B. An active 
 * coordinator or acceptor on the other hand could easily use 
 * 1000B or more. 
 * 
 * Notes: (1) This class itself has no state other than the
 * PaxosCoordinatorState pointer. (2) Most methods require 
 * synchronization for atomicity even though PaxosCoordinatoState
 * is internally synchronized.
 * 
 * Testability: This class is not unit-testable as it is just as shell
 * for PaxosCoordinatorState. Both PaxosManager and TESTPaxosMain test 
 * this class along with PaxosInstanceStateMachine.
 */
public class PaxosCoordinator {
	public static final boolean DEBUG=PaxosManager.DEBUG;
	private PaxosCoordinatorState pcs=null; // This gets recreated for each ballot.

	private static Logger log = Logger.getLogger(PaxosCoordinator.class.getName()); // GNS.getLogger();

	/* Come to exist if nonexistent. Called by PaxosInstanceStateMachine
	 */
	protected synchronized Ballot makeCoordinator(int bnum, NodeId<String> coord, NodeId<String>[] members, int slot, boolean recovery) {
		boolean sendPrepare=false;
		if(pcs==null || (pcs.getBallot().compareTo(bnum, coord))<0) {
			pcs = new PaxosCoordinatorState(bnum, coord, slot, members, pcs);
			if(bnum==0 || recovery) pcs.setCoordinatorActive(); // Initial coordinator status assumed, not explicitly prepared.
			else sendPrepare=true;
		} else if(pcs!=null && (pcs.getBallot().compareTo(bnum, coord))==0 && !pcs.isActive()) sendPrepare = true; // resend prepare 

		return sendPrepare ? pcs.prepare(members) : null; // For ballotnum>0, must explicitly prepare
	}
	protected synchronized Ballot remakeCoordinator(NodeId<String>[] members) {
		return (this.exists() && !this.isActive() ? pcs.prepare(members): null);
	}
	protected synchronized Ballot hotRestore(HotRestoreInfo hri) {
		if(hri.coordBallot==null) return null;
		Ballot ballot = makeCoordinator(hri.coordBallot.ballotNumber, hri.coordBallot.coordinatorID, 
				hri.members, hri.nextProposalSlot, true);
		assert(this.isActive());
		this.pcs.setNodeSlots(hri.nodeSlots);
		return ballot;
	}
	/* Cease to exist. Internally called when preempted. 
	 */
	private synchronized ArrayList<ProposalPacket> resignAsCoordinator() {
		ArrayList<ProposalPacket> preActiveProposals=null;
		if(this.exists() && !pcs.isActive()) {
			preActiveProposals = pcs.getPreActiveProposals();
			log.info("Coordinator "+this.pcs.getBallot() + " resigning, " +
					"preActive proposals = " + preActiveProposals);
		}

		pcs = null; // The main point of this method.

		return preActiveProposals; // Pass these to the coordinator to whom you deferred
	}
	/* Returns true if pcs!=null. Existence is different from 
	 * the coordinator being active (=pcs.active==true).
	 */
	protected synchronized boolean exists() {return pcs!=null;}
	protected synchronized boolean exists(Ballot b) {return pcs!=null && pcs.getBallot().compareTo(b)>=0;}

	/* Allows PaxosInstanceStateMachine to forcibly call resignAsCoordinator().
	 * We don't care about the return value (preActives) here.
	 */
	protected synchronized void forceStop() {this.resignAsCoordinator();}

	/* Phase2a
	 */
	protected synchronized AcceptPacket propose(NodeId<String>[] groupMembers, RequestPacket req) {
		if(!this.exists()) log.severe("Coordinator resigned after check, DROPPING request: " + req);
		return this.exists() ? this.pcs.propose(groupMembers, req) : null;
	}

	/* Phase2b
	 */
	protected synchronized PValuePacket handleAcceptReply(NodeId<String>[] members, AcceptReplyPacket acceptReply) {
		if(!this.exists() || !this.isActive()) return null;

		PValuePacket committedPValue=null; PValuePacket preemptedPValue=null;
		if(acceptReply.ballot.compareTo(this.pcs.getBallot()) > 0) {
			if((preemptedPValue = pcs.handleAcceptReplyHigherBallot(acceptReply))!=null) {
				/* Can ignore return value of preActiveProposals below as handleAcceptReplyHigherBallot 
				 * returns true only if there are no proposals at this coordinator.
				 */
				//if(DEBUG) 
				log.info("Coordinator " + this.pcs.getBallot() + " PREEMPTED request#: " + 
						acceptReply.slotNumber + ", " + acceptReply.getPaxosID());
				assert(preemptedPValue.ballot.compareTo(acceptReply.ballot) < 0);
				if(pcs.preemptedFully()) {
					log.info("Coordinator " + this.pcs.getBallot() + " preempted fully, about to resign");
					resignAsCoordinator();
				}
			}
		}
		else if(acceptReply.ballot.compareTo(pcs.getBallot()) == 0) {
			committedPValue = pcs.handleAcceptReplyMyBallot(members, acceptReply);
		} else assert(false):"YIKES! Acceptor "+acceptReply.nodeID+" replied without updating its ballot";
		return committedPValue!=null ? committedPValue : preemptedPValue; // both could be null too
	}

	/* Phase1a
	 * Propose ballot.
	 */
	protected synchronized Ballot prepare(NodeId<String>[] members) {return this.pcs.prepare(members);}
	/* Phase1b
	 * Event: Received a prepare reply message.
	 * Action: Resign if reply contains higher ballot as we are
	 * guaranteed to lose the election. Else if this prepare reply 
	 * constitutes a majority, combine received pvalues onto 
	 * existing proposals, spawn commanders for all proposals, 
	 * and set coordinator to active state.
	 * 
	 * Return: The set of accept messages (phase2a) to be sent out
	 * corresponding to spawned commanders.
	 */
	protected synchronized ArrayList<AcceptPacket> handlePrepareReply(PrepareReplyPacket prepareReply, NodeId<String>[] members) {
		if(!this.exists()) return null; 
		ArrayList<AcceptPacket> acceptPacketList = null;

		if(this.pcs.isPrepareAcceptedByMajority(prepareReply, members)) { // pcs still valid
			assert(!this.pcs.isActive());   // ******ensures this else block is called exactly once
			this.pcs.combinePValuesOntoProposals(members); // okay even for multiple threads to call in parallel
			acceptPacketList = this.pcs.spawnCommandersForProposals(); // should be called only once, o/w conflicts possible
			this.pcs.setCoordinatorActive(); // *****ensures this else block is called exactly once
			log.info("Coordinator " + pcs.getBallot() + " acquired PREPARE MAJORITY: About to conduct view change.");
		} // "synchronized" in the method definition ensures that this else block is called atomically 

		return (acceptPacketList);
	}

	/* Phase1b
	 * Received prepare reply with higher ballot.
	 */
	protected synchronized ArrayList<ProposalPacket> getPreActivesIfPreempted(PrepareReplyPacket prepareReply, NodeId<String>[] members) {
		return (this.exists() && !this.isActive() && this.pcs.isPreemptable(prepareReply)) ? this.resignAsCoordinator() : null;
	}
	protected synchronized boolean isOverloaded(int acceptorSlot) {
		return this.isActive() ? this.pcs.isOverloaded(acceptorSlot) : false;
	}
	protected synchronized boolean waitingTooLong() {
		return this.exists() && this.pcs.waitingTooLong() ? true : false;
	}
	protected synchronized boolean waitingTooLong(int slot) {
		return (this.exists() && this.isCommandering(slot) && this.pcs.waitingTooLong(slot)) ? true : false;
	}
	protected synchronized boolean isCommandering(int slot) {
		return (this.isActive() && this.pcs.isCommandering(slot)) ? true : false;
	}
	protected synchronized AcceptPacket reissueAcceptIfWaitingTooLong(int slot) {
		return (this.isActive() && this.waitingTooLong(slot)) ? this.pcs.reInitCommander(slot) : null;
	}
	protected synchronized boolean ranRecently() {
		return (this.exists() && this.pcs.ranRecently());
	}
	protected synchronized Ballot getBallot() {return this.exists() ? this.pcs.getBallot() : null;} 
	protected synchronized String getBallotStr() {return this.exists() ? this.pcs.getBallotStr() : null;} 
	protected int getMajorityCommittedSlot() {return (this.isActive() ? this.pcs.getMajorityCommittedSlot():-1);}
	protected synchronized boolean caughtUp() {return !this.exists() || this.pcs.caughtUp();}
	protected synchronized int getNextProposalSlot() {return this.exists() ? this.pcs.getNextProposalSlot() : -1;}
	protected synchronized int[] getNodeSlots() {return this.exists() ? this.pcs.getNodeSlots() : null;}

	// Testing methods
	protected boolean isActive() {return this.exists() && this.pcs.isActive();}

	public String toString() {return "{Coordinator=" + (pcs!=null ? this.pcs.toString() : null)+"}";}
	protected void testingInitCoord(int load) {if(pcs!=null) pcs.testingInitCoord(load);}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("FAILURE: This is an untested shell class for PaxosCoordinatorState that is tested.");
		System.out.println("Try running PaxosManager's tests for now.");
	}
}
