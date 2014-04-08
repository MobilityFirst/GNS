package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;

import edu.umass.cs.gns.nsdesign.packet.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptReplyPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PrepareReplyPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.WaitForUtility;
import edu.umass.cs.gns.util.NullIfEmptyMap;

/* This class manages paxos coordinator state.
 * It has no public methods, only protected methods,
 * as it is used *only* by PaxosCoordinator.
 * 
 * An inactive PaxosCoordinatorState instance, i.e., when 
 * the corresponding node is a coordinator, but this paxos
 * instance is not processing any requests uses about 50B
 * plus an int each for each member of nodeSlotNumbers.
 */
/**
@author V. Arun
 */
public class PaxosCoordinatorState  {
	private static final String NO_OP = "NO_OP";
	private static final String STOP = "STOP";
	private static final int MAX_ELECTION_WAIT_TIME = 30000; // ms
	public static final boolean DEBUG=PaxosManager.DEBUG;


	// final ballot, takes birth and dies with this PaxosCoordinatorState
	private final int myBallotNum; // using two ints is 24 bytes less than Ballot
	private final int myBallotCoord;
	
	// List of replicas who have accepted my ballot
	private WaitForUtility waitforMyBallot=null; // non-null only until the coordinator becomes active

	/* List of proposals carried over from lower ballots to be re-proposed or committed in my ballot.
	 * Non-null only untul the coordinator becomes active.
	 */
	private NullIfEmptyMap<Integer,PValuePacket> carryoverProposals=new NullIfEmptyMap<Integer,PValuePacket>(); 

	/* List of proposals I am currently trying to push in my ballot as coordinator (or when I get the election 
	 * majority to be a coordinator). Typically, this map is the only non-trivial space consumer when a
	 * paxos instance is actively processing requests. The size of this map depends on the rate of incoming
	 * requests and paxos throughput typically, and depends on the number of adopted proposals from
	 * lower ballots during coordinator changes. The latter in turn depends on how quickly a majority
	 * of instances commit a decision, as accepts below the majority committed frontier are garbage
	 * collected.
	 */
	private NullIfEmptyMap<Integer, ProposalStateAtCoordinator> myProposals=new NullIfEmptyMap<Integer, ProposalStateAtCoordinator>();

	private int nextProposalSlotNumber = 0; // next free slot number to propose

	/* After this coordinator becomes active, it remains active until it no longer
	 * exists. That is, we never set active back to false again. Instead, this 
	 * coordinator state is expunged and new coordinator state (for a different 
	 * ballot) may get created in the future.
	 */
	private boolean active = false; 

	/* This field is useful for garbage collection. It is maintained here 
	 * so that we can piggyback on coordinator back-and-forth messages
	 * in order to disseminate the maximum slot cumulatively executed at 
	 * a majority of acceptors, which is useful for garbage collecting 
	 * accepted proposals at acceptors. It works in two phases as follows. 
	 * 
	 * Before a coordinator becomes active, nodeSlotNumbers[i] maintains the 
	 * slot number below which acceptor i has garbage collected accepted 
	 * proposals. The acceptor maintains this slot in the field 
	 * minCommittedFrontierSlot and sends it to the coordinator in its 
	 * prepare reply. It is important for a coordinator to know while 
	 * processing an acceptor's prepare reply from which slot the 
	 * carryoverProposals reported by an acceptor *starts* so that the 
	 * coordinator does not incorrectly assume an empty slot in the reported 
	 * carryoverProposals for a no-op slot (a safety violation). Upon getting 
	 * elected by getting non-preempting prepare replies from a majority of 
	 * acceptors, the coordinator uses the maximum starting slot across this 
	 * majority of replicas as the first slot that it will try to get 
	 * accepted in its ballot. 
	 * 
	 * After becoming active, nodeSlotNumbers[i] maintains the slot up to 
	 * which acceptor i has cumulatively executed requests. Acceptors provide 
	 * this information to the coordinator through accept reply messages. 
	 * Based on this information in nodeSlotNumbers, the active coordinator 
	 * in its accept messages tells acceptors the slot number up to which a 
	 * majority of replicas have cumulatively executed requests. The acceptor 
	 * updates this information in its minCommittedFrontierSlot field and 
	 * garbage collects accepted proposals with lower slot numbers, and uses
	 * this field to report to the next coordinator as in the pre-active
	 * phase described above. 
	 * 
	 * Notes: (1) We don't bother keeping track of members here because that 
	 * is sorted and final in the paxos instance and is always passed as an 
	 * argument when needed here. (2) All of this garbage collection is 
	 * referring to an acceptor's acceptedProposals; prepares and decisions
	 * are garbage collected simply below the checkpoint.
	 */
	private int[] nodeSlotNumbers=null;

	private static Logger log = Logger.getLogger(PaxosCoordinatorState.class.getName()); // GNS.getLogger();

	// Used in myProposals map above and nowhere else
	private class ProposalStateAtCoordinator {
		final PValuePacket pValuePacket;
		final WaitForUtility waitfor;
		ProposalStateAtCoordinator(int[] members, PValuePacket pValuePacket) {
			this.pValuePacket = pValuePacket;
			this.waitfor = new WaitForUtility(members);
		}
		public String toString() {return  this.pValuePacket + this.waitfor.toString();}
	}
	
	/* Coordinator state is born into the ballot myBallot. 
	 * myBallot can never change.
	 */
	PaxosCoordinatorState(int bnum, int coord, int slot, int[] members) { 
		this.myBallotNum = bnum; this.myBallotCoord=coord; 
		this.nextProposalSlotNumber=slot;
		this.nodeSlotNumbers = new int[members.length];
		for(int i=0; i<members.length; i++) this.nodeSlotNumbers[i]=0;
	}

	/* Phase1a
	 * Event: A call to try to become coordinator.
	 * Action: If I am not already a coordinator, initiate a waitfor
	 * to start receiving prepare replies. 
	 * 
	 * Return: The ballot. The caller will send out the ballot
	 * to all nodes.
	 */
	protected synchronized Ballot prepare(int[] members) {
		if(this.active==true) return null; // I am already an active coordinator 
		if(this.waitforMyBallot==null) this.waitforMyBallot = new WaitForUtility(members);
		return new Ballot(this.myBallotNum, this.myBallotCoord);
	}

	/* Phase2a 
	 * Event: A call to propose a request with any available slot number.
	 * Action: Assign the next available slot number and add it to the list
	 * of proposals I am pushing for. Actually send it out by invoking 
	 * initCommander only if I am active as a coordinator. Otherwise, I 
	 * am still in the process of receiving pvalues accepted by other nodes 
	 * in lower ballots that might supersede my proposals.
	 * 
	 * Return: An AcceptPacket if any that will be sent out by the caller to 
	 * other nodes so as to actually propose this request.
	 */
	protected synchronized AcceptPacket propose(int[] members, RequestPacket request) {
		if(this.myProposals.containsKey(this.nextProposalSlotNumber-1) && // no point enqueuing anything after stop
				this.myProposals.get(this.nextProposalSlotNumber-1).pValuePacket.isStopRequest()) return null; 
		AcceptPacket acceptPacket=null;
		Ballot myBallot = new Ballot(this.myBallotNum, this.myBallotCoord);
		PValuePacket pvalue = new PValuePacket(myBallot, new ProposalPacket(this.nextProposalSlotNumber++, request)); 
		assert(!this.myProposals.containsKey(pvalue.slot)); // nextSlot should always be free
		this.myProposals.put(pvalue.slot, new ProposalStateAtCoordinator(members,pvalue));
		if(DEBUG) log.info("Node " + myBallot.coordinatorID + " inserted proposal: " + pvalue);
		if(this.isActive()) {
			if(DEBUG) log.finest("Coordinator at node " + myBallot.coordinatorID + " is active");
			acceptPacket =  this.initCommander(members, pvalue);
		} else {
			if(DEBUG) log.fine("Coordinator at node " + myBallot.coordinatorID + " is not active");
			/* Got to wait till view change is complete as proposals for a slot 
			 * can change in the process. Do nothing for now.
			 */
		}
		return acceptPacket;
	} 

	/*********************** Start of Phase1b methods ************************/


	/* Phase1b
	 * If I receive a prepare reply with a higher ballot, I am guaranteed
	 * to lose the election, so concede defeat already.
	 */
	protected synchronized boolean isPreemptable(PrepareReplyPacket prepareReply) {
		boolean preempt=false;
		if(prepareReply.ballot.compareTo(new Ballot(this.myBallotNum, this.myBallotCoord)) > 0) {
			preempt = true;
		}
		return preempt;
	}

	/* Phase1b
	 * Event: Received a prepare reply for my ballot or lower.
	 * Return: true if prepare reply should be ignored. A prepare 
	 * message is sent by a coordinator to initiate a ballot. The 
	 * coordinator waits for prepare replies containing its proposed
	 * ballot. Lower ballots can be ignored. Higher ballots mean that
	 * I should resign.
	 */
	private synchronized boolean canIgnorePrepareReply(PrepareReplyPacket prepareReply) {
		// I am not even trying to be a coordinator.
		if(this.waitforMyBallot==null) return true; 

		// The replica already answered my ballot request.
		else if(!this.waitforMyBallot.updateHeardFrom(prepareReply.receiverID)) return true;

		/* This should not usually happen. Why did the replica bother to send me a lower 
		 * ballot in the first place? It can happen is some replica is recovering and
		 * replaying logged messages with networking turned on.
		 */
		else if(prepareReply.ballot.compareTo(new Ballot(this.myBallotNum, this.myBallotCoord)) < 0) {
			log.warning("Node " + this.myBallotCoord + " received outdated prepare reply " +
					"which should normally happen only during recovery): " + prepareReply);
			return true;
		}

		return false;
	}

	/* Phase1b
	 * Event: Received prepare reply for my ballot.
	 * Action: Update waitfor to check for majority in ballot.
	 * 
	 * Return: true if my ballot has been accepted by a majority.
	 * 
	 * Note: If accepted by a majority, we next need to do some
	 * nontrivial processing of pvalues received in these prepare
	 * replies. The caller will next invoke combinePValuesOntoProposals.
	 */
	protected synchronized boolean isPrepareAcceptedByMajority(PrepareReplyPacket prepareReply, int[] members) {
		if(this.canIgnorePrepareReply(prepareReply)) {
			if(DEBUG) log.info("Node " + this.myBallotCoord+ " ignoring prepare reply"); 
			return false;
		}
		// isPreemptable and canIgnorePrepareReply should have been called already, hence the assert
		assert(prepareReply.ballot.compareTo(new Ballot(this.myBallotNum, this.myBallotCoord))==0) : 
			"prepareReply.ballot = "+prepareReply.ballot + ", myBallot = "+this.myBallotNum+":"+this.myBallotCoord;

		recordSlotNumber(members, prepareReply); // useful in combinePValuesOntoProposals
		
		boolean acceptedByMajority = false; 
		for(PValuePacket pvalue : prepareReply.accepted.values()) {
			int curSlot = pvalue.slot;
			/* The latter part of the disjunction below is because some replica(s) may have
			 * accepted a different proposal for curSlot in a higher ballot, so we should
			 * choose the proposal accepted for a slot in the highest ballot.
			 */
			PValuePacket existing = this.carryoverProposals.get(curSlot);
			if (existing==null || pvalue.ballot.compareTo(existing.ballot) > 0)  {
				this.carryoverProposals.put(pvalue.slot, pvalue);
			} else if (pvalue.ballot.compareTo(existing.ballot) == 0) {assert(pvalue.requestValue.equals(existing.requestValue));}
		}
		waitforMyBallot.updateHeardFrom(prepareReply.receiverID);
		if(DEBUG) log.finest("Waitfor = " + waitforMyBallot.toString());
		if(this.waitforMyBallot.heardFromMajority()) {
			acceptedByMajority = true;
		}
		return acceptedByMajority;
	}

	/* Phase1b
	 * Called after the above method by caller. We could
	 * also call it from the above method instead, but 
	 * having the caller invoke these methods makes them
	 * more readable.
	 * 
	 * This method combines the received pvalues with
	 * existing proposals so that the former supersedes
	 * the latter. This completes the "proposals*pmax(pvals)"
	 * step. The pmax() was already being done as pvalues 
	 * were being received.
	 */
	protected synchronized void combinePValuesOntoProposals(int[] members) {
		if(this.carryoverProposals.isEmpty()) return; // no need to process stop requests either
		
		int maxCarryoverSlot = getMaxPValueSlot(this.carryoverProposals);
		int maxMinCarryoverSlot = this.getMaxMinCarryoverSlot(); 
		
		/* Combine carryoverProposals with myProposals prioritizing the former
		 * and selecting no-ops for slots for which neither contain a value.
		 */
		for (int curSlot = maxMinCarryoverSlot; curSlot <= maxCarryoverSlot; curSlot++) {
			if(this.carryoverProposals.containsKey(curSlot)) { // received pvalues dominate local proposals
				this.myProposals.put(curSlot, 
						new ProposalStateAtCoordinator(members, this.carryoverProposals.get(curSlot)));
			} else if(!this.myProposals.containsKey(curSlot)) { // no-ops if neither received nor local proposal
				this.myProposals.put(curSlot, 
						new ProposalStateAtCoordinator(members, makeNoopPValue(curSlot)));
			}
		}
		/* The next free slot number to use for proposals should be the maximum of 
		 * the maximum slot in received pvalues, and the nextProposalSlotNumber 
		 * that reflects the highest locally proposed slot number. 
		 */
		this.nextProposalSlotNumber = Math.max(maxCarryoverSlot+1, this.nextProposalSlotNumber);
		assert(noGaps(maxMinCarryoverSlot, this.nextProposalSlotNumber, this.myProposals)); // gaps means state machine will be stuck

		this.reproposePreemptedProposals(maxMinCarryoverSlot, members);
		
		processStop(members); // need to ensure that a regular request does not follow a stop
	}
	/* Phase1b
	 * Utility method invoked in combinePValuesOntoProposals to repropose slot numbers
	 * below maxMinCarryoverSlot with slot numbers >= nextProposalSlotNumber. We could
	 * also safely just drop these requests, but why be so heartless.
	 */
	private synchronized void reproposePreemptedProposals(int maxMinCarryoverSlot, int[] members) {
		// Note: Can't use iterators here because propose(.) modifies the map
		for(int curSlot : new ArrayList<Integer>(this.myProposals.keySet())) { 
			if(curSlot < maxMinCarryoverSlot) {
				AcceptPacket accept = this.propose(members, this.myProposals.get(curSlot).pValuePacket); 
				this.myProposals.remove(curSlot);
				assert(accept==null); // because we are not active yet
			}
		}
	}
	/* Phase1b
	 * Utility method to handle stop requests correctly after combining pvalues 
	 * on to proposals. The goal is to make sure that there is no request after
	 * a stop request. If there is a request after a stop in a higher ballot, 
	 * the stop has to be turned into a noop. If there is a request after a 
	 * stop in a lower ballot, the request should be turned into a stop.
	 */
	private synchronized void processStop(int[] members) {
		ArrayList<ProposalStateAtCoordinator> modified = new ArrayList<ProposalStateAtCoordinator>(); 
		boolean stopExists = false;
		for(ProposalStateAtCoordinator psac1 : this.myProposals.values()) {
			if(!psac1.pValuePacket.isStopRequest()) continue; else stopExists = true;
			for(ProposalStateAtCoordinator psac2 : this.myProposals.values()) {
				if(psac1.pValuePacket.isStopRequest() && // 1 is stop
						!psac2.pValuePacket.isStopRequest() && // other is not stop
						!psac2.pValuePacket.requestValue.equals(NO_OP) && // other is not noop
						psac1.pValuePacket.slot < psac2.pValuePacket.slot)  // other has a higher slot
				{
					if(psac1.pValuePacket.ballot.compareTo(psac2.pValuePacket.ballot) > 0) { // stop ballot > other ballot
						// convert request (psac2) to stop (psac1)
						int reqSlot = psac2.pValuePacket.slot;
						ProposalStateAtCoordinator psac2ToStop = new ProposalStateAtCoordinator(members, 
								new PValuePacket(new Ballot(this.myBallotNum, this.myBallotCoord), 
										new ProposalPacket(reqSlot, psac1.pValuePacket)));
						modified.add(psac2ToStop);
					}
					else if(psac1.pValuePacket.ballot.compareTo(psac2.pValuePacket.ballot) < 0) { // stop ballot < other ballot
						// convert stop (psac1) to noop (psac1)
						PValuePacket noopPValue = this.makeNoopPValue(psac1.pValuePacket.slot);
						psac1 = new ProposalStateAtCoordinator(members, noopPValue);
						modified.add(psac1); // could "continue" here as psac1 is not stop anymore
					}
					else {
						assert(false) : "YIKES! Some coordinator proposed a regular request after a stop";
					}
				}
			}
		}
		for(ProposalStateAtCoordinator psac : modified) this.myProposals.put(psac.pValuePacket.slot, psac);
		if(stopExists && !this.myProposals.get(this.nextProposalSlotNumber-1).pValuePacket.isStopRequest()) {
			this.propose(members, new RequestPacket(0, STOP, true));
		}
	}

	/* Phase1b
	 * The second last step of Phase1b. The caller invokes this method after
	 * the above method to spawn commanders for all updated local proposals.
	 * 
	 * Return: The list of AcceptPackets that the caller will actually 
	 * send out in order to propose these proposals.
	 * 
	 * The view change is complete when the caller invokes setCoordinatorActive
	 * after this method.
	 */
	protected synchronized ArrayList<AcceptPacket> spawnCommandersForProposals() {
		if(this.active) return null; // ensures called only once

		ArrayList<AcceptPacket> acceptPacketList=null;
		for(ProposalStateAtCoordinator pstate : this.myProposals.values()) {
			if(acceptPacketList==null) acceptPacketList = new ArrayList<AcceptPacket>();
			acceptPacketList.add(this.initCommander(pstate.waitfor.getMembers(), pstate.pValuePacket));
		}

		this.active=true; // ensures called only once as active is never set to false
		return acceptPacketList;
	}
	/* Phase1b
	 * Phase1b is complete. I am active. I will remain active until
	 * I am destroyed and my ballot is forgotten forever.
	 */
	protected synchronized void setCoordinatorActive() {
		this.active = true;
		/* The two structures below have no more use. They
		 * hardly take up any space, especially coz the latter
		 * is a NullIfEmptyMap, but why bother to even keep that.
		 * Plus it serves as an implicit assert(false) if any
		 * code tries to access these structures here onwards.
		 */
		this.waitforMyBallot = null;
		this.carryoverProposals = null;
	}
	/*********************** End of Phase1b methods ************************/
		
	/*********************** Start of Phase2b methods ************************/
	/* Phase2b
	 * Event: Received an accept reply (phase2b) for a proposal in the current ballot. 
	 * Action: Update waitfor to check for a majority.
	 * Return: the proposal if a majority of accept replies have been received.
	 */
	protected synchronized PValuePacket handleAcceptReplyMyBallot(int[] members, AcceptReplyPacket acceptReply) {
		assert((acceptReply.ballot.compareTo(new Ballot(this.myBallotNum, this.myBallotCoord)) == 0));
		assert(!this.myProposals.containsKey(acceptReply.slotNumber) ||  
				this.myProposals.get(acceptReply.slotNumber).waitfor.contains(acceptReply.nodeID));
		// Current ballot, makes sense to process now
		recordSlotNumber(members, acceptReply); // useful for garbage collection

		boolean acceptedByMajority=false;
		ProposalStateAtCoordinator pstate = this.myProposals.get(acceptReply.slotNumber);
		WaitForUtility waitfor=null;
		PValuePacket decision=null;
		/* If I proposed it and it is not yet preempted or committed, then 
		 * I must have some waitfor state for it.
		 */
		if(pstate!=null && ((waitfor=pstate.waitfor) != null)) { 
			waitfor.updateHeardFrom(acceptReply.nodeID);
			if(DEBUG) log.finest("Updated waitfor to: " + waitfor);
			if(waitfor.heardFromMajority()) {
				// phase2b success
				acceptedByMajority=true;
				decision = pstate.pValuePacket.getDecisionPacket(getMajorityCommittedSlot());
				this.myProposals.remove(acceptReply.slotNumber);
			} 
		}
		return (acceptedByMajority ? decision : null);
	}

	/* Phase2b
	 * Event: Received an accept reply (phase2b) for a higher ballot.
	 * Action: Relinquish this proposal. 
	 * 
	 * Return: true if it's time to resign, i.e., no more outstanding 
	 * proposals are left, else false. 
	 * 
	 * Technically, the coordinator could also simply resign when 
	 * any higher ballot than myBallot for any proposal is observed, 
	 * as the rival coordinator(s) pushing the higher ballot or beyond 
	 * will do the job. But, in general, receiving an accept reply 
	 * with higherBallot for (myBallot, slot_x, proposal_x), where 
	 * higherBallot > myBallot, does not imply that this coordinator can 
	 * not successfully complete (myBallot, slot_y, proposal_y) for 
	 * some other slot y (including y>x). Indeed, it might get a majority 
	 * of accept replies anyway even for y=x. But we choose to relinquish 
	 * this specific slot x, and resign overall as coordinator if there 
	 * are no outstanding proposals left, when a higher ballot in an 
	 * accept reply for x is received.
	 */
	protected synchronized PValuePacket handleAcceptReplyHigherBallot(AcceptReplyPacket acceptReply) {
		assert((acceptReply.ballot.compareTo(new Ballot(this.myBallotNum, this.myBallotCoord)) > 0));
		assert(!this.myProposals.containsKey(acceptReply.slotNumber) ||  
				this.myProposals.get(acceptReply.slotNumber).waitfor.contains(acceptReply.nodeID));
		// Stop coordinating this specific proposal.
		ProposalStateAtCoordinator psac = this.myProposals.remove(acceptReply.slotNumber);
		PValuePacket preempted= (psac!=null ? psac.pValuePacket.preempt() : null);
		return preempted;
	}
	protected synchronized boolean preemptedFully() {
		// If no outstanding proposals and received higher ballot, resign as coordinator.
		if(this.myProposals.isEmpty()) return true;
		return false;
	}
	/*********************** End of Phase2b methods ************************/

	protected synchronized Ballot getBallot() {
		return new Ballot(this.myBallotNum, this.myBallotCoord);
	}
	protected synchronized boolean isActive() {
		return this.active;
	}
	protected synchronized boolean isOverloaded(int acceptorSlot) {
		return this.myProposals.size() > PaxosInstanceStateMachine.MAX_OUTSTANDING_LOAD || 
				this.nextProposalSlotNumber - acceptorSlot > PaxosInstanceStateMachine.MAX_OUTSTANDING_LOAD;
	}
	protected synchronized boolean waitingTooLong() {
		return (!this.isActive() && this.waitforMyBallot.totalWaitTime() > MAX_ELECTION_WAIT_TIME) ?
				true : false;
	}
	protected synchronized boolean waitingTooLong(int slot) {
		return (this.isActive() && this.myProposals.containsKey(slot) && 
				this.myProposals.get(slot).waitfor.totalWaitTime() > MAX_ELECTION_WAIT_TIME) ?
			true : false;
	}
	protected synchronized boolean isCommandering(int slot) {
		return (this.isActive() && this.myProposals.containsKey(slot)) ? true : false;
	}
	protected synchronized AcceptPacket reInitCommander(int slot) {
		return this.isCommandering(slot) ? this.initCommander(this.myProposals.get(slot)) : null;
	}

	/* This method returns proposals received by this coordinator before it
	 * actually became active. If preempted, it is nice to pass them over
	 * to the next coordinator. Not necessary for safety.
	 */
	protected synchronized ArrayList<ProposalPacket> getPreActiveProposals() {
		Collection<ProposalStateAtCoordinator> preActiveSet =  this.myProposals.values();
		ArrayList<ProposalPacket> preActiveProposals = null;
		for(ProposalStateAtCoordinator psac : preActiveSet) {
			if(preActiveProposals==null) preActiveProposals = new ArrayList<ProposalPacket>();
			preActiveProposals.add(psac.pValuePacket);
		}
		return preActiveProposals;
	}
	/****************************** Start of non-public/protected methods ******************************/

	/* Record the acceptor GC slot number specified in the prepare reply. Acceptors remove
	 * accepted proposals at or below the GC slot number, i.e., the maximum slot number that
	 * has been cumulatively committed by a majority of replicas.
	 */
	protected synchronized void recordSlotNumber(int[] members, PrepareReplyPacket preply) {
		assert(this.nodeSlotNumbers!=null);
		for(int i=0; i<members.length; i++) {
			if(members[i] == preply.receiverID) {
				this.nodeSlotNumbers[i] = preply.acceptorGCSlot+1; // starting slot is GCSlot+1
			}
		}
	}
	/* Record the cumulative committed slot number in the accept reply. After becoming active,
	 * the coordinator will disseminate the slot number cumulatively committed by a majority 
	 * of acceptors.
	 */
	protected synchronized void recordSlotNumber(int[] members, AcceptReplyPacket acceptReply) {
		assert(this.nodeSlotNumbers!=null);
		for(int i=0; i<members.length; i++) {
			if(members[i] == acceptReply.nodeID) {
				this.nodeSlotNumbers[i] = acceptReply.committedSlot;
			}
		}
	}

	private synchronized AcceptPacket initCommander(int[] members, PValuePacket pvalue) {
		return this.initCommander(new ProposalStateAtCoordinator(members, pvalue));
	}
	private synchronized AcceptPacket initCommander(ProposalStateAtCoordinator pstate) {
		AcceptPacket acceptPacket = new AcceptPacket(this.myBallotCoord, 
				pstate.pValuePacket, getMajorityCommittedSlot()); 
		if(DEBUG) log.info("Node " + this.myBallotCoord + " initCommandering " + acceptPacket);
		return acceptPacket;
	}

	
	/********************* Start of stand-alone'ish utility methods *************************
	 * These private methods are not synchronized even though some of them take synchronization 
	 * requiring structures as arguments because the callers above are *all* synchronized.
	 */
	private int getMajorityCommittedSlot() {
		return this.getMedianMinus(this.nodeSlotNumbers);
	}
	/* Gets the median (just before median) value with odd (even) number of members */
	private int getMedianMinus(int[] array) {
		int[] copy = new int[this.nodeSlotNumbers.length];
		System.arraycopy(this.nodeSlotNumbers, 0, copy, 0, this.nodeSlotNumbers.length);
		Arrays.sort(copy);
		int medianMinus = copy.length%2==0 ? copy.length/2-1 : copy.length/2;
		return copy[medianMinus];
	}
	private boolean noGaps(int x, int y, NullIfEmptyMap<Integer,ProposalStateAtCoordinator> map) {
		for(int i=x; i<y; i++) {
			if(map.get(i) == null) return false;
		}
		return true;
	}

	private PValuePacket makeNoopPValue(int curSlot) {
		ProposalPacket proposalPacket = new ProposalPacket(curSlot,
				new RequestPacket(0, NO_OP, false));
		PValuePacket pvalue = new PValuePacket(new Ballot(this.myBallotNum, this.myBallotCoord), proposalPacket);
		return pvalue;
	}
	private int getMaxPValueSlot(NullIfEmptyMap<Integer, PValuePacket> pvalues) {
		int maxSlot = -1; // this is maximum slot for which some request has been found that is in progess
		for (int cur: pvalues.keySet()) {
			if (cur > maxSlot) maxSlot = cur;
		}
		return maxSlot;
	}
	/* Before a coordinator becomes active, max(nodeSlotNumbers) is the
	 * maximum of the minimum slot numbers for which replicas sent
	 * carryover proposals.
	 */
	private int getMaxMinCarryoverSlot() {
		int maxSlot=Integer.MIN_VALUE;
		for(int i=0; i<this.nodeSlotNumbers.length; i++) {
			if(this.nodeSlotNumbers[i] > maxSlot) maxSlot = this.nodeSlotNumbers[i];
		}
		return maxSlot;
	}

	/********************* End of stand-alone utility methods *************************/


	/********************** Start of testing methods ****************************/
	public String toString() {
		return "[ballot="+this.myBallotNum+":"+this.myBallotCoord+", nextProposalSlotNumber="+this.nextProposalSlotNumber +
				", active="+this.isActive()+"]";
	}
	public String printState() {
		String s="carryoverProposals = " + (this.carryoverProposals.isEmpty() ? "[]" : "") + "\n";
		for(PValuePacket pvalue : this.carryoverProposals.values()) {
			s += pvalue + "\n";
		}
		s+=printMyProposals();
		s+="nodeSlotNumbers=[ ";
		for(int i=0; i<this.nodeSlotNumbers.length; i++) s+=this.nodeSlotNumbers[i]+" ";
		s+=" ]";
		return s;
	}
	public String printMyProposals() {
		String s="myProposals = \n";
		for(ProposalStateAtCoordinator psac : this.myProposals.values()) {
			s += psac.pValuePacket + "\n";
		}

		return s;
	}

	protected void testingInitCoord(int load) {
		//this.testingInitInstance(load);
		this.myProposals = new NullIfEmptyMap<Integer,ProposalStateAtCoordinator>();
		int[] group = {21, 32, 32, 91, 14};
		for(int i=0; i<load; i++) {
			this.myProposals.put(25+i, 
					new ProposalStateAtCoordinator(group, new PValuePacket(new Ballot(this.myBallotNum, this.myBallotCoord), 
							new ProposalPacket(45+i,
									new RequestPacket(34+i, "hello39"+i,false))
							))
					);
		}
	}
	
	private PValuePacket createCarryover(RequestPacket req, int slot, int ballot, int coord) {
		ProposalPacket prop = new ProposalPacket(slot, req);
		PValuePacket pvalue = new PValuePacket(new Ballot(ballot, coord), prop);
		return pvalue;
	}
	protected synchronized boolean isPresent(int slot) {
		return this.myProposals.containsKey(slot) || !this.isActive();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int myID = 21;
		int ballotnum=2;
		int numMembers = 43;
		int[] members = new int[numMembers];
		members[0] = myID;
		for(int i=1; i<members.length; i++) {
			members[i] = members[i-1] + 1 + (int)(Math.random()*10);
		}
		for(int i=0; i<members.length-1; i++) assert(members[i] < members[i+1]);
		
		PaxosCoordinatorState pcs = new PaxosCoordinatorState(ballotnum,myID, 0, members);
		System.out.println("Created PaxosCoordinatorState");
		int numReqs=100;
		RequestPacket[] reqs = new RequestPacket[numReqs];
		
		for(int i=0; i<numReqs; i++) {
			reqs[i] = new RequestPacket(23+i, "req"+i, false);
		}
		RequestPacket stop = new RequestPacket(1234, "STOP_REQUEST", true);
		
		// Check propose
		assert(pcs.propose(members, reqs[0])==null && !pcs.isActive() && !pcs.myProposals.isEmpty()); 
		assert(pcs.propose(members, stop)==null && !pcs.isActive() && !pcs.myProposals.isEmpty()); 
		assert(pcs.propose(members, reqs[9])==null && !pcs.isActive() && !pcs.myProposals.isEmpty()); 
		
		// Check preactives
		boolean inserted=false;
		ArrayList<ProposalPacket> preactives = pcs.getPreActiveProposals();
		for(ProposalPacket prop : preactives) if(prop.requestID==reqs[0].requestID) inserted = true; 
		assert(inserted);

		// ignore preplies before prepare
		PrepareReplyPacket preply = new PrepareReplyPacket(myID, 10, new Ballot(29, 42), null, 0);
		assert(pcs.waitforMyBallot==null);
		assert(pcs.canIgnorePrepareReply(preply));

		// Check prepare ballot 
		assert(pcs.waitforMyBallot==null);
		Ballot ballot = pcs.prepare(members);
		assert(pcs.waitforMyBallot!=null);
		assert(ballot.ballotNumber==ballotnum && ballot.coordinatorID==myID);
		
		// ignore lower ballot preplies
		preply = new PrepareReplyPacket(myID, 10, new Ballot(ballot.ballotNumber-1, ballot.coordinatorID), null, 0);
		assert(pcs.canIgnorePrepareReply(preply));
		preply = new PrepareReplyPacket(myID, 10, new Ballot(ballot.ballotNumber, ballot.coordinatorID-1), null, 0);
		assert(pcs.canIgnorePrepareReply(preply));

		// not active until enough correct prepare replies are received.
		assert(!pcs.isActive());
		
		Ballot preplyBallot = new Ballot(ballot.ballotNumber, ballot.coordinatorID);
		preply = new PrepareReplyPacket(myID, 10, preplyBallot, null, 0);
		assert(pcs.canIgnorePrepareReply(preply)); // coz 10 is not in members

		int numPValues = 100;
		PValuePacket[] pvalues = new PValuePacket[numPValues];
		
		System.out.println(pcs.printState());
		int maxMinSlot=2;

		// preply by members[2]
		pvalues[0] = pcs.createCarryover(reqs[0], 2, ballot.ballotNumber-1, ballot.coordinatorID-1);
		HashMap<Integer,PValuePacket> accepted = new HashMap<Integer,PValuePacket>(); 
		accepted.put(0, pvalues[0]);
		System.out.println("Combining " + accepted);

		preply = new PrepareReplyPacket(myID, members[2], preplyBallot, accepted, maxMinSlot-1);
		assert(!pcs.isPrepareAcceptedByMajority(preply, members) && pcs.waitforMyBallot.alreadyHeardFrom(members[2])); // finally a legitimate reply
		assert(pcs.canIgnorePrepareReply(preply)); // no duplicates
		System.out.println(pcs.printState());

		// preply by members[0]
		pvalues[1] = pcs.createCarryover(reqs[1], 2, ballot.ballotNumber-1, ballot.coordinatorID);
		pvalues[2] = pcs.createCarryover(reqs[2], 6, ballot.ballotNumber-1, ballot.coordinatorID);
		accepted.put(pvalues[1].slot, pvalues[1]);
		accepted.put(pvalues[2].slot, pvalues[2]);
		assert(pvalues[0].ballot.compareTo(pvalues[1].ballot) < 0);
		System.out.println("Combining " + accepted);

		preply = new PrepareReplyPacket(myID, members[0], preplyBallot, accepted, maxMinSlot-1);
		assert(!pcs.isPrepareAcceptedByMajority(preply, members) && pcs.canIgnorePrepareReply(preply)); // legitimate self-reply followed by duplicate
		
		// prepare by members[4] including stop
		reqs[3] = new RequestPacket(reqs[3].clientID, reqs[3].requestValue, true);
		pvalues[3] = pcs.createCarryover(reqs[3], 7, ballot.ballotNumber-1, ballot.coordinatorID);
		pvalues[4] = pcs.createCarryover(reqs[4], 8, ballot.ballotNumber-1, ballot.coordinatorID+1);
		pvalues[5] = pcs.createCarryover(reqs[5], 9, ballot.ballotNumber-1, ballot.coordinatorID-1);
		accepted.clear();
		accepted.put(pvalues[3].slot, pvalues[3]);
		accepted.put(pvalues[4].slot, pvalues[4]);
		accepted.put(pvalues[5].slot, pvalues[5]);
		preply = new PrepareReplyPacket(myID, members[4], preplyBallot, accepted, 1);
		assert(!pcs.isPrepareAcceptedByMajority(preply, members) && pcs.canIgnorePrepareReply(preply)); 

		for(int i=0; i<members.length; i+=2) {
			preply = new PrepareReplyPacket(myID, members[i], preplyBallot, null, maxMinSlot-1);
			assert(!pcs.isPrepareAcceptedByMajority(preply, members) || i==members.length-1);
		}
		assert(pcs.waitforMyBallot.heardFromMajority());
		assert(!pcs.myProposals.isEmpty());
		assert(!pcs.carryoverProposals.isEmpty());
		System.out.println(pcs.printState());

		pcs.combinePValuesOntoProposals(members);
		System.out.println(pcs.printState());
		
		boolean stopped=false;
		Set<Integer> slots = pcs.myProposals.keySet();
		Integer[] sorted = new Integer[slots.size()];
		slots.toArray(sorted);
		
		for(int i=0; i<sorted.length; i++) {
			if(i<maxMinSlot) {assert(!pcs.myProposals.containsKey(i)); continue;}
			if(stopped) assert(pcs.myProposals.get(i).pValuePacket.isStopRequest());
			if(pcs.myProposals.get(i).pValuePacket.isStopRequest()) stopped=true;
		}

		pcs.spawnCommandersForProposals();
		pcs.setCoordinatorActive();
		assert(pcs.carryoverProposals==null);
		assert(pcs.waitforMyBallot==null);

		System.out.println("Testing accept replies");
		AcceptReplyPacket[] areplies = new AcceptReplyPacket[members.length];
		for(int i : new ArrayList<Integer>(pcs.myProposals.keySet())) {
			PValuePacket pvalue = pcs.myProposals.get(i).pValuePacket;
			assert(pvalue!=null);
			assert(!pcs.preemptedFully());
			for(int j=0; j<members.length; j++) {
				PValuePacket decision=null;
				if(Math.random()>0.99) { // Note: a single member's accept reply is enough to preempt
					Ballot b = new Ballot(pcs.myBallotNum+1, pcs.myBallotCoord);
					areplies[j] = new AcceptReplyPacket(members[j], b, pvalue.slot, -1);
					decision = pcs.handleAcceptReplyHigherBallot(areplies[j]);
					assert(decision==null || decision.getType()==PaxosPacketType.PREEMPTED);
				} else {
					areplies[j] = new AcceptReplyPacket(members[j], new Ballot(pcs.myBallotNum, pcs.myBallotCoord), pvalue.slot, -1);
					decision = pcs.handleAcceptReplyMyBallot(members, areplies[j]);
					assert(decision==null || decision.getType()==PaxosPacketType.DECISION);
				}
				if(decision!=null) {
					if(decision.getType()==PaxosPacketType.DECISION) {
						System.out.println("Accepted by majority decision: " + decision);
						assert(!pcs.myProposals.containsKey(decision.slot));
					}
					else if(decision.getType()==PaxosPacketType.PREEMPTED) {
						System.out.println("PREEMPTED proposal: " + decision);
						assert(!pcs.myProposals.containsKey(decision.slot));
					}

				}
			}
		}
		assert(pcs.myProposals.isEmpty());
		
		System.out.println("SUCCESS! TBD: Only minimally tested. Not stress tested under concurrency.");
	}
}
