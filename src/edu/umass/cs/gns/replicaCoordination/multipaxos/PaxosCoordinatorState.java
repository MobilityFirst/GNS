package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import edu.umass.cs.gns.packet.PaxosPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptReplyPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PreparePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.util.NullIfEmptyMap;

/* This class manages paxos coordinator state.
 * It has no public methods, only protected methods,
 * as it is used *only* by PaxosCoordinator.
 * 
 * An inactive PaxosCoordinatorState instance, i.e., when 
 * the corresponding node is a coordinator, but this paxos
 * instance is not processing any requests uses about 50B.
 * With this class, the *total* space that can be attributed
 * to an inactive PaxosInstanceStateMachine is about 225B.
 */
/**
@author V. Arun
 */
public class PaxosCoordinatorState  {

	private final Ballot myBallot; // final, takes birth and dies with this PaxosCoordinatorState

	// List of replicas who have accepted my ballot
	private WaitForUtility waitforMyBallot=null; // non-null only until the coordinator becomes active

	// List of proposals carried over from lower ballots to be re-proposed or committed in my ballot
	private NullIfEmptyMap<Integer,PValuePacket> carryoverProposals=new NullIfEmptyMap<Integer,PValuePacket>(); // non-null only until the coordinator becomes active

	// List of proposals I am currently trying to push in my ballot as coordinator (or when I get the election majority to be a coordinator)
	private NullIfEmptyMap<Integer, ProposalStateAtCoordinator> myProposals=new NullIfEmptyMap<Integer, ProposalStateAtCoordinator>();

	private int nextProposalSlotNumber = 0; // next free slot number to propose

	/* After this coordinator becomes active, it remains active until it no longer
	 * exists. That is, we never set active back to false again. Instead, this 
	 * coordinator state is expunged and new coordinator state (for a different 
	 * ballot) may get created in the future.
	 */
	private boolean active = false; 

	/* This field is really unrelated to paxos coordinator state. It is kept 
	 * here so that we can piggyback on coordinator back-and-forth messages
	 * to disseminate the maximum cumulatively executed slot at nodes, which 
	 * is useful for garbage collection. nodeSlotNumbers[i] is the slot number 
	 * up to which members[i] has executed all requests. Once all nodes have 
	 * executed a request with slot s, all state in the system for slots up 
	 * to s can be deleted. Note: we don't bother keeping track of members 
	 * here because that is final in the corresponding paxos instance and is 
	 * always passed as an argument when needed.
	 */
	private int[] nodeSlotNumbers=null;

	private static Logger log = Logger.getLogger(PaxosCoordinatorState.class.getName()); // GNS.getLogger();

	/* Coordinator state is born into the ballot myBallot. 
	 * myBallot can never change.
	 */
	PaxosCoordinatorState(Ballot b) { this.myBallot = b; }

	/* Phase1a
	 * Event: A call to try to become coordinator.
	 * Action: If I am not already a coordinator, initiate a waitfor
	 * to start receiving prepare replies. 
	 * 
	 * Return: The ballot. The caller will send out the ballot
	 * to all nodes.
	 */
	public synchronized Ballot prepare(int[] members) {
		if(this.active==true) return null; // I am already an active coordinator 
		this.waitforMyBallot = new WaitForUtility(members);
		return this.myBallot;
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
	public synchronized AcceptPacket propose(int[] members, RequestPacket request) {
		AcceptPacket acceptPacket=null;
		PValuePacket pvalue = new PValuePacket(this.myBallot, 
				new ProposalPacket(this.nextProposalSlotNumber++, request, PaxosPacketType.PROPOSAL));
		assert(!this.myProposals.containsKey(pvalue.proposal.slot));
		this.myProposals.put(pvalue.proposal.slot, new ProposalStateAtCoordinator(members,pvalue));
		log.info("Node " + this.myBallot.coordinatorID + " inserting proposal: " + pvalue);
		if(this.isActive()) {
			log.finest("Coordinator at node " + this.myBallot.coordinatorID + " is active");
			acceptPacket =  this.initCommander(members, pvalue);
		} else {
			log.info("Coordinator at node " + this.myBallot.coordinatorID + " is not active");
			/* Got to wait till view change is complete as proposals for a slot 
			 * can change in the process. Do nothing for now.
			 */
		}
		return acceptPacket;
	} 

	/*********************** Start of Phase1b methods ************************/

	/* Phase1b
	 * Event: Received a prepare reply for my ballot.
	 * Return: true if prepare reply should be ignored. A prepare 
	 * message is sent by a coordinator to initiate a ballot. The 
	 * coordinator waits for prepare replies containing its proposed
	 * ballot. Lower ballots can be ignored. Higher ballots mean that
	 * I should resign.
	 */
	public synchronized boolean canIgnorePrepareReply(PreparePacket prepareReply) {
		// I am not even trying to be a coordinator.
		if(this.waitforMyBallot==null) return true; 

		// The replica already answered my ballot request.
		else if(!this.waitforMyBallot.updateHeardFrom(prepareReply.receiverID)) return true;

		// This should never happen. Why did the replica bother to send me a lower ballot in the first place?
		else if(prepareReply.ballot.compareTo(this.myBallot) < 0) {assert(false);}

		return false;
	}

	/* Phase1b
	 * If I receive a prepare reply with a higher ballot, I am guaranteed
	 * to lose the election, so concede defeat already.
	 */
	public synchronized boolean isPreemptable(PreparePacket prepareReply) {
		boolean preempt=false;
		if(prepareReply.ballot.compareTo(this.myBallot) > 0) {
			preempt = true;
		}
		return preempt;
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
	public synchronized boolean isPrepareAcceptedByMajority(PreparePacket prepareReply) {
		if(this.canIgnorePrepareReply(prepareReply)) {log.info("Node " + this.myBallot.coordinatorID+ " ignoring prepare reply"); return false;}
		// isPreemptable and canIgnorePrepareReply should have been called already, hence the assert
		assert(prepareReply.ballot.compareTo(this.myBallot)==0) : "prepareReply.ballot = "+prepareReply.ballot + ", myBallot = "+this.myBallot;

		boolean acceptedByMajority = false;
		for(PValuePacket pvalue : prepareReply.accepted.values()) {
			int curSlot = pvalue.proposal.slot;
			/* The latter part of the disjunction below is because some replica(s) may have
			 * accepted a different proposal for curSlot in a higher ballot, so we should
			 * choose the proposal accepted for a slot in the highest ballot.
			 */
			PValuePacket existing = this.carryoverProposals.get(curSlot);
			if (existing==null || prepareReply.ballot.compareTo(existing.ballot) > 0)  {
				this.carryoverProposals.put(pvalue.proposal.slot, pvalue);
			}
		}
		waitforMyBallot.updateHeardFrom(prepareReply.receiverID);
		log.finest("Waitfor = " + waitforMyBallot.toString());
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
	public synchronized void combinePValuesOntoProposals(int[] members) {
		if(this.carryoverProposals.isEmpty()) return; // no need to process stop requests either
		
		int maxPValueSlot = getMaxPValueSlot(carryoverProposals);
		int maxCommittedSlot = getMaxSlotNumberAcrossNodes(); // we are sure that all slots have been filled

		/* Combine carryoverProposals with myProposals prioritizing the former
		 * and selecting no-ops for slots for which neither contain a value.
		 */
		for (int curSlot = maxCommittedSlot; curSlot <= maxPValueSlot; curSlot++) {
			if(this.carryoverProposals.containsKey(curSlot)) { // received pvalues dominate local proposals
				this.myProposals.put(curSlot, 
						new ProposalStateAtCoordinator(members, this.carryoverProposals.get(curSlot)));
			} else if(!this.myProposals.containsKey(curSlot)) { // no-ops if neither received nor local proposal
				this.myProposals.put(curSlot, 
						new ProposalStateAtCoordinator(members, makeNoopPValue(curSlot)));
			}
		}
		/* The next free slot number to use for proposals should be the maximum of 
		 * the maximum slot for which some request has been (cumulatively) 
		 * executed by some node, the maximum slot in received pvalues, and the
		 *  nextProposalSlotNumber that reflects the highest locally proposed
		 *  slot number. 
		 */
		updateNextProposalSlotNumber(Math.max(Math.max(maxPValueSlot+1, maxCommittedSlot), 
				this.nextProposalSlotNumber));
		assert(noGaps(maxCommittedSlot, this.nextProposalSlotNumber, this.myProposals)); // gaps means state machine will be stuck

		processStop(members); // need to ensure that a regular request does not follow a stop
	}
	
	/* Phase1b
	 * Utility method to handle stop requests correctly after combining pvalues 
	 * on to proposals. The goal is to make sure that there is no request after
	 * a stop request. If there is a request after a stop in a higher ballot, 
	 * the stop has to be turned into a noop. If there is a request after a 
	 * stop in a lower ballot, the request should be turned into a stop.
	 */
	public synchronized void processStop(int[] members) {
		for(ProposalStateAtCoordinator psac1 : this.myProposals.values()) {
			for(ProposalStateAtCoordinator psac2 : this.myProposals.values()) {
				if(psac1.pValuePacket.proposal.req.isStopRequest() && // 1 is stop
						!psac2.pValuePacket.proposal.req.isStopRequest() && // other is not stop
						!psac2.pValuePacket.proposal.req.value.equals(PaxosInstanceStateMachine.NO_OP) && // other is not noop
						psac1.pValuePacket.proposal.slot < psac2.pValuePacket.proposal.slot)  // other has a higher slot
				{
					if(psac1.pValuePacket.ballot.compareTo(psac2.pValuePacket.ballot) > 0) { // stop ballot > other ballot
						// convert request to stop
						RequestPacket req = psac2.pValuePacket.proposal.req;
						RequestPacket stopReq = new RequestPacket(req.clientID, req.value, req.getType(), true);
						PValuePacket stopPValue = psac2.pValuePacket;
						stopPValue.ballot = this.myBallot;
						stopPValue.proposal = new ProposalPacket(stopPValue.proposal.slot, stopReq, PaxosPacket.PROPOSAL);
						ProposalStateAtCoordinator stopProposal = new ProposalStateAtCoordinator(members, stopPValue);
						this.myProposals.put(stopPValue.proposal.slot, stopProposal);
					}
					else if(psac1.pValuePacket.ballot.compareTo(psac2.pValuePacket.ballot) < 0) { // stop ballot < other ballot
						// convert stop to noop
						PValuePacket noopPValue = this.makeNoopPValue(psac2.pValuePacket.proposal.slot);
						ProposalStateAtCoordinator noopProposal = new ProposalStateAtCoordinator(members, noopPValue);
						this.myProposals.put(noopPValue.proposal.slot, noopProposal);
					}
					else {
						assert(false) : "Some coordinator proposed a regular request after a slot";
					}
				}
			}
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
	public synchronized ArrayList<AcceptPacket> spawnCommandersForProposals() {
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
	public void setCoordinatorActive() {
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
	public synchronized PValuePacket handleAcceptReplyMyBallot(AcceptReplyPacket acceptReply) {
		assert((acceptReply.ballot.compareTo(this.myBallot) == 0));
		// Current ballot, makes sense to process now

		boolean acceptedByMajority=false;
		ProposalStateAtCoordinator pstate = this.myProposals.get(acceptReply.slotNumber);
		WaitForUtility waitfor=null;
		/* If I proposed it and it is not yet preempted or committed, then 
		 * I must have some waitfor state for it.
		 */
		if(pstate!=null && ((waitfor=pstate.waitfor) != null)) { 
			waitfor.updateHeardFrom(acceptReply.nodeID);
			log.finer("Updated waitfor to: " + waitfor);
			if(waitfor.heardFromMajority()) {
				// phase2b success
				acceptedByMajority=true;
				this.myProposals.remove(acceptReply.slotNumber);
				pstate.pValuePacket.setType(PaxosPacket.DECISION);
			} 
		}
		return (acceptedByMajority ? pstate.pValuePacket : null);
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
	public synchronized boolean handleAcceptReplyHigherBallot(AcceptReplyPacket acceptReply) {
		assert((acceptReply.ballot.compareTo(this.myBallot) > 0));

		boolean resignAsCoordinator=false; 
		// Stop coordinating this specific proposal.
		this.myProposals.remove(acceptReply.slotNumber);

		// If no outstanding proposals and received higher ballot, resign as coordinator.
		if(this.myProposals.isEmpty()) resignAsCoordinator=true;
		return resignAsCoordinator;
	}
	/*********************** End of Phase2b methods ************************/

	public synchronized Ballot getBallot() {
		return this.myBallot;
	}
	public synchronized boolean isActive() {
		return this.active;
	}
	/* This method returns proposals received by this coordinator before it
	 * actually became active. If preempted, it is nice to pass them over
	 * to the next coordinator. Not necessary for safety.
	 */
	public synchronized ArrayList<ProposalPacket> getPreActiveProposals() {
		Collection<ProposalStateAtCoordinator> preActiveSet =  this.myProposals.values();
		ArrayList<ProposalPacket> preActiveProposals = null;
		for(ProposalStateAtCoordinator psac : preActiveSet) {
			if(preActiveProposals==null) preActiveProposals = new ArrayList<ProposalPacket>();
			preActiveProposals.add(psac.pValuePacket.proposal);
		}
		return preActiveProposals;
	}
	/****************************** Start of non-public methods ******************************/

	/* Record the slot number specified in prepare reply. For garbage collection. */
	protected synchronized void recordSlotNumber(int[] groupMembers, PreparePacket pp) {
		if(this.nodeSlotNumbers==null) this.nodeSlotNumbers = new int[groupMembers.length];
		for(int i=0; i<groupMembers.length; i++) {
			if(groupMembers[i] == pp.receiverID) {
				this.nodeSlotNumbers[i] = pp.slotNumber;
			}
		}
	}
	private synchronized AcceptPacket initCommander(int[] members, PValuePacket pvalue) {
		return this.initCommander(new ProposalStateAtCoordinator(members, pvalue));
	}
	private synchronized AcceptPacket initCommander(ProposalStateAtCoordinator pstate) {
		//this.myProposals.put(pstate.pValuePacket.proposal.slot, pstate); // unnecessary, remove?
		log.info("Node " + this.myBallot.coordinatorID + " initCommandering " + pstate.pValuePacket);
		AcceptPacket acceptPacket = new AcceptPacket(this.myBallot.coordinatorID, 
				pstate.pValuePacket, PaxosPacketType.ACCEPT, 0); //FIXME: slotNumberAtReplica != 0 in general
		return acceptPacket;
	}
	private synchronized void updateNextProposalSlotNumber(int s) {
		this.nextProposalSlotNumber = s;
	}
	private synchronized int getMaxSlotNumberAcrossNodes() {
		int maxSlot=-1;
		for(int i=0; i<this.nodeSlotNumbers.length; i++) {
			if(this.nodeSlotNumbers[i] > maxSlot) maxSlot = this.nodeSlotNumbers[i];
		}
		return maxSlot;
	}
	
	/********************* Start of stand-alone'ish utility methods *************************
	 * These private methods are not synchronized even though some of them take synchronization 
	 * requiring structures as arguments because the callers above are *all* synchronized.
	 */
	private boolean noGaps(int x, int y, NullIfEmptyMap<Integer,ProposalStateAtCoordinator> map) {
		for(int i=x; i<y; i++) {
			if(map.get(i) == null) return false;
		}
		return true;
	}

	private PValuePacket makeNoopPValue(int curSlot) {
		ProposalPacket proposalPacket = new ProposalPacket(curSlot,
				new RequestPacket(0, PaxosInstanceStateMachine.NO_OP, PaxosPacketType.REQUEST, false),
				PaxosPacketType.PROPOSAL, 0);
		PValuePacket pvalue = new PValuePacket(this.myBallot, proposalPacket);
		return pvalue;
	}
	private int getMaxPValueSlot(NullIfEmptyMap<Integer, PValuePacket> pvalues) {
		int maxSlot = -1; // this is maximum slot for which some request has been found that is in progess
		for (int cur: pvalues.keySet()) {
			if (cur > maxSlot) maxSlot = cur;
		}
		return maxSlot;
	}
	/********************* End of stand-alone utility methods *************************/


	/********************** Start of testing methods ****************************/
	protected void testingInitCoord(int load) {
		//this.testingInitInstance(load);
		this.myProposals = new NullIfEmptyMap<Integer,ProposalStateAtCoordinator>();
		int[] group = {21, 32, 32, 91, 14};
		for(int i=0; i<load; i++) {
			this.myProposals.put(25+i, 
					new ProposalStateAtCoordinator(group, new PValuePacket(this.myBallot, 
							new ProposalPacket(45+i,
									new RequestPacket(34+i, "hello39"+i, PaxosPacketType.REQUEST,false),
									PaxosPacketType.PROPOSAL)
							))
					);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("FAILURE: I am not tested, so I am useless and should drown myself in a puddle. Try running PaxosManager's test for now.");
	}

}
