package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.packet.PaxosPacket;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptReplyPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PreparePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.SynchronizePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.SynchronizeReplyPacket;

/**
@author V. Arun
 */

/* This class is the top-level paxos class per application. This 
 * is the only class that exposes public methods. Actually even this 
 * class will soon become "protected" as the only way to use it will be 
 * through the corresponding PaxosManager even if there is just one 
 * paxos group being used by an application.
 * 
 * This class delegates much of the interesting paxos actions to 
 * PaxosAcceptorState and PaxosCoordinator. It delegates all messaging
 * to PaxosManager's PaxosMessenger. It is "managed", i.e., its paxos
 * group is created and its incoming packets are demultiplexed, by
 * its PaxosManager. It's logging (not yet implemented) will be handled
 * by PaxosManager's logging agent.
 * 
 * The high-level organization is best reflected in handlePaxosMessage, 
 * a method that delegates processing to the acceptor or coordinator
 * and gets back a messaging task, e.g., receiving a prepare message
 * will probably result in a prepare-reply messaging task, and so on.
 * 
 * Space: An inactive PaxosInstanceStateMachine, i.e., whose corresponding
 * application is currently not processing any requests, uses ~225B *total*. 
 * Here is the breakdown:
 * PaxosInstanceStateMachine final fields: ~70B
 * PaxosAcceptor: ~100B
 * PaxosCoordinatorState: ~50B 
 * Even in an inactive paxos instance, the total *total* space is much more 
 * because of PaxosManager (that internally uses FailureDetection) etc.,
 * but all that state is not incurred per paxos application, just per 
 * machine. Thus, if we have S=10 machines and N=10M applications each 
 * using paxos with K=10 replicas one each at each machine, each machine
 * has 10M PaxosInstanceStateMachine instances that will use about 
 * 2.25GB (10M*225B). The amount of space used by PaxosManager and others
 * is small and depends only on S, not N or K.
 * 
 * When actively processing requests, the total space per paxos instance
 * can easily go up to thousands of bytes. But we are unlikely to be
 * processing requests across even hundreds of thousands of different 
 * applications simultaneously if each request finishes executing in 
 * under a second. For example, if a single server's execution
 * throughput is 10K requests/sec and each request takes 100ms to
 * finish executing (including paxos coordination), then the number 
 * of active *requests* at a machine is on average ~100K. The 
 * number of active paxos instances at that machine is at most 
 * the number of active requests at that machine.
 */
public class PaxosInstanceStateMachine {
	public static final String NO_OP = "NO_OP";
	private static int TEST_FAILED_NODE_ID = 100; // Used only during testing

	/************ Paxos state that is unchangeable (final) after creation ***************/

	/* List of node IDs that belong to this paxos group. 
	 * Declared final because this group does not change
	 * once created. Group change operations involve 
	 * creating an entirely new instance of PaxosInstanceStateMachine.
	 */
	private final int[] groupMembers; 
	
	/* Same as the name of the object (GUID) for which this paxos
	 * instance is created. The paxosID remains the same even across 
	 * group changes, so it is definitely final.
	 */
	private final String paxosID; // Can change to byte[] for compactness
	private final int myID;
	private final PaxosManager paxosManager;
	private final PaxosInterface clientRequestHandler;

	/************ All paxos state that is changeable after creation ****************/
	PaxosAcceptorState paxosState=null;	// uses ~125B of empty space when not actively processing requests
	PaxosCoordinator coordinator=null; // uses just a single pointer's worth of space unless I am a coordinator
	
	// static, so does not count towards space.
	private static Logger log = Logger.getLogger(PaxosInstanceStateMachine.class.getName()); // GNS.getLogger();

	public PaxosInstanceStateMachine(String groupId, int id, Set<Integer> gms, PaxosInterface app, PaxosManager pm) {

		/**************** final assignments ***********************
		 * A paxos instance is born with a paxosID,
		 * this instance's node ID, the application request handler, 
		 * the paxos manager, and the group members.
		 */
		this.paxosID = groupId;
		this.myID = id;
		this.clientRequestHandler = app;
		this.paxosManager = pm;

		// Copy set gms to array groupMembers
		assert(gms.size()>0);
		this.groupMembers = new int[gms.size()];
		int index=0;
		for(int i : gms) this.groupMembers[index++] = i; 
		Arrays.sort(this.groupMembers); 
		/**************** End of final assignments *******************/

		/* All non-final state is store in PaxosInstanceState (for acceptors)
		 * or in PaxosCoordinatorState (for coordinators) that inherits from
		 * PaxosInstanceState.
		 */
		this.coordinator = new PaxosCoordinator(); // just a shell class to wrap coordinator state
		Ballot initBallot = new Ballot(0, getNextCoordinator(0, groupMembers)); 
		if(initBallot.coordinatorID==this.myID) coordinator.makeCoordinator(initBallot, groupMembers);
		/* Note: Initial coordinator status is currently assumed and not explicitly prepared.
		 * If wrong, say because replica 0 is recovering, safety will not be affected. */
		
		this.paxosState = new PaxosAcceptorState(initBallot);
		
		//log.fine("Initializating paxos " + (initBallot.coordinatorID==this.myID ? "COORDINATOR":"instance") + " for " + paxosID + " at node " + myID + " with a total of " + groupMembers.length + " members.");
	}

	public void handlePaxosMessage(JSONObject msg) throws JSONException {
		int msgType = -1;
		msgType = msg.getInt(PaxosPacket.ptype);
		log.info("Node " + this.myID + " received " + PaxosPacket.typeToString[msg.getInt(PaxosPacket.ptype)] + ": " + msg);

		if(PaxosInstanceStateMachine.TEST_FAILED_NODE_ID == this.myID) return; // act like dead.
		
		MessagingTask rfcTask = checkRunForCoordinator(); // Check periodically, especially if we get any messages
		
		MessagingTask mtask=null;
		switch(msgType) {
		case -1: // Do nothing. 
			break;
		case PaxosPacket.REQUEST:
			mtask = handleRequest(new RequestPacket(msg));
			// send RequestPacket to current coordinator
			break;
			// replica --> coordinator
		case PaxosPacket.PROPOSAL:
			mtask = handleProposal(new ProposalPacket(msg));
			// either send ProposalPacket to current coordinator or send AcceptPacket to all
			break;
			// coordinator --> replica
		case  PaxosPacket.DECISION:
			handleCommittedRequest(new ProposalPacket(msg));
			// send nothing
			break;
			// coordinator --> replica
		case PaxosPacketType.PREPARE:
			mtask = handlePrepare(new PreparePacket(msg));
			// send PreparePacket prepare reply to coordinator
			break;
			// replica --> coordinator
		case PaxosPacket.PREPARE_REPLY:
			mtask = handlePrepareMessageReply(new PreparePacket(msg));
			// send AcceptPacket[] to all
			break;
			// coordinator --> replica
		case PaxosPacket.ACCEPT:
			mtask = handleAccept(new AcceptPacket(msg));
			// send AcceptReplyPacket to coordinator
			break;
			// replica --> coordinator
		case PaxosPacket.ACCEPT_REPLY:
			mtask = handleAcceptReply(new AcceptReplyPacket(msg));
			// send PValuePacket decision to all
			break;
			// replica --> replica
			// local failure detector --> replica
		case PaxosPacket.NODE_STATUS:
			handleNodeStatusUpdate(new FailureDetectionPacket(msg));
			break;
		case PaxosPacket.SYNC_REQUEST:
			mtask = handleSyncRequest(new SynchronizePacket(msg));
			// send SynchronizeReplyPacket to sender
			break;
		case PaxosPacket.SYNC_REPLY:
			mtask = handleSyncReplyPacket(new SynchronizeReplyPacket(msg));
			// send SynchronizeReplyPacket to sender
			break;
			
			/* TBD: Need to remove or handle the cases below */
		case PaxosPacket.RESEND_ACCEPT:
			break;
		case PaxosPacket.FAILURE_DETECT:
			break;
		case PaxosPacket.FAILURE_RESPONSE:
			break;
		case PaxosPacket.SEND_STATE:
			break;
		case PaxosPacket.SEND_STATE_NO_RESPONSE:
			break;
		case PaxosPacket.REQUEST_STATE:
			break;
		case PaxosPacket.START:
			break;
		case PaxosPacket.STOP:
			break;
		}


		try {
			sendMessagingTask(mtask);
			sendMessagingTask(rfcTask);
		} catch(IOException e) {
			e.printStackTrace();
			log.severe("IOException encountered in PaxosManager while sending " + mtask);
			/* FIXME: We can't throw this exception upward because it will get sent all
			 * the way back up to PacketDemultiplexer whose incoming packet initiated
			 * this whole chain of events. It seems silly for PacketDemultiplexer
			 * to throw an IOException caused by the sends resulting from processing
			 * that packet. So we should handle this exception right here.
			 * 
			 * But what should we do? We could ignore it as the network does not need
			 * to be reliable anyway. Revisit as needed.
			 */
		}
	}
	
	
	/************** Start of private methods ****************/

	/* The one method for all message sending. */
	private void sendMessagingTask(MessagingTask mtask) throws JSONException, IOException{
		if(mtask==null) return;
		log.info("Node " + this.myID + " sending response: " + mtask.toString());
		mtask.putPaxosID(this.paxosID);
		paxosManager.send(mtask);
		
	}

	/* "Phase0"
	 * Event: Received a request from a client.
	 * 
	 * Action: Call handleProposal which will send the corresponding
	 * proposal to the current coordinator.
	 */
	private MessagingTask handleRequest(RequestPacket msg) throws JSONException {
		log.info("Phase0/CLIENT_REQUEST: Node " + this.myID + " received request " + msg);
		return handleProposal(new ProposalPacket(0, msg, PaxosPacketType.PROPOSAL));
	}

	/* "Phase0"->Phase2a:
	 * Event: Received a proposal [request, slot] from any node.
	 * 
	 * Action: If a non-coordinator node receives a proposal, send to 
	 * the coordinator. Otherwise, propose it to acceptors with a good 
	 * slot number (thereby initiating phase2a for this request).
	 * 
	 * Return: A send either to a coordinator of the proposal or to all
	 * replicas of the proposal with a good slot number. 
	 */
	private MessagingTask handleProposal(ProposalPacket proposal) throws JSONException {
		MessagingTask mtask=null; // could be multicast or unicast to coordinator.
		AcceptPacket acceptPacket=null;
		if(this.coordinator.exists(this.paxosState.getBallot())) { // propose to all
			log.info("Phase2a/ACCEPT: Coordinator at " + this.myID + 
					" exists (maybe active or inactive), initiating accept phase for " + proposal);
			acceptPacket = this.coordinator.propose(this.groupMembers, proposal.req);
			if(acceptPacket!=null) mtask = new MessagingTask(this.groupMembers, acceptPacket); // multicast accept to all
		}
		else { // else send to current coordinator
			log.fine("I am not the coordinator, forwarding instead to node " + this.paxosState.getBallot().coordinatorID);
			mtask = new MessagingTask(this.paxosState.getBallot().coordinatorID, proposal.req); // unicast to coordinator
		}
		return mtask;
	}

	
	/* Phase1a
	 * Event: Received a prepare request for a ballot, i.e. that
	 * ballot's coordinator is acquiring proposing rights for all
	 * slot numbers (lowest uncommitted up to infinity)
	 * 
	 * Action: This node needs to check if it has accepted a higher
	 * numbered ballot already and if not, it can accept this
	 * ballot, thereby promising not to accept any lower ballots.
	 * 
	 * Return: Send prepare reply with proposal values previously
	 * accepted to the sender (the received ballot's coordinator).
	 */
	private MessagingTask handlePrepare(PreparePacket prepare) throws JSONException{
		prepare.receiverID = this.myID; // Coz the sent multicast prepare packet's receiver ID is meaningless
		PreparePacket prepareReply = this.paxosState.handlePrepare(prepare);
		/* Can resign here, but might lose requests forcing client retransmissions. */
		//this.coordinator.resignIfActiveCoordinator(this.paxosState.getBallot()); 
		MessagingTask mtask = new MessagingTask(prepareReply.coordinatorID, prepareReply);
		return mtask;
	}

	/* Phase1b
	 * Event: Received a reply to my ballot preparation request.
	 * 
	 * Action: If the reply contains a higher ballot, we must resign.
	 * Otherwise, if we acquired a majority with the receipt of this 
	 * reply, send all previously accepted (but uncommitted) requests
	 * reported in the prepare replies, each in its highest reported 
	 * ballot, to all replicas. These are the proposals that get 
	 * carried over across a ballot change and must be re-proposed.
	 * 
	 * Return: A list of messages each of which has to be multicast
	 * (proposed) to all replicas.
	 */
	private MessagingTask handlePrepareMessageReply(PreparePacket prepareReply) {
		MessagingTask mtask=null;
		
		ArrayList<ProposalPacket> preActiveProposals=null;
		ArrayList<AcceptPacket> acceptList = null;

		if((preActiveProposals = this.coordinator.getPreActivesIfPreempted(prepareReply, this.groupMembers))!=null) {
			log.info("Node " + this.myID + " coordinator election PREEMPTED by node " + prepareReply.ballot.coordinatorID);
			mtask = new MessagingTask(prepareReply.ballot.coordinatorID, toPaxosPacketArray(preActiveProposals.toArray()));
		}
		else if((acceptList = this.coordinator.handlePrepareMessageReply(prepareReply, this.groupMembers))!=null) {
			mtask = new MessagingTask(this.groupMembers, toPaxosPacketArray(acceptList.toArray()));
		}

		return mtask; // Could be unicast or multicast 
	}
	
	/* Utility method coz there seems to be no handy way to convert 
	 * a collection of one type to an array of a parent type. Yuck.
	 */
	private PaxosPacket[] toPaxosPacketArray(Object[] ppChildArray) {
		assert(ppChildArray!=null);
		PaxosPacket[] ppArray = new PaxosPacket[ppChildArray.length];
		for(int i=0; i<ppChildArray.length; i++) {
			ppArray[i] = (PaxosPacket)ppChildArray[i];
		}
		return ppArray;
	}

	/* Phase2a 
	 * Event: Received an accept message for a proposal with some ballot.
	 * 
	 * Action: Send back current or updated ballot to the ballot's coordinator. 
	 */
	private MessagingTask handleAccept(AcceptPacket accept) throws JSONException {

		Ballot b = this.paxosState.acceptAndUpdateBallot(accept); 
		AcceptReplyPacket acceptReply = new AcceptReplyPacket(this.myID, b, accept.pValue.proposal.slot); 
		
		MessagingTask mtask = new MessagingTask(accept.nodeID, acceptReply);
		return mtask;
	}

	/* Phase2b
	 * Event: Received a reply to an accept request (a proposal)
	 * 
	 * Action: If this reply results in a majority for the
	 * corresponding proposal, commit the request and notify all.
	 * 
	 * Return: The committed proposal if any to be multicast to 
	 * all replicas.
	 */
	private MessagingTask handleAcceptReply(AcceptReplyPacket acceptReply) {
		MessagingTask multicastDecision=null;
		PValuePacket committedPValue = this.coordinator.handleAcceptReply(acceptReply);
		// Could also call handleCommittedRequest below or even just rely on broadcast to all
		if(committedPValue!=null) {
			this.paxosState.putCommittedRequest(committedPValue.proposal.slot, committedPValue.proposal.req); // inform self of decision
			multicastDecision = new MessagingTask(this.groupMembers, committedPValue); // inform others of the decision
			log.info("Phase3/COMMIT: Coordinator " + this.myID + " sending commit for " + committedPValue);
		}
		return multicastDecision;
	}
	
	/* Phase3
	 * Event: Received notification about a committed proposal.
	 * 
	 * Action: This method is responsible for executing a committed 
	 * request. For this, it needs to call a handler implementing
	 * the PaxosInterface interface.
	 * 
	 * The methods executeIfNextCommittedRequest is coded so that
	 * it can be called exactly once per slot. This property
	 * means that we don't need any synchronization here (even
	 * if we happen to receive and process in parallel duplicate
	 * ProposalPackets reporting the same committed request).
	 */
	private void handleCommittedRequest(ProposalPacket proposal) {
		/* The structure below executes the nest committed request if
		 * it arrives in order, and then executes as many more as are
		 * queued in order. NOTE: Do not change this code structure 
		 * instead to queuing it first (as in the else block) and then 
		 * running the while loop to avoid repeating code (in the if block).
		 * Doing so will invoke putCommittedRequest() that might create
		 * a hashmap for each committed request and then nullify it 
		 * immediately even if the committed requests arrive in order.
		 */
		if(this.paxosState.executeIfNextCommittedRequest(proposal)) { // If next in-order request
			log.info("In-order commit");
			//FIXME: How to set recovery flag to true/false?
			this.clientRequestHandler.handlePaxosDecision(paxosID, proposal.req, false); 

			// Execute as many more in-order requests as you can. 
			RequestPacket rp=null;
			while((rp = this.paxosState.executeIfNextCommittedRequest()) != null) {
				this.clientRequestHandler.handlePaxosDecision(paxosID, rp, false); 
			}
		} else { // not in-order, so queue so as to execute later.
			log.info("Out-of-order commit");
			this.paxosState.putCommittedRequest(proposal.slot, proposal.req);
		}
		
		// No messaging task here.
	}
	

	/********************** Start of failure detection and recovery methods *****************/
	
	/* Should be called regularly. Checks whether current ballot
	 * coordinator is alive. If not, it checks if it should try
	 * to be the nest coordinator and if so, it becomes the next
	 * coordinator. This method can be called any time safely 
	 * by any thread.
	 * 
	 * FIXME: protected because it may be eventually called by 
	 * scheduleRunForCoordinatorTask in FailureDetection, but that 
	 * code is currently unused. 
	 */
	protected MessagingTask checkRunForCoordinator() {
		Ballot curBallot = this.paxosState.getBallot();
		MessagingTask multicastPrepare=null;

		/* If I am not coordinator and the current coordinator is dead 
		 * and I am not already trying to become coordinator (by existing). 
		 */
		if(curBallot.coordinatorID!=this.myID && !this.paxosManager.isNodeUp(curBallot.coordinatorID) &&
				!this.coordinator.exists(curBallot)) { 
			/* It is more robust to check if it has been a while since we heard anything
			 * from some recent coordinator and if so, try to become a coordinator ourself. 
			 * Otherwise, weird partitions can result in loss of liveness, e.g., the next-in-line
			 * coordinator thinks the current coordinator is up but most everyone else thinks
			 * the current coordinator is down. The downside of this check is that many nodes
			 * might near simultaneously try to become coordinator with no one succeeding, but
			 * this is unlikely if we rely on the above deterministic rule in the common case and 
			 * rely on no-coordinator-timeout only with a long timeout (much longer than it 
			 * typically takes for a new coordinator to get elected). We need to do this without 
			 * introducing additional state in this class, but instead using lastHeardFrom(C)
			 * in place of isNodeUp(C) where C is the current ballot coordinator.
			 * 
			 */
			log.info("Node " + this.myID + " finds current coordinator " + curBallot.coordinatorID + " dead");
			if(this.myID==getNextCoordinator(curBallot.ballotNumber+1, this.groupMembers) || // I am next-in-line
					paxosManager.lastCoordinatorLongDead(curBallot.coordinatorID)) { // or really long since *any* coordinator
				log.info("Node " + this.myID + " decides to be coordinator");
				Ballot newBallot = new Ballot(curBallot.ballotNumber+1, this.myID); 
				if(this.coordinator.makeCoordinator(newBallot, this.groupMembers)!=null) 
					multicastPrepare = new MessagingTask(this.groupMembers, 
							new PreparePacket(this.myID, this.myID, newBallot, PaxosPacketType.PREPARE));
			} else {
				log.fine("Node " + this.myID + " thinks next coordinator = " + getNextCoordinator(curBallot.ballotNumber+1, this.groupMembers) + 
						", and lastCoordinatorLongDead = " + paxosManager.lastCoordinatorLongDead(curBallot.coordinatorID));
			}
		}
		return multicastPrepare;
	}
	/* Computes the next coordinator as the node with the smallest ID 
	 * that is still up. We could plug in any dterministic policy here.
	 * But this policy should somehow take into account whether nodes
	 * are up or down. Otherwise, paxos will be stuck if both the 
	 * current and the next-in-line coordinator are both dead.
	 */
	private int getNextCoordinator(int ballotnum, int[] members) {
		Arrays.sort(members);
		for(int i=0; i<members.length;i++) {
			if(this.paxosManager!=null && this.paxosManager.isNodeUp(members[i])) return members[i];
		}
		return members[ballotnum % members.length]; // TBD: vestige of earlier policy
	}

	/* Event: Received a sync request.
	 * Action: Send a sync reply containing missing committed requests to the sender (requester).
	 */
	private MessagingTask handleSyncRequest(SynchronizePacket synchronizePacket) throws JSONException{
		ArrayList<Integer> missingSlotNumbers = this.paxosState.getMissingSlotsCommittedRequests();
		int maxDecision = this.paxosState.getMaxSlotCommittedRequests();
		SynchronizeReplyPacket srp =  new SynchronizeReplyPacket(this.myID,maxDecision, missingSlotNumbers, true);

		MessagingTask mtask = new MessagingTask(synchronizePacket.nodeID, srp);
		return mtask;
	}

	/* Event: Received a sync reply packet with a list of missing committed requests
	 * Action: Send back all missing committed requests to the sender (replier).
	 */
	private MessagingTask handleSyncReplyPacket(SynchronizeReplyPacket synchronizeReplyPacket) throws JSONException {
		ArrayList<ProposalPacket> missingCommits = this.paxosState.getMissingCommits(synchronizeReplyPacket);

		MessagingTask unicasts = new MessagingTask(synchronizeReplyPacket.nodeID, (PaxosPacket[])missingCommits.toArray());
		return unicasts;
	}
	
	/* FIXME: This method is only needed to handle weird partitions, e.g.,
	 * when the current coordinator seems up to the next coordinator
	 * but is in reality down with respect to everyone else. In this case,
	 * we will be stuck unless a node takes into account reports from
	 * other nodes declaring a coordinator dead.
	 */
	private void handleNodeStatusUpdate(
			FailureDetectionPacket failureDetectionPacket) {
		// TODO Auto-generated method stub
		assert(false) : "This method has not yet been implemented.";
	}

	/********************** End of failure detection and recovery methods *****************/
	
	/************************ Start of testing methods **************************/
	protected void testingInit(int load) {
		this.coordinator.testingInitCoord(load);
		this.paxosState.testingInitInstance(load);
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("FAILURE: I am not tested, so I am useless. Try running PaxosManager's test for now.");
	}

}
