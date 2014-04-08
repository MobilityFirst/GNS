package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;
import edu.umass.cs.gns.nsdesign.packet.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptReplyPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PreparePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PrepareReplyPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.StatePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.SynchronizePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.SynchronizeReplyPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.MessagingTask;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.SlotBallotState;
import edu.umass.cs.gns.util.MatchKeyable;

/**
@author V. Arun
 */

/* This class is the top-level paxos class per application or paxos group
 * on a machine. This is the only class that exposes public methods. 
 * Actually even this class will soon become "protected" as the only way 
 * to use it will be through the corresponding PaxosManager even if there 
 * is just one paxos application running on the machine.
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
 * PaxosInstanceStateMachine final fields: ~80B
 * PaxosAcceptor: ~90B
 * PaxosCoordinatorState: ~60B 
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
 * 
 * FIXME: Can this state machine get stuck permanently? Yes, in some
 * scenarios. (1) Under high load, messages to an unreachable destination
 * can get dropped. NIO will buffer up to a threshold limit and keep
 * trying to reconnect and send to the destination, and Messenger will 
 * keep trying to retransmit with exponential backoff, but if a 
 * destination is just dead, the send buffer will eventually fill up
 * and we have to stop retransmitting (or exhaust heap space). Now if
 * the destination comes back up, it would have missed some messages.
 * If more than a majority of destinations are highly loaded and they 
 * also crash at some point (i.e., they become !isConnected() from
 * NIO's view) and they subsequently come back up, they could have
 * missed some messages. 
 * 
 * If a majority miss some message, say a prepare or an accept, 
 * the state machine could get stuck. E.g., if a majority miss a prepare, 
 * the coordinator may never get elected as follows. The minority of 
 * acceptors who did receive the prepare will assume the prepare's 
 * sender as the current coordinator. The rest might still think the 
 * previous coordinator is the current coordinator. All acceptors 
 * could be thinking that their current coordinator is up, so nobody 
 * will bother running for coordinator. To break this impasse, we need
 * to resend the prepare. This has been now incorporated in the
 * checkRunForCoordinator method that periodically checks to see if
 * we need to "(re)run for coordinator" (for the same ballot) if we
 * have been waiting for too long (having neither received a prepare
 * majority nor a preemption) for the ballot to complete.
 * 
 * If a majority miss an accept, but decisions for any requests at 
 * all are being committed, then the loss will likely get fixed by the
 * fixGaps method designed to fix decisions received out of order.
 * This method will first check if the next-in-line decision's 
 * coordinator is local and if so, will poke it to recommander the 
 * request if the accept has been waiting for too long (for a majority 
 * or preemption). If not, it will do its usual, graceful job of 
 * checking if the received decision is too out of order 
 * (>SYNC_THRESHOLD) and if so, will send out a request hunting for 
 * missing decisions.
 * 
 * So when can the machine get stuck? If the most recent accept(s) 
 * for the next-in-line request(s) is(are) missed by a majority and 
 * there are no more new decisions that get committed (because
 * there are no more requests being sent), then nothing in the 
 * system will prompt the coordinator to resend the last few 
 * accepts. This case is not worth worrying about. Note that it
 * is unlikely that messages to/from a majority are lost
 * 
 */
public class PaxosInstanceStateMachine implements MatchKeyable<String,Short> {
	private static final boolean PAXOS_ID_AS_STRING=false; // if false, must invoke getPaxosID() as less often as possible
	protected static final int INTER_CHECKPOINT_INTERVAL = 100; // must be >= 1, does not depend on anything else
	private static final int SYNC_THRESHOLD = 10; // out-of-order-ness prompting synchronization, must be >=1 
	private static final int MAX_SYNC_GAP = INTER_CHECKPOINT_INTERVAL-1;
	protected static final int MAX_OUTSTANDING_LOAD = 100*INTER_CHECKPOINT_INTERVAL;
	public static final boolean DEBUG=PaxosManager.DEBUG;


	/************ final Paxos state that is unchangeable after creation ***************/
	private final int[] groupMembers; // final coz group changes => new paxos instance
	private final Object paxosID; // App ID or "GUID". Object to allow easy testing across byte[] and String
	private final short version;
	private final int myID;
	private final PaxosManager paxosManager;
	private final Replicable clientRequestHandler;

	/************ Non-final paxos state that is changeable after creation *******************/
	PaxosAcceptor paxosState=null;	// uses ~125B of empty space when not actively processing requests
	PaxosCoordinator coordinator=null; // uses just a single pointer's worth of space unless I am a coordinator
	/************ End of non-final paxos state ***********************************************/

	// static, so does not count towards space.
	private static Logger log = Logger.getLogger(PaxosInstanceStateMachine.class.getName()); // GNS.getLogger();

	PaxosInstanceStateMachine(String groupId, short version, int id, Set<Integer> gms, Replicable app, PaxosManager pm) {

		/**************** final assignments ***********************
		 * A paxos instance is born with a paxosID, version
		 * this instance's node ID, the application request handler, 
		 * the paxos manager, and the group members.
		 */
		this.paxosID = PAXOS_ID_AS_STRING ? groupId : groupId.getBytes();
		this.version = version;
		this.myID = id;
		this.clientRequestHandler = app;
		this.paxosManager = pm;

		// Copy set gms to array groupMembers
		assert(gms.size()>0);
		this.groupMembers = new int[gms.size()];
		int index=0; for(int i : gms) this.groupMembers[index++] = i; 
		Arrays.sort(this.groupMembers); 
		/**************** End of final assignments *******************/

		/* All non-final state is store in PaxosInstanceState (for acceptors)
		 * or in PaxosCoordinatorState (for coordinators) that inherits from
		 * PaxosInstanceState.
		 */
		if(pm!=null) {initiateRecovery(pm.getPaxosLogger());} 
		else testingNoRecovery(); // used only for testing size
		
		if(!TESTPaxosConfig.MEMORY_TESTING) log.info("Initialized paxos " + (this.paxosState.getBallot().coordinatorID==this.myID ? 
				"COORDINATOR":"instance") + " for " + groupId + " at node " + myID + " with a total of " +
				groupMembers.length + " members. " + this.paxosState.toString() +this.coordinator.toString());
	}

	protected String getPaxosID() {return (paxosID instanceof String ? (String)paxosID : new String((byte[])paxosID)); }
	public String getKey() {return this.getPaxosID();}
	public Short getVersion() {return this.version;}
	public int[] getMembers() {return this.groupMembers;}
	public int getNodeID() {return this.myID;}
	public Replicable getApp() {return this.clientRequestHandler;}

	/* isStopped()==true means that this paxos instance is
	 * dead and completely harmless (even if the underlying object has
	 * not been garbage collected by the JVM. In particular, it can 
	 * NOT make the app execute requests or send out paxos messages 
	 * to the external world.
	 */
	public boolean isStopped() {return this.paxosState.isStopped();}
	public boolean forceStop() { // not synchronized as coordinator can die anytime anyway 
		this.coordinator.forceStop(); 
		this.paxosState.forceStop(); // 
		this.paxosManager.getPaxosLogger().remove(getPaxosID()); // drop all log state
		return true;
	}

	protected void handlePaxosMessage(JSONObject msg) throws JSONException {
		/* Note: Because incoming messages may be handled concurrently, some messages
		 * may continue to get processed for a little while after a stop has been
		 * executed and even after isStopped() is true (because isStopped() was
		 * false when those messages came in here). But that is okay coz these 
		 * messages can only be ineffective old messages or messages reinforcing the 
		 * stop as it is impossible for paxos to commit any request after a stop.
		 */
		if(this.paxosState.isStopped()) return;
		if(TESTPaxosConfig.isCrashed(this.myID)) return; // Tester says I have crashed
		if(this.coordinator.isOverloaded(this.paxosState.getSlot())) {this.paxosManager.killAndRecoverMe(this);}

		PaxosPacketType msgType = PaxosPacket.getPaxosPacketType(msg);
		if(DEBUG) log.info("Node " + this.myID + " received " + PaxosPacket.getPaxosPacketType(msg) + ": " + msg);
		boolean recovery = PaxosPacket.isRecovery(msg); // recovery means we won't message any replies

		MessagingTask[] mtasks = new MessagingTask[2];
		mtasks[0] = (!recovery ? checkRunForCoordinator() : null); // Check periodically; we only need to bother if we get any messages.

		MessagingTask mtask=null;
		switch(msgType) {
		case REQUEST:
			mtask = handleRequest(new RequestPacket(msg));
			// send RequestPacket to current coordinator
			break;
			// replica --> coordinator
		case PROPOSAL:
			mtask = handleProposal(new ProposalPacket(msg));
			// either send ProposalPacket to current coordinator or send AcceptPacket to all
			break;
			// coordinator --> replica
		case DECISION:
			mtask = handleCommittedRequest(new PValuePacket(msg));
			// send nothing, but log decision
			break;
			// coordinator --> replica
		case PREPARE:
			mtask = handlePrepare(new PreparePacket(msg));
			// send PreparePacket prepare reply to coordinator
			break;
			// replica --> coordinator
		case PREPARE_REPLY:
			mtask = handlePrepareReply(new PrepareReplyPacket(msg));
			// send AcceptPacket[] to all
			break;
			// coordinator --> replica
		case ACCEPT:
			mtask = handleAccept(new AcceptPacket(msg));
			// send AcceptReplyPacket to coordinator
			break;
			// replica --> coordinator
		case ACCEPT_REPLY:
			mtask = handleAcceptReply(new AcceptReplyPacket(msg));
			// send PValuePacket decision to all
			break;
		case SYNC_REQUEST:
			mtask = handleSyncRequest(new SynchronizePacket(msg));
			// send SynchronizeReplyPacket to sender
			break;
		case SYNC_REPLY:
			mtask = handleSyncReplyPacket(new SynchronizeReplyPacket(msg));
			// send SynchronizeReplyPacket to sender
			break;
		case CHECKPOINT_STATE:
			handleCheckpoint(new StatePacket(msg));
			break;
		case CHECKPOINT_REQUEST:
			handleCheckpointRequest(new SynchronizeReplyPacket(msg));
			break;
		default: 
			assert(false) : "Paxos instance received an unrecognizable packet: " + msg;
		}
		mtasks[1] = mtask;
		
		if(!recovery) {this.sendMessagingTask(mtasks);} 
	}


	/************** Start of private methods ****************/

	/* Invoked both when a paxos instance is first created and when
	 * it recovers after a crash. It is all the same as far as the
	 * paxos instance is concerned (provided we ensure that
	 * the app state after executing the first request (slot 0)
	 * is checkpointed, which we do).
	 */
	private boolean initiateRecovery(AbstractPaxosLogger paxosLogger) {
		String pid=this.getPaxosID();
		SlotBallotState slotBallot = paxosLogger.getSlotBallotState(pid);
		if(!TESTPaxosConfig.MEMORY_TESTING) log.info("Node "+this.myID + ", " + pid+
				" recovered state: "+ (slotBallot!=null ? slotBallot.state:"NULL"));

		// update app state
		if(slotBallot!=null && slotBallot.state!=null) this.clientRequestHandler.updateState(pid, slotBallot.state);

		this.coordinator = new PaxosCoordinator(); // just a shell class to wrap coordinator state
		//if(initBallot.coordinatorID==this.myID) coordinator.makeCoordinator(initBallot, groupMembers, initSlot, true);
		/* Note: We don't create coordinator state here. It will get created if needed when 
		 * the first external (non-recovery) packet is received. We could create the 
		 * coordinator here if the initial slot is 0. But in general, coordinator
		 * state is not recoverable from the logs and the easiest way to recover
		 * it is by running for coordinator again; we already have a provision for
		 * checking as the first thing in handlePaxosMessage whether we should run
		 * for coordinator, which will work in all situations including slot 0.
		 */

		/* Creating the acceptor should be the last step just before rolling
		 * forward coz creating the acceptor makes this paxos instance "live",
		 * i.e., it starts allowing messages to modify safety-critical state.
		 */
		this.paxosState = new PaxosAcceptor(slotBallot!=null ? slotBallot.ballotnum : 0, 
				slotBallot!=null ? slotBallot.coordinator: getNextCoordinator(0, groupMembers), 
						slotBallot!=null ? slotBallot.slot+1 : 0); 
		if(slotBallot==null) TESTPaxosConfig.setRecovered(this.getNodeID(), pid, true);

		return true; // return value will be ignored 
	}

	/* The one method for all message sending. */
	private void sendMessagingTask(MessagingTask mtask) {
		if(this.paxosState.isStopped()) return;
		if(TESTPaxosConfig.isCrashed(myID)) return; 
		if(mtask==null || mtask.isEmpty()) return; 

		if(DEBUG) log.info("Node " + this.myID + " sending: " + mtask.toString());
		mtask.putPaxosIDVersion(this.getPaxosID(), this.getVersion());
		try {
			paxosManager.send(mtask);
		} catch (IOException ioe) {
			log.severe("Node "+this.myID +" encountered IOException while sending " + mtask);
			ioe.printStackTrace();
			/* FIXME: We can't throw this exception upward because it will get sent all
			 * the way back up to PacketDemultiplexer whose incoming packet initiated
			 * this whole chain of events. It seems silly for PacketDemultiplexer
			 * to throw an IOException caused by the sends resulting from processing
			 * that packet. So we should handle this exception right here. But what
			 * should we do? We can ignore it as the network does not need to be 
			 * reliable anyway. Revisit as needed.
			 */
		} catch(JSONException je) {
			/* Same thing for other exceptions. Nothing useful to do here */
			log.severe("Node "+this.myID+" encountered JSONException while sending "+mtask);
			je.printStackTrace();
		}
	}
	private void sendMessagingTask(MessagingTask[] mtasks) throws JSONException {
		for(MessagingTask mtask : mtasks) this.sendMessagingTask(mtask);
	}

	/* "Phase0"
	 * Event: Received a request from a client.
	 * 
	 * Action: Call handleProposal which will send the corresponding
	 * proposal to the current coordinator.
	 */
	private MessagingTask handleRequest(RequestPacket msg) throws JSONException {
		if(DEBUG) log.info("Node " + this.myID + " Phase0/CLIENT_REQUEST: " + msg);
		return handleProposal(new ProposalPacket(0, msg));
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
		if(this.coordinator.exists(this.paxosState.getBallot())) { // propose to all
			if(DEBUG) log.info("Node " + this.myID + " Phase2a/ACCEPT: Coordinator " + 
					"exists (maybe active or inactive), initiating accept phase for " + proposal);
			AcceptPacket multicastAccept = null;
			if(this.coordinator.isOverloaded(this.paxosState.getSlot())) {log.warning("Node " +this.myID+" OVERLOADED, dropping "+proposal);} 
			else multicastAccept = this.coordinator.propose(this.groupMembers, proposal); 
			mtask = multicastAccept!=null ? new MessagingTask(this.groupMembers, multicastAccept) : null; // multicast accept to all
		}
		else { // else unicast to current coordinator
			if(DEBUG) log.fine("Node "+this.myID+" is not the coordinator, forwarding to " + this.paxosState.getBallot().coordinatorID);
			mtask = new MessagingTask(this.paxosState.getBallot().coordinatorID, proposal); // unicast to coordinator
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
	private MessagingTask handlePrepare(PreparePacket rcvdPrepare) throws JSONException{
		PreparePacket prepare = rcvdPrepare.fixPreparePacketReceiver(this.myID);
		PrepareReplyPacket prepareReply = this.paxosState.handlePrepare(prepare);  
		if(prepareReply==null) return null; // can happen only if acceptor is stopped
		
		// we may also need to look into disk if ACCEPTED_PROPOSALS_ON_DISK is true
		if(PaxosAcceptor.ACCEPTED_PROPOSALS_ON_DISK) {
			prepareReply.accepted.putAll(this.paxosManager.getPaxosLogger().getLoggedAccepts(
					this.getPaxosID(), prepare.firstUndecidedSlot));
		}

		LogMessagingTask mtask = new LogMessagingTask(prepareReply.coordinatorID, prepareReply, (PaxosPacket)prepare);
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
	private MessagingTask handlePrepareReply(PrepareReplyPacket prepareReply) {
		MessagingTask mtask=null;

		ArrayList<ProposalPacket> preActiveProposals=null;
		ArrayList<AcceptPacket> acceptList = null;

		if((preActiveProposals = this.coordinator.getPreActivesIfPreempted(prepareReply, this.groupMembers))!=null) {
			log.info("Node " + this.myID + " coordinator election PREEMPTED by node " + prepareReply.ballot.coordinatorID);
			mtask = new MessagingTask(prepareReply.ballot.coordinatorID, MessagingTask.toPaxosPacketArray(preActiveProposals.toArray()));
		}
		else if((acceptList = this.coordinator.handlePrepareReply(prepareReply, this.groupMembers))!=null && !acceptList.isEmpty()) {
			mtask = new MessagingTask(this.groupMembers, MessagingTask.toPaxosPacketArray(acceptList.toArray()));
			log.info("Node "+this.myID + " elected coordinator; sending ACCEPTs for the following adopted/self-proposed proposals: " + mtask);
		}

		return mtask; // Could be unicast or multicast 
	}

	/* Phase2a 
	 * Event: Received an accept message for a proposal with some ballot.
	 * 
	 * Action: Send back current or updated ballot to the ballot's coordinator. 
	 */
	private MessagingTask handleAccept(AcceptPacket accept) throws JSONException {
		Ballot ballot = this.paxosState.acceptAndUpdateBallot(accept); 
		if(ballot==null) return null; // can happen only if acceptor is stopped

		this.garbageCollectAccepted(accept.majorityCommittedSlot); 

		AcceptReplyPacket acceptReply = new AcceptReplyPacket(this.myID, ballot, accept.slot, this.paxosState.getSlot()-1);
		AcceptPacket toLog = (accept.ballot.compareTo(ballot)>=0 ? accept : null); // lower ballot => no logging, only accept reply
		MessagingTask mtask = toLog!=null ? new LogMessagingTask(accept.nodeID, acceptReply, toLog) :
			new MessagingTask(accept.nodeID, acceptReply);
		return mtask;
	}
	/* We don't need to implement this. Accept logs are pruned 
	 * while checkpointing anyway, which is enough. Worse, it 
	 * is probably inefficient to touch the disk for GC upon
	 * every new gcSlot (potentially every accept and decision).
	 */
	private void garbageCollectAccepted(int gcSlot) {}	

	/* Phase2b
	 * Event: Received a reply to an accept request (a proposal)
	 * 
	 * Action: If this reply results in a majority for the
	 * corresponding proposal, commit the request and notify all.
	 * If this preempts a proposal being coordinated because
	 * it contains a higher ballot, forward to the preempting
	 * coordinator in the higher ballot reported.
	 * 
	 * Return: The committed proposal if any to be multicast to 
	 * all replicas, or the preempted proposal if any to be
	 * unicast to the preempting coordinator. Null if neither.
	 */
	private MessagingTask handleAcceptReply(AcceptReplyPacket acceptReply) {
		PValuePacket committedPValue = this.coordinator.handleAcceptReply(this.groupMembers, acceptReply);
		if(committedPValue==null) return null;

		MessagingTask multicastDecision=null; MessagingTask unicastPreempted=null; // separate variables only for code readability
		// Could also call handleCommittedRequest below or even just rely on broadcast to all
		if(committedPValue.getType()==PaxosPacket.PaxosPacketType.DECISION) {
			this.handleCommittedRequest(committedPValue); // can't inform even self without logging first, ignoring return value
			multicastDecision = new MessagingTask(this.groupMembers, committedPValue); // inform everyone of the decision
		} else if (committedPValue.getType()==PaxosPacket.PaxosPacketType.PREEMPTED) {
			/* Could do nothing or could forward to a new coordinator. The 
			 * new(er) coordinator information is within acceptReply. Note
			 * that our coordinator status may still be active and it
			 * will be so until all of its requests have been preempted.
			 * Note also that our local acceptor might still think
			 * we are the coordinator. The only evidence of a new 
			 * coordinator is in acceptReply that must have reported
			 * a higher ballot if we are here, hence the assert.
			 */
			assert(committedPValue.ballot.compareTo(acceptReply.ballot) < 0);
			unicastPreempted = new MessagingTask(acceptReply.ballot.coordinatorID, committedPValue.getRequestPacket()); 
			if(DEBUG) log.info("Node " + this.myID + " forwarding preempted request to node " + 
					acceptReply.ballot.coordinatorID + ": " + committedPValue);

		}
		return committedPValue.getType()==PaxosPacket.PaxosPacketType.DECISION ? multicastDecision : unicastPreempted;
	}

	/* Phase3
	 * Event: Received notification about a committed proposal.
	 * 
	 * Action: This method is responsible for executing a committed 
	 * request. For this, it needs to call a handler implementing
	 * the PaxosInterface interface.
	 * 	
	 */
	private MessagingTask handleCommittedRequest(PValuePacket committed) {

		// Log, extract from or add to acceptor, and execute the request at the app
		AbstractPaxosLogger.logAndExecute(this.paxosManager.getPaxosLogger(), committed, this); 
		TESTPaxosConfig.commit(committed.requestID); 

		if(this.paxosState.getSlot() < committed.slot) if(DEBUG) log.info("Node " + this.myID + " expecting " + 
				this.paxosState.getSlot() + " recieved out-of-order commit: "+committed);

		return this.fixLongDecisionGaps(committed);
	}
	/* The three actions--(1) extracting the next slot request from the acceptor, 
	 * (2) having the app execute the request, and (3) checkpoint if needed--need
	 * to happen atomically. If the app throws an error while executing the 
	 * request, we need to retry until successful, otherwise, the replicated 
	 * state machine will be stuck. So, essentially, the app has to support
	 * atomicity or the operations have to be idempotent for correctness of
	 * the replicated state machine.
	 * 
	 * This method is protected, not private, because it needs to be called by 
	 * the logger after it is done logging the committed request. Having the 
	 * logger call this method is only space-efficient design alternative.
	 */
	protected synchronized void extractExecuteAndCheckpoint(PValuePacket loggedDecision) {
		if(this.paxosState.isStopped()) return;
		this.paxosState.assertSlotInvariant();
		PValuePacket inorderDecision=null;
		// extract next in-order decision
		while((inorderDecision = this.paxosState.putAndRemoveNextExecutable(loggedDecision))!=null) { 
			if(DEBUG) log.info("Node " + this.myID + " in-order commit: "+inorderDecision); 
			
			// execute it until executed, we are *by design* stuck o/w; must be atomic with extraction
			boolean executed=false;
			String pid = this.getPaxosID();
			while(!executed) {
					executed = this.clientRequestHandler.handleDecision(pid, 
							inorderDecision.toString(), false);
					if(!executed) log.severe("App failed to execute request, retrying: "+inorderDecision);
			}
			TESTPaxosConfig.execute(inorderDecision.requestID);

			// checkpoint if needed, must be atomic with the execution 
			if(inorderDecision.slot%INTER_CHECKPOINT_INTERVAL==0 || inorderDecision.isStopRequest()) { 
				this.paxosManager.getPaxosLogger().putCheckpointState(pid, this.version, 
						this.groupMembers, inorderDecision.slot, this.paxosState.getBallot(), 
						this.clientRequestHandler.getState(pid), this.paxosState.getGCSlot());
				log.info("Node " + this.myID + " checkpointed slot " + inorderDecision.slot + " and garbage " +
						"collected logged accepts upto slot " + this.paxosState.getGCSlot());
			}
			if(inorderDecision.isStopRequest()) this.paxosManager.kill(this);
		}
		this.paxosState.assertSlotInvariant();
		assert(this.paxosState.getSlot()!=loggedDecision.slot); // otherwise it would've been executed
	}
	// Invoked by handleCommittedRequest above ...
	private MessagingTask fixLongDecisionGaps(PValuePacket committed) {
		MessagingTask reAccept = this.pokeLocalCoordinator();
		MessagingTask fixGapsRequest=null;
		if(committed.slot - this.paxosState.getSlot() > PaxosInstanceStateMachine.SYNC_THRESHOLD) {
			fixGapsRequest = this.handleSyncRequest(new SynchronizePacket(committed.ballot.coordinatorID));
			if(fixGapsRequest!=null) log.info("Node " + this.myID + " fixing gaps: " + fixGapsRequest);
		}
		return reAccept!=null ? reAccept : fixGapsRequest;
	}
	private MessagingTask pokeLocalCoordinator() {
		MessagingTask reAccept = null;
		if(this.coordinator.isCommandering(this.paxosState.getSlot())) {
			AcceptPacket accept = this.coordinator.reCommander(this.paxosState.getSlot());
			if(accept!=null) reAccept = new MessagingTask(this.groupMembers, accept);
		}
		return reAccept;
	}

	/*************************** End of phase 3 methods ********************************/


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

		/* 
		 * curBallot is my acceptor's ballot; "my acceptor's coordinator" is that ballot's coordinator.
		 * 
		 *  If I am not already a coordinator at least as high as my acceptor's ballot's coordinator
		 *    AND
		 *  (I am my acceptor's coordinator 
		 *      OR (my acceptor's coordinator is dead 
		 *              AND 
		 *         (I am next in line OR the current coordinator has been dead for a really long time)
		 *         )
		 *   )
		 */
		if(!this.coordinator.exists(curBallot) && 
				(curBallot.coordinatorID==this.myID // can happen during recovery
				|| 
				(!this.paxosManager.isNodeUp(curBallot.coordinatorID) 
						&& 
						(this.myID==getNextCoordinator(curBallot.ballotNumber+1, this.groupMembers) 
						|| 
						paxosManager.lastCoordinatorLongDead(curBallot.coordinatorID))))) 
		{ 
			/* We normally round-robin across nodes for electing coordinators, e.g., 
			 * node 7 will try to become coordinator in ballotnum such that ballotnum%7==0
			 * if it suspects that the current coordinator is dead. But it is more robust 
			 * to check if it has been a while since we heard anything from the current 
			 * coordinator and if so, try to become a coordinator ourself even though it
			 * is not our turn. Otherwise, weird partitions can result in loss of liveness, 
			 * e.g., the next-in-line coordinator thinks the current coordinator is up but 
			 * most everyone else thinks the current coordinator is down. Or the next-in-line 
			 * coordinator itself could be dead. The downside of this lasCoordinatorLongDead 
			 * check is that many nodes might near simultaneously try to become coordinator 
			 * with no one succeeding for a while, but this is unlikely to be a problem 
			 * if we rely on the deterministic round-robin rule in the common case and rely 
			 * on the lasCoordinatorLongDead with a longer timeout (much longer 
			 * than the typical node failure detection timeout). 
			 */
			log.info("Node " + this.myID + " decides to take over from current coordinator " + 
					curBallot.coordinatorID + " who appears to be dead or non-existent (if myself)");
			Ballot newBallot = new Ballot(curBallot.ballotNumber+1, this.myID); 
			if(this.coordinator.makeCoordinator(newBallot.ballotNumber, newBallot.coordinatorID, 
					this.groupMembers, this.paxosState.getSlot(), false)!=null) {
				multicastPrepare = new MessagingTask(this.groupMembers, new PreparePacket(this.myID, this.myID, newBallot));
			}
		} else if(this.coordinator.waitingTooLong()) { // resending prepare
			Ballot newBallot = this.coordinator.remakeCoordinator(groupMembers);
			if(newBallot!=null) multicastPrepare = new MessagingTask(this.groupMembers, 
					new PreparePacket(this.myID, this.myID, newBallot));
		} else if(!this.paxosManager.isNodeUp(curBallot.coordinatorID)) { // not my job even though current coordinator is dead
			log.info("Node " + this.myID + " thinks next coordinator = " + 
					getNextCoordinator(curBallot.ballotNumber+1, this.groupMembers) + 
					", and lastCoordinatorLongDead = " + paxosManager.lastCoordinatorLongDead(curBallot.coordinatorID));
		}
		return multicastPrepare;
	}
	/* Computes the next coordinator as the node with the smallest ID 
	 * that is still up. We could plug in any deterministic policy here.
	 * But this policy should somehow take into account whether nodes
	 * are up or down. Otherwise, paxos will be stuck if both the 
	 * current and the next-in-line coordinator are both dead.
	 */
	private int getNextCoordinator(int ballotnum, int[] members) {
		for(int i=1; i<members.length; i++) assert(members[i-1] < members[i]);
		for(int i=0; i<members.length;i++) {
			if(this.paxosManager!=null && this.paxosManager.isNodeUp(members[i])) return members[i];
		}
		return members[ballotnum % members.length]; // TBD: vestige of earlier policy
	}

	/* Event: Received or locally generated a sync request.
	 * Action: Send a sync reply containing missing committed requests to the requester.
	 * If the requester is myself, multicast to all.
	 */
	// FIXME: Change name to a local method and call handleSyncReply handleSyncRequest instead
	private MessagingTask handleSyncRequest(SynchronizePacket syncPacket) {
		ArrayList<Integer> missingSlotNumbers = this.paxosState.getMissingCommittedSlots(MAX_SYNC_GAP);
		if(missingSlotNumbers==null || missingSlotNumbers.isEmpty()) return null;

		int maxDecision = this.paxosState.getMaxCommittedSlot();
		SynchronizeReplyPacket srp =  new SynchronizeReplyPacket(this.myID,maxDecision, missingSlotNumbers, 
				maxDecision-this.paxosState.getSlot() >= MAX_SYNC_GAP);

		MessagingTask mtask = syncPacket.nodeID!=this.myID ? new MessagingTask(syncPacket.nodeID, srp) :
			new MessagingTask(otherGroupMembers(), srp); // send sync request to coordinator or multicast to all but me
		return mtask;
	}

	// Utility method to get members except myself
	private int[] otherGroupMembers() {
		int[] others = new int[this.groupMembers.length-1];
		int j=0;
		for(int i=0; i<this.groupMembers.length;i++) {
			if(this.groupMembers[i]!=this.myID) others[j++] = this.groupMembers[i];
		}
		return others;
	}

	/* Event: Received a sync reply packet with a list of missing committed requests
	 * Action: Send back all missing committed requests from the log to the sender (replier).
	 * 
	 * We could try to send some from acceptor memory instead of the log, but in general, it is not worth 
	 * the effort. Furthermore, if the sync gap is too much, do a checkpoint transfer.
	 */
	private MessagingTask handleSyncReplyPacket(SynchronizeReplyPacket syncReply) throws JSONException {
		// A sync reply also acts as a checkpoint request.
		if(syncReply.missingTooMuch) return handleCheckpointRequest(syncReply); 

		// Get decisions from database. We are unlikely to have most of them in memory.
		int minMissingSlot = Integer.MAX_VALUE; 
		for(int i : syncReply.missingSlotNumbers) minMissingSlot = Math.min(minMissingSlot, i);
		ArrayList<PValuePacket> missingDecisions = 
				this.paxosManager.getPaxosLogger().getLoggedDecisions(this.getPaxosID(), minMissingSlot, syncReply.maxDecisionSlot);
		for(Iterator<PValuePacket> pvalueIterator = missingDecisions.iterator(); pvalueIterator.hasNext();) {
			if(!syncReply.missingSlotNumbers.contains(pvalueIterator.next().slot)) pvalueIterator.remove(); // filter non-missing
		}
		MessagingTask unicasts = missingDecisions.isEmpty() ? null : 
			new MessagingTask(syncReply.nodeID, MessagingTask.toPaxosPacketArray(missingDecisions.toArray()));
		log.info("Node " + this.myID + " sending missing decisions to node " + syncReply.nodeID +": " + unicasts);
		return unicasts;
	}

	/* Event: Received a request for a recent checkpoint presumably
	 * from a replica that has recovered after a long down time. 
	 * Action: Send checkpoint to requester.
	 */
	private MessagingTask handleCheckpointRequest(SynchronizeReplyPacket syncReply) {
		if(this.paxosState.getSlot()==0) log.warning("Node " + this.myID + (!this.coordinator.exists() ? "[acceptor]" : 
			this.coordinator.isActive() ? "[coordinator]" : "[preactive-coordinator]" ) + " has no state (yet) for " + syncReply);
		StatePacket statePacket = this.paxosManager.getPaxosLogger().getStatePacket(this.getPaxosID());
		if(statePacket!=null) log.info("Node " + this.myID + " sending checkpoint to node " + syncReply.nodeID +": " + statePacket);
		return statePacket!=null ? new MessagingTask(syncReply.nodeID, statePacket) : null;
	}
	private void handleCheckpoint(StatePacket statePacket) {
		if(statePacket.slotNumber > this.paxosState.getSlot()) {
			this.paxosManager.getPaxosLogger().putCheckpointState(this.getPaxosID(), this.version, groupMembers, 
					statePacket.slotNumber, statePacket.ballot, statePacket.state, this.paxosState.getGCSlot());
		}
	}
	/********************** End of failure detection and recovery methods *****************/

	/************************ Start of testing methods **************************/
	/* Used only to test paxos instance size. We really need a paxosManager
	 * to do anything real with paxos.
	 */
	private void testingNoRecovery() {
		int initSlot = 0;
		this.coordinator = new PaxosCoordinator();
		if(this.groupMembers[0]==this.myID) coordinator.makeCoordinator(0, this.groupMembers[0], groupMembers, initSlot, true);
		this.paxosState = new PaxosAcceptor(0, this.groupMembers[0],initSlot);
	}

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
