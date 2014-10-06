package edu.umass.cs.gns.gigapaxos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.AcceptPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.AcceptReplyPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PaxosPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PreparePacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PrepareReplyPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.StatePacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.SyncDecisionsPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gns.gigapaxos.paxosutil.ActivePaxosState;
import edu.umass.cs.gns.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gns.gigapaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gns.gigapaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gns.gigapaxos.paxosutil.MessagingTask;
import edu.umass.cs.gns.gigapaxos.paxosutil.PaxosInstrumenter;
import edu.umass.cs.gns.gigapaxos.paxosutil.SlotBallotState;
import edu.umass.cs.gns.util.MatchKeyable;

/**
@author V. Arun
 */

/* This class is the top-level paxos class per instance or paxos group
 * on a machine. This is the only class that exposes public methods. 
 * Actually even this class will soon become "protected" as the only way 
 * to use it will be through the corresponding PaxosManager even if there 
 * is just one paxos application running on the machine.
 * 
 * This class delegates much of the interesting paxos actions to 
 * PaxosAcceptorState and PaxosCoordinator. It delegates all messaging
 * to PaxosManager's PaxosMessenger. It is "managed", i.e., its paxos
 * group is created and its incoming packets are demultiplexed, by
 * its PaxosManager. It's logging is handled by an implementation of
 * AbstractPaxosLogger.
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
 * Can this state machine get stuck permanently? Hopefully not coz here
 * is how we deal with loss. Under high load, messages to an unreachable 
 * destination can get dropped. NIO will buffer up to a threshold limit 
 * and keep trying to reconnect and send to the destination, and Messenger 
 * will keep trying to retransmit with exponential backoff, but if a 
 * destination is just dead, the send buffer will eventually fill up
 * and we have to stop retransmitting (or exhaust heap space). Now if
 * the destination comes back up, it would have missed some messages.
 * If more than a majority of destinations are highly loaded and they 
 * also crash at some point (i.e., they become !isConnected() from
 * NIO's view) and they subsequently come back up, they could have
 * missed some messages. If a majority miss some message, say a prepare 
 * or an accept, and there is nothing in the code here (not just in 
 * Messenger as that might eventually give up) to trigger their 
 * retransmission, the state machine can get stuck forever. We address
 * these as follows.
 * 
 * FIXED:  If a majority miss a prepare, 
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
 * FIXED: If a majority miss an accept, but any messages are still 
 * being received at all, then the loss will eventually get fixed by
 * a check similar to checkRunForCoordinator that upon receipt
 * of every message will poke the local coordinator to recommander the 
 * next-in-line accept if the accept has been waiting for too long 
 * (for a majority or preemption). Both the prepare and accept waiting
 * checks are quick O(1) operations.
 * 
 * So can the machine still get stuck forever? Hopefully not! 
 * 
 * Testability: This class is not unit-testable as almost all methods
 * depend on other classes. Both PaxosManager as well as TESTPaxosMain 
 * test this class.
 */
public class PaxosInstanceStateMachine implements MatchKeyable<String,Short> {
	private static final boolean PAXOS_ID_AS_STRING=false; // if false, must invoke getPaxosID() as less often as possible
	protected static final int INTER_CHECKPOINT_INTERVAL = 100; // must be >= 1, does not depend on anything else
	private static final int SYNC_THRESHOLD = 4*INTER_CHECKPOINT_INTERVAL; // out-of-order-ness prompting synchronization, must be >=1 
	private static final int MAX_SYNC_GAP = INTER_CHECKPOINT_INTERVAL;
	protected static final int MAX_OUTSTANDING_LOAD = 100*INTER_CHECKPOINT_INTERVAL;
	protected static final boolean POKE_ENABLED = false;
	public static final boolean DEBUG=PaxosManager.DEBUG;

	/************ final Paxos state that is unchangeable after creation ***************/
	private final int[] groupMembers; // final coz group changes => new paxos instance
	private final Object paxosID; // App ID or "GUID". Object to allow easy testing across byte[] and String
	private final short version;
	private final int myID;
	private final PaxosManager paxosManager;
	private final Replicable clientRequestHandler;

	/************ Non-final paxos state that is changeable after creation *******************/
	private PaxosAcceptor paxosState=null;	// uses ~125B of empty space when not actively processing requests
	private PaxosCoordinator coordinator=null; // uses just a single pointer's worth of space unless I am a coordinator
	/************ End of non-final paxos state ***********************************************/

	// static, so does not count towards space.
	private static Logger log = Logger.getLogger(PaxosInstanceStateMachine.class.getName()); // GNS.getLogger();

	PaxosInstanceStateMachine(String groupId, short version, int id, Set<Integer> gms, 
			Replicable app, PaxosManager pm, HotRestoreInfo hri) {

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
		if(pm!=null && hri==null) {initiateRecovery(pm.getPaxosLogger());}
		else if(hri!=null) hotRestore(hri);
		else testingNoRecovery(); // used only for testing size

		getActiveState(); // initialize active state, so that we can be deactivated if idle

		if(!TESTPaxosConfig.MEMORY_TESTING) log.info("Initialized paxos " + (this.paxosState.getBallotCoord()==this.myID ? 
				"COORDINATOR":"instance") + " for " + groupId + " at node " + myID + " with a total of " +
				groupMembers.length + " members. " + this.paxosState.toString() +this.coordinator.toString());
	}
	public String getKey() {return this.getPaxosID();}
	public Short getVersion() {return this.version;}

	protected String getPaxosID() {return (paxosID instanceof String ? (String)paxosID : new String((byte[])paxosID)); }
	protected int[] getMembers() {return this.groupMembers;}
	protected int getNodeID() {return this.myID;}
	protected Replicable getApp() {return this.clientRequestHandler;}
	protected PaxosManager getPaxosManager() {return this.paxosManager;}
	public String toString() {return this.getNodeState() + " " + (this.paxosState!=null ? this.paxosState.toString():"null") +
			(this.coordinator.exists()?this.coordinator.toString():"null");}

	/* isStopped()==true means that this paxos instance is
	 * dead and completely harmless (even if the underlying object has
	 * not been garbage collected by the JVM. In particular, it can 
	 * NOT make the app execute requests or send out paxos messages 
	 * to the external world.
	 */
	public boolean isStopped() {return this.paxosState.isStopped();}
	protected boolean forceStop() { // not synchronized as coordinator can die anytime anyway 
		this.coordinator.forceStop(); 
		this.paxosState.forceStop(); // 
		return true;
	}
	protected boolean kill() {  // removes all database state and can not be recovered anymore
		this.forceStop();
		AbstractPaxosLogger.kill(this.paxosManager.getPaxosLogger(), getPaxosID()); // drop all log state
		if(DEBUG) assert(this.paxosManager.getPaxosLogger().getSlotBallotState(getPaxosID())==null);
		return true;
	}

	protected void handlePaxosMessage(JSONObject msg) throws JSONException {
		long methodEntryTime = System.currentTimeMillis();
		/* Note: Because incoming messages may be handled concurrently, some messages
		 * may continue to get processed for a little while after a stop has been
		 * executed and even after isStopped() is true (because isStopped() was
		 * false when those messages came in here). But that is okay coz these 
		 * messages can not spawn unsafe outgoing messages (as messaging is 
		 * turned off for all but DECISION or CHECKPOINT_STATE packets) and
		 * can not change any disk state.
		 */
		if(this.paxosState.isStopped()) return;
		if(TESTPaxosConfig.isCrashed(this.myID)) return; // Tester says I have crashed
		boolean recovery = PaxosPacket.isRecovery(msg); // recovery means we won't message any replies
		/* The reason we should not process regular messages until this instance has 
		 * rolled forward is that it might respond to a prepare with a list of 
		 * accepts fetched from disk that may be inconsistent with its acceptor state.
		 */
		if(!this.paxosManager.hasRecovered(getPaxosID()) && !recovery) return; // don't process regular messages until ready for rollForward
		this.getActiveState(true); 

		PaxosPacketType msgType = PaxosPacket.getPaxosPacketType(msg);
		if(DEBUG) log.info(getNodeState() + " received " + PaxosPacket.getPaxosPacketType(msg) + ": " + msg);

		MessagingTask[] mtasks = new MessagingTask[2];
		// check upon every message 
		mtasks[0] = (!recovery ? !this.coordinator.isActive() ? checkRunForCoordinator() : this.pokeLocalCoordinator() : null); 

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
		case SYNC_DECISIONS:
			mtask = handleSyncDecisionsPacket(new SyncDecisionsPacket(msg));
			// send SynchronizeReplyPacket to sender
			break;
		case CHECKPOINT_STATE:
			mtask = handleCheckpoint(new StatePacket(msg));
			break;
		case NO_TYPE:
			if(!TESTPaxosConfig.MEMORY_TESTING) 
				log.info(this.getNodeState() + " received a \"self-poke\" NO_TYPE message");
			break;
		default: 
			assert(false) : "Paxos instance received an unrecognizable packet: " + msg;
		}
		mtasks[1] = mtask;

		PaxosInstrumenter.update("handlePaxosMessage", methodEntryTime);

		this.checkIfTrapped(msg, mtasks[1]); // just to print a warning if needed
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
		SlotBallotState slotBallot = paxosLogger.getSlotBallotState(pid, this.getVersion(), true); // only place where version is checked
		if(!TESTPaxosConfig.MEMORY_TESTING) log.info(this.getNodeState()+
				" recovered state: "+ (slotBallot!=null ? slotBallot.state:"NULL"));

		// update app state
		if(slotBallot!=null && slotBallot.state!=null) this.clientRequestHandler.updateState(pid, slotBallot.state);

		this.coordinator = new PaxosCoordinator(); // just a shell class to wrap coordinator state
		if(slotBallot==null && roundRobinCoordinator(0)==myID) this.coordinator.makeCoordinator(0, myID, 
				getMembers(), 0, true); // initial coordinator assumed, not prepared
		/* Note: We don't have to create coordinator state here. It will get created if 
		 * needed when the first external (non-recovery) packet is received. But we 
		 * create the very first coordinator here as otherwise it is possible that 
		 * no coordinator gets elected as follows: the lowest ID node wakes up and
		 * either upon an external or self-poke message sends a prepare, but gets
		 * no responses because no other node is up yet. In this case, the other
		 * nodes when they boot up will not run for coordinator, and the lowest
		 * ID node will not resend its prepare if no more requests come, so the
		 * first request could be stuck in its pre-active queue for a long time.
		 */

		this.paxosState = new PaxosAcceptor(slotBallot!=null ? slotBallot.ballotnum : 0, 
				slotBallot!=null ? slotBallot.coordinator: getNextCoordinator(0, groupMembers), 
						slotBallot!=null ? slotBallot.slot+1 : 0,null); 
		if(slotBallot==null) TESTPaxosConfig.setRecovered(this.getNodeID(), pid, true);

		return true; // return value will be ignored 
	}
	private boolean hotRestore(HotRestoreInfo hri) {
		assert(this.paxosState==null && this.coordinator==null); // called from constructor only
		if(DEBUG) log.info("Node"+myID + " hot restoring with " + hri);
		this.coordinator = new PaxosCoordinator();
		this.coordinator.hotRestore(hri);
		this.paxosState = new PaxosAcceptor(hri.accBallot.ballotNumber,
				hri.accBallot.coordinatorID, hri.accSlot,hri);
		return true;
	}


	/* The one method for all message sending. 
	 * Protected coz the logger also calls this. 
	 */
	protected void sendMessagingTask(MessagingTask mtask) {
		if(mtask==null || mtask.isEmpty()) return; 
		if(this.paxosState!=null && this.paxosState.isStopped() && 
				mtask.msgs[0].getType()!=PaxosPacketType.DECISION &&
				mtask.msgs[0].getType()!=PaxosPacketType.CHECKPOINT_STATE) return;
		if(TESTPaxosConfig.isCrashed(myID)) return; 

		if(DEBUG) log.info(this.getNodeState() + " sending: " + mtask.toString());
		mtask.putPaxosIDVersion(this.getPaxosID(), this.getVersion());
		try {
			paxosManager.send(mtask);
		} catch (IOException ioe) {
			log.severe(this.getNodeState() +" encountered IOException while sending " + mtask);
			ioe.printStackTrace();
			/* We can't throw this exception upward because it will get sent all
			 * the way back up to PacketDemultiplexer whose incoming packet initiated
			 * this whole chain of events. It seems silly for PacketDemultiplexer
			 * to throw an IOException caused by the sends resulting from processing
			 * that packet. So we should handle this exception right here. But what
			 * should we do? We can ignore it as the network does not need to be 
			 * reliable anyway. Revisit as needed.
			 */
		} catch(JSONException je) {
			/* Same thing for other exceptions. Nothing useful to do here */
			log.severe(this.getNodeState()+" encountered JSONException while sending "+mtask);
			je.printStackTrace();
		}
	}
	private void sendMessagingTask(MessagingTask[] mtasks) throws JSONException {
		for(MessagingTask mtask : mtasks) this.sendMessagingTask(mtask);
	}
	protected void sendTestPaxosMessageToSelf() {
		if(POKE_ENABLED) {
			try {
				JSONObject msg = new JSONObject();
				msg.put(PaxosPacket.PAXOS_ID, this.getPaxosID());
				msg.put(PaxosPacket.PAXOS_VERSION, this.getVersion());
				msg.put(PaxosPacket.PAXOS_PACKET_TYPE, PaxosPacketType.NO_TYPE.getInt());
				if(!TESTPaxosConfig.MEMORY_TESTING) log.info(this.getNodeState() + 
						" sending test paxos message upon recovery");
				this.handlePaxosMessage(msg);
			} catch(JSONException je) {je.printStackTrace();}
		}
	}

	/* "Phase0"
	 * Event: Received a request from a client.
	 * 
	 * Action: Call handleProposal which will send the corresponding
	 * proposal to the current coordinator.
	 */
	private MessagingTask handleRequest(RequestPacket msg) throws JSONException {
		if(DEBUG) log.info(this.getNodeState() + " Phase0/CLIENT_REQUEST: " + msg);
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
		if(proposal.getForwardCount()==0) proposal.setReceiptTime(); // first receipt into the system
		if(this.coordinator.exists(this.paxosState.getBallot())) { // propose to all
			if(DEBUG) log.info(this.getNodeState() + " issuing Phase2a/ACCEPT after " + (System.currentTimeMillis() - 
					proposal.getCreateTime()) + " ms : " + (this.coordinator.isActive() ? 
							" sending accept for request ":" received pre-active request ") + proposal);
			AcceptPacket multicastAccept = null;
			proposal.addDebugInfo("a");
			multicastAccept = this.coordinator.propose(this.groupMembers, proposal); 
			mtask = multicastAccept!=null ? new MessagingTask(this.groupMembers, multicastAccept) : null; // multicast accept to all
		}
		else { // else unicast to current coordinator
			if(DEBUG) log.info(this.getNodeState()+" is not the coordinator, forwarding to " + 
					this.paxosState.getBallotCoord() + " : " + proposal);
			mtask = new MessagingTask(this.paxosState.getBallotCoord(), proposal.setForwarderID(myID)); // unicast to coordinator
			if(proposal.isPingPonging()) {
				log.warning(this.getNodeState() + " dropping ping-ponging proposal: " + proposal);
				mtask = this.checkRunForCoordinator(true);
			} else 	proposal.addDebugInfo("f");
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
		this.paxosManager.heardFrom(rcvdPrepare.coordinatorID); // FD optimization, not necessary
		PreparePacket prepare = rcvdPrepare.fixPreparePacketReceiver(this.myID); // coz receivers in multicast packets are meaningless
		PrepareReplyPacket prepareReply = this.paxosState.handlePrepare(prepare, this.getNodeState());  
		if(prepareReply==null) return null; // can happen only if acceptor is stopped
		if(prepare.isRecovery()) return null; // no need to get accepted pvalues from disk during recovery as networking is disabled anyway

		// we may also need to look into disk if ACCEPTED_PROPOSALS_ON_DISK is true
		if(PaxosAcceptor.ACCEPTED_PROPOSALS_ON_DISK) {
			prepareReply.accepted.putAll(this.paxosManager.getPaxosLogger().getLoggedAccepts(
					this.getPaxosID(), prepare.firstUndecidedSlot));
			for(PValuePacket pvalue : prepareReply.accepted.values()) {
				assert(this.paxosState.getBallot().compareTo(pvalue.ballot) >= 0) :
					this.getNodeState() + ":" + pvalue;
			}
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
		this.paxosManager.heardFrom(prepareReply.receiverID); // FD optimization, not necessary
		MessagingTask mtask=null;
		ArrayList<ProposalPacket> preActiveProposals=null;
		ArrayList<AcceptPacket> acceptList = null;

		if((preActiveProposals = this.coordinator.getPreActivesIfPreempted(prepareReply, this.groupMembers))!=null) {
			log.info(this.getNodeState()+" ("+this.coordinator.getBallotStr()+ ") election PREEMPTED by " + prepareReply.ballot);
			if(!preActiveProposals.isEmpty()) mtask = new MessagingTask(prepareReply.ballot.coordinatorID, 
					MessagingTask.toPaxosPacketArray(preActiveProposals.toArray()));
		}
		else if((acceptList = this.coordinator.handlePrepareReply(prepareReply, this.groupMembers))!=null && !acceptList.isEmpty()) {
			mtask = new MessagingTask(this.groupMembers, MessagingTask.toPaxosPacketArray(acceptList.toArray()));
			log.info(this.getNodeState()+" ("+this.coordinator.getBallotStr()+ ") elected coordinator; sending ACCEPTs for: " + mtask);
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
		if(ballot==null) {return null;}// can happen only if acceptor is stopped. 

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
	 * Event: Received a reply to an accept request, i.e. to 
	 * a request to accept a proposal from the coordinator.
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
			committedPValue.addDebugInfo("d"); 
			//this.handleCommittedRequest(committedPValue); // shouldn't inform even self without logging first
			multicastDecision = new MessagingTask(this.groupMembers, committedPValue); // inform everyone of the decision
		} else if (committedPValue.getType()==PaxosPacket.PaxosPacketType.PREEMPTED) { 
			/* Could drop the request, but we forward the preempted proposal as a no-op to 
			 * the new coordinator for testing purposes. The new(er) coordinator information 
			 * is within acceptReply. Note that our coordinator status may still be active 
			 * and it will be so until all of its requests have been preempted. Note also 
			 * that our local acceptor might still think we are the coordinator. The only 
			 * evidence of a new coordinator is in acceptReply that must have reported
			 * a higher ballot if we are here, hence the assert.
			 * 
			 * Warning: Can not forward the preempted request as-is to the new coordinator
			 * as this can result in multiple executions of a request. Although the 
			 * multiple executions will have different slot numbers and will not violate
			 * paxos safety, this is extremely undesirable for most applications. 
			 */
			assert(committedPValue.ballot.compareTo(acceptReply.ballot) < 0 || committedPValue.hasTakenTooLong()) : 
				(committedPValue +" >= " + acceptReply +", hasTakenTooLong="+committedPValue.hasTakenTooLong());
			if(!committedPValue.isNoop()) {
				// forward only if not already a no-op
				unicastPreempted = new MessagingTask(acceptReply.ballot.coordinatorID, committedPValue.makeNoop()); 
			}
			log.info(this.getNodeState() + " forwarding preempted request as no-op to node " + 
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
		if(!committed.isRecovery()) 
			AbstractPaxosLogger.logAndExecute(this.paxosManager.getPaxosLogger(), committed, this);
		else this.extractExecuteAndCheckpoint(committed);

		TESTPaxosConfig.commit(committed.requestID); 

		if(this.paxosState.getSlot() < committed.slot) if(DEBUG) log.info(this.getNodeState() + " expecting " + 
				this.paxosState.getSlot() + " recieved out-of-order commit: "+committed);

		return null; //this.fixLongDecisionGaps(committed);
	}
	private ActivePaxosState getActiveState(boolean active) {
		return this.paxosManager.getActiveState(active, getPaxosID());
	}
	private ActivePaxosState getActiveState() {return getActiveState(false);}
	// Invoked by handleCommittedRequest above ...
	private MessagingTask fixLongDecisionGaps(PValuePacket committed) {
		if(committed==null) return null;

		ActivePaxosState activeState = getActiveState(true);

		MessagingTask fixGapsRequest=null;
		if((committed.slot - this.paxosState.getSlot() > SYNC_THRESHOLD) && activeState.canSync()) {
			fixGapsRequest = this.requestMissingDecisions((committed.ballot.coordinatorID));
			if(fixGapsRequest!=null) {
				log.info(this.getNodeState() + " fixing gaps: " + fixGapsRequest);
				activeState.justSyncd(); // don't need to put into map here again
			}
		}
		return fixGapsRequest;
	}
	private boolean checkIfTrapped(JSONObject incoming, MessagingTask mtask) {
		if(this.isStopped()) {
			log.warning(this.getNodeState() + " DROPPING message trapped inside stopped " +
					"instance: " + incoming + " ; " + mtask);
			return true;
		}
		return false;
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
	protected synchronized MessagingTask extractExecuteAndCheckpoint(PValuePacket loggedDecision) {
		long methodEntryTime = System.currentTimeMillis(); 
		if(this.paxosState.isStopped()) return null;
		PValuePacket inorderDecision=null;
		int execCount = 0;
		// extract next in-order decision
		while((inorderDecision = this.paxosState.putAndRemoveNextExecutable(loggedDecision))!=null) { 
			if(DEBUG) log.info(this.getNodeState() + " in-order commit: "+inorderDecision); 
			if(inorderDecision.ballot.coordinatorID==this.myID && !inorderDecision.isRecovery()) 
				inorderDecision.setReplyToClient(true); // used only for testing

			// execute it until executed, we are *by design* stuck o/w; must be atomic with extraction
			boolean executed=false;
			String pid = this.getPaxosID();
			while(!executed) {
				executed = this.clientRequestHandler.handleDecision(pid, 
						inorderDecision.toString(), false);
				if(!executed) log.severe("App failed to execute request, retrying: "+inorderDecision);
			} execCount++;

			// checkpoint if needed, must be atomic with the execution 
			if(shouldCheckpoint(inorderDecision) && !inorderDecision.isRecovery() && !inorderDecision.isStopRequest()) { 
				AbstractPaxosLogger.checkpoint(this.paxosManager.getPaxosLogger(), pid, this.version, 
						this.groupMembers, inorderDecision.slot, this.paxosState.getBallot(), 
						this.clientRequestHandler.getState(pid), this.paxosState.getGCSlot());
				log.info(PaxosInstrumenter.getStats());
			}
			if(inorderDecision.isStopRequest()) this.paxosManager.kill(this);
		}
		this.paxosState.assertSlotInvariant();
		assert(loggedDecision==null || this.paxosState.getSlot()!=loggedDecision.slot); // otherwise it would've been executed
		if(loggedDecision!=null && !loggedDecision.isRecovery()) PaxosInstrumenter.update("EECNoSync", methodEntryTime, execCount);
		return this.fixLongDecisionGaps(loggedDecision);
	}
	// Like extractExecuteAndCheckpoint but invoked upon checkpoint transfer
	private synchronized MessagingTask handleCheckpoint(StatePacket statePacket) {
		if(statePacket.slotNumber > this.paxosState.getSlot()) {
			// update acceptor (like extract)
			this.paxosState.jumpSlot(statePacket.slotNumber+1);
			// put checkpoint in app (like execute)
			this.clientRequestHandler.updateState(getPaxosID(), statePacket.state);
			// put checkpoint in logger (like checkpoint)
			this.paxosManager.getPaxosLogger().putCheckpointState(this.getPaxosID(), this.version, groupMembers, 
					statePacket.slotNumber, statePacket.ballot, statePacket.state, this.paxosState.getGCSlot());
			log.info(this.getNodeState()+" inserted checkpoint through handleCheckpoint, next slot = " + this.paxosState.getSlot());
		}
		return extractExecuteAndCheckpoint(null); // coz otherwise we can get stuck as assertSlotInvariant() may not hold here
	}
	/* This method is called by PaxosManager.hibernate that blocks on the
	 * checkpoint operation to finish (unlike regular checkpoints that are 
	 * asynchronously handled by a helper thread). But hibernate is currently
	 * not really used as pause suffices. And PaxosManager methods are likely
	 * called by an executor task anyway, so blocking should be harmless.
	 */
	protected boolean tryForcedCheckpointAndStop() {
		boolean checkpointed=false;
		synchronized(this.paxosState) { synchronized(this.coordinator) { // Ugly nesting, not sure how else to do this correctly
			if(this.paxosState.caughtUp() && this.coordinator.caughtUp()) {
				String pid = this.getPaxosID();
				this.paxosManager.getPaxosLogger().putCheckpointState(pid, this.getVersion(), 
						this.groupMembers, this.paxosState.getSlot(), this.paxosState.getBallot(), 
						this.clientRequestHandler.getState(pid), this.paxosState.getGCSlot());
				checkpointed = true;
				log.info(this.getNodeState() + " forcing checkpoint at slot " + this.paxosState.getSlot() + 
						"; garbage collected accepts upto slot " + this.paxosState.getGCSlot() + "; max committed slot = " + 
						this.paxosState.getMaxCommittedSlot() + (this.paxosState.getBallotCoord()==myID ? 
								"; maxCommittedFrontier="+this.coordinator.getMajorityCommittedSlot() : ""));
				this.forceStop();
			}
		}}
		return checkpointed;
	}
	// Same as tryForcedCheckpointAndStop without checkpoint. **Don't call frivolously.**
	protected boolean tryPause() {
		boolean paused = false;
		synchronized(this.paxosState) { synchronized(this.coordinator) { // Ugly nesting, not sure how else to do this correctly
			if(this.paxosState.caughtUp() && this.coordinator.caughtUp()) {
				if(!TESTPaxosConfig.MEMORY_TESTING) log.info(this.getNodeState()+" caught up, about to force stop");
				HotRestoreInfo hri = new HotRestoreInfo(this.getPaxosID(), this.getVersion(), this.groupMembers, 
						this.paxosState.getSlot(), this.paxosState.getBallot(), this.paxosState.getGCSlot(),
						this.coordinator.getBallot(), this.coordinator.getNextProposalSlot(), 
						this.coordinator.getNodeSlots());
				PaxosAcceptor A = this.paxosState; PaxosCoordinator C = this.coordinator;
				this.forceStop();
				if(!TESTPaxosConfig.MEMORY_TESTING) log.info(this.getNodeState()+" pausing " + hri);
				paused = this.paxosManager.getPaxosLogger().pause(getPaxosID(), hri.toString());
				if(!paused) {this.paxosState = A; this.coordinator = C;} // revert back if pause failed
			}
		}}
		return paused;
	}

	private boolean shouldCheckpoint(PValuePacket decision) {
		return (decision.slot%INTER_CHECKPOINT_INTERVAL==0 || decision.isStopRequest());
	}
	/*************************** End of phase 3 methods ********************************/


	/********************** Start of failure detection and recovery methods *****************/

	/* Should be called regularly. Checks whether current ballot
	 * coordinator is alive. If not, it checks if it should try
	 * to be the nest coordinator and if so, it becomes the next
	 * coordinator. This method can be called any time safely 
	 * by any thread.
	 */
	private MessagingTask checkRunForCoordinator() {return this.checkRunForCoordinator(false);}
	private MessagingTask checkRunForCoordinator(boolean forceRun) {
		Ballot curBallot = this.paxosState.getBallot();
		MessagingTask multicastPrepare=null;

		/* 
		 * curBallot is my acceptor's ballot; "my acceptor's coordinator" is that ballot's coordinator.
		 * 
		 *  If I am not already a coordinator at least as high as my acceptor's ballot's coordinator
		 *  AND
		 *  I didn't run too recently
		 *  AND
		 *  (I am my acceptor's coordinator 
		 *      OR (my acceptor's coordinator is dead 
		 *         AND 
		 *         (I am next in line OR the current coordinator has been dead for a really long time)
		 *         )
		 *   )
		 *   OR forceRun
		 */
		if((!this.coordinator.exists(curBallot) && !this.coordinator.ranRecently() && 
				(curBallot.coordinatorID==this.myID // can happen during recovery
				|| 
				(!this.paxosManager.isNodeUp(curBallot.coordinatorID) 
						&& 
						(this.myID==getNextCoordinator(curBallot.ballotNumber+1, this.groupMembers) 
						|| 
						paxosManager.lastCoordinatorLongDead(curBallot.coordinatorID)))))
						|| forceRun) 
		{ 
			/* We normally round-robin across nodes for electing coordinators, e.g., 
			 * node 7 will try to become coordinator in ballotnum such that ballotnum%7==0
			 * if it suspects that the current coordinator is dead. But it is more robust 
			 * to check if it has been a long time since we heard anything from the current 
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
			log.info(getNodeState() + " decides to run for coordinator as node " + curBallot.coordinatorID + 
					(curBallot.coordinatorID!=myID ? " appears to be dead" : " has not yet initialized its coordinator"));
			Ballot newBallot = new Ballot(curBallot.ballotNumber+1, this.myID); 
			if(this.coordinator.makeCoordinator(newBallot.ballotNumber, newBallot.coordinatorID, 
					this.groupMembers, this.paxosState.getSlot(), false)!=null) {
				multicastPrepare = new MessagingTask(this.groupMembers, new PreparePacket(this.myID, this.myID, newBallot));
			}
		} else if(this.coordinator.waitingTooLong()) { // just "re-run" by resending  prepare
			TESTPaxosConfig.testAssert(false);
			log.warning(this.getNodeState() + " resending timed out PREPARE; this is only needed under high congestion");
			Ballot newBallot = this.coordinator.remakeCoordinator(groupMembers);
			if(newBallot!=null) multicastPrepare = new MessagingTask(this.groupMembers, 
					new PreparePacket(this.myID, this.myID, newBallot));
		} else if(!this.paxosManager.isNodeUp(curBallot.coordinatorID) && !this.coordinator.exists(curBallot)) { // not my job
			log.info(getNodeState() + " thinks current coordinator " + curBallot.coordinatorID + " is"+
					(paxosManager.lastCoordinatorLongDead(curBallot.coordinatorID)?" *long* ":" ") +
					"dead, the next-in-line is " + getNextCoordinator(curBallot.ballotNumber+1, this.groupMembers) + 
					(this.coordinator.ranRecently() ? ", and I ran too recently to try again":""));
		}
		return multicastPrepare;
	}
	private String getBallots() {return "[C:("+(this.coordinator!=null ? this.coordinator.getBallotStr():"null")+
			"), A:("+(this.paxosState!=null ? this.paxosState.getBallotSlot():"null")+")]";}
	private String getNodeState() {return "Node "+this.myID+ " "+ this.getBallots() + ", " + this.getPaxosID();}

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
		return roundRobinCoordinator(ballotnum); 
	}
	private int roundRobinCoordinator(int ballotnum) {
		return this.getMembers()[ballotnum % this.getMembers().length];
	}

	// Resend long-waiting accepts. Otherwise, the machine can get stuck.
	private MessagingTask pokeLocalCoordinator() {
		AcceptPacket accept = this.coordinator.reissueAcceptIfWaitingTooLong(this.paxosState.getSlot());
		TESTPaxosConfig.testAssert(accept==null); // Just to see if this happens ever
		if(accept!=null) log.info(this.getNodeState() + " resending timed out ACCEPT " + accept);
		MessagingTask reAccept = (accept!=null ? new MessagingTask(this.groupMembers, accept) : null);
		return reAccept;
	}

	/* Event: Received or locally generated a sync request.
	 * Action: Send a sync reply containing missing committed requests to the requester.
	 * If the requester is myself, multicast to all.
	 */
	private MessagingTask requestMissingDecisions(int coordinatorID) {
		ArrayList<Integer> missingSlotNumbers = this.paxosState.getMissingCommittedSlots(MAX_SYNC_GAP);
		if(missingSlotNumbers==null || missingSlotNumbers.isEmpty()) return null;

		int maxDecision = this.paxosState.getMaxCommittedSlot();
		SyncDecisionsPacket srp =  new SyncDecisionsPacket(this.myID,maxDecision, missingSlotNumbers, 
				maxDecision-this.paxosState.getSlot() >= MAX_SYNC_GAP);

		MessagingTask mtask = coordinatorID!=this.myID ? new MessagingTask(coordinatorID, srp) :
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
	private MessagingTask handleSyncDecisionsPacket(SyncDecisionsPacket syncReply) throws JSONException {
		int minMissingSlot = syncReply.missingSlotNumbers.get(0); 
		if(this.paxosState.getSlot() <= minMissingSlot) return null; // I am worse than you
		else if (minMissingSlot < this.lastCheckpointSlot()) return handleCheckpointRequest(syncReply); //sync reply = checkpoint request.

		// Else get decisions from database (as we are unlikely to have most of them in memory)
		ArrayList<PValuePacket> missingDecisions = 
				this.paxosManager.getPaxosLogger().getLoggedDecisions(this.getPaxosID(), minMissingSlot, syncReply.maxDecisionSlot);
		for(Iterator<PValuePacket> pvalueIterator = missingDecisions.iterator(); pvalueIterator.hasNext();) {
			PValuePacket pvalue = pvalueIterator.next();
			if(!syncReply.missingSlotNumbers.contains(pvalue.slot)) pvalueIterator.remove(); // filter non-missing
			assert(!pvalue.isRecovery());
		}
		MessagingTask unicasts = missingDecisions.isEmpty() ? null : new MessagingTask(syncReply.nodeID, 
				MessagingTask.toPaxosPacketArray(missingDecisions.toArray()));
		log.info(this.getNodeState() + " sending missing decisions to node " + syncReply.nodeID +": " + unicasts);
		return unicasts;
	}
	private int lastCheckpointSlot() {return this.paxosState.getSlot() - (this.paxosState.getSlot()%INTER_CHECKPOINT_INTERVAL);}

	/* Event: Received a request for a recent checkpoint presumably
	 * from a replica that has recovered after a long down time. 
	 * Action: Send checkpoint to requester.
	 */
	private MessagingTask handleCheckpointRequest(SyncDecisionsPacket syncReply) {
		/* The assertion below does not mean that the state we actually get will be 
		 * at lastCheckpointSlot() or higher because, even though getSlot() has gotten
		 * updated, the checkpoint to disk may not yet have finished. We have no way 
		 * of knowing other than reading the disk. So we first do a read to check if
		 * the checkpointSlot is at least higher than the minMissingSlot in syncReply.
		 * If the state is tiny, this will double the state fetching overhead as we 
		 * are doing two database reads.
		 */
		assert(syncReply.missingSlotNumbers.get(0) < this.lastCheckpointSlot());
		if(this.paxosState.getSlot()==0) log.warning(this.getNodeState() + (!this.coordinator.exists() ? "[acceptor]" : 
			this.coordinator.isActive() ? "[coordinator]" : "[preactive-coordinator]" ) + " has no state (yet) for " + syncReply);
		int checkpointSlot = this.paxosManager.getPaxosLogger().getCheckpointSlot(getPaxosID());
		StatePacket statePacket = (checkpointSlot>=syncReply.missingSlotNumbers.get(0) ? 
				this.paxosManager.getPaxosLogger().getStatePacket(this.getPaxosID()) : null);
		if(statePacket!=null) log.info(this.getNodeState() + " sending checkpoint to node " + syncReply.nodeID +": " + statePacket);
		return statePacket!=null ? new MessagingTask(syncReply.nodeID, statePacket) : null;
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
		this.paxosState = new PaxosAcceptor(0, this.groupMembers[0],initSlot,null);
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
