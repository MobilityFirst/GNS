package edu.umass.cs.gigapaxos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxospackets.AcceptPacket;
import edu.umass.cs.gigapaxos.paxospackets.AcceptReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PreparePacket;
import edu.umass.cs.gigapaxos.paxospackets.PrepareReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.ProposalPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxospackets.StatePacket;
import edu.umass.cs.gigapaxos.paxospackets.SyncDecisionsPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gigapaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.MessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.PaxosInstanceCreationException;
import edu.umass.cs.gigapaxos.paxosutil.RequestInstrumenter;
import edu.umass.cs.gigapaxos.paxosutil.SlotBallotState;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Keyable;
import edu.umass.cs.utils.MyLogger;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.DelayProfiler;

/**
 * @author V. Arun
 * 
 *         This class is the top-level paxos class per instance or paxos group
 *         on a machine. This class is "protected" as the only way to use it
 *         will be through the corresponding PaxosManager even if there is just
 *         one paxos application running on the machine.
 *         <p>
 * 
 *         This class delegates much of the interesting paxos actions to
 *         PaxosAcceptorState and PaxosCoordinator. It delegates all messaging
 *         to PaxosManager's PaxosMessenger. It is "managed", i.e., its paxos
 *         group is created and its incoming packets are demultiplexed, by its
 *         PaxosManager. It's logging is handled by an implementation of
 *         AbstractPaxosLogger.
 *         <p>
 * 
 *         The high-level organization is best reflected in handlePaxosMessage,
 *         a method that delegates processing to the acceptor or coordinator and
 *         gets back a messaging task, e.g., receiving a prepare message will
 *         probably result in a prepare-reply messaging task, and so on.
 *         <p>
 * 
 *         Space: An inactive PaxosInstanceStateMachine, i.e., whose
 *         corresponding application is currently not processing any requests,
 *         uses ~225B *total*. Here is the breakdown: PaxosInstanceStateMachine
 *         final fields: ~80B PaxosAcceptor: ~90B PaxosCoordinatorState: ~60B
 *         Even in an inactive paxos instance, the total *total* space is much
 *         more because of PaxosManager (that internally uses FailureDetection)
 *         etc., but all that state is not incurred per paxos application, just
 *         per machine. Thus, if we have S=10 machines and N=10M applications
 *         each using paxos with K=10 replicas one each at each machine, each
 *         machine has 10M PaxosInstanceStateMachine instances that will use
 *         about 2.25GB (10M*225B). The amount of space used by PaxosManager and
 *         others is small and depends only on S, not N or K.
 *         <p>
 * 
 *         When actively processing requests, the total space per paxos instance
 *         can easily go up to thousands of bytes. But we are unlikely to be
 *         processing requests across even hundreds of thousands of different
 *         applications simultaneously if each request finishes executing in
 *         under a second. For example, if a single server's execution
 *         throughput is 10K requests/sec and each request takes 100ms to finish
 *         executing (including paxos coordination), then the number of active
 *         *requests* at a machine is on average ~100K. The number of active
 *         paxos instances at that machine is at most the number of active
 *         requests at that machine.
 * 
 */
public class PaxosInstanceStateMachine implements Keyable<String> {
	/*
	 * If false, the paxosID is represented as a byte[], so we must invoke
	 * getPaxosID() as infrequently as possible.
	 */
	private static final boolean PAXOS_ID_AS_STRING = false;

	// must be >= 1, does not depend on anything else
	protected static final int INTER_CHECKPOINT_INTERVAL = 100;

	// out-of-order-ness prompting synchronization, must be >=1
	protected static final int SYNC_THRESHOLD = 4 * INTER_CHECKPOINT_INTERVAL;

	// max decisions gap when reached will prompt checkpoint transfer
	protected static final int MAX_SYNC_DECISIONS_GAP = INTER_CHECKPOINT_INTERVAL;

	// used by PaxosCoordinatorState to determine if overloaded
	protected static final int MAX_OUTSTANDING_LOAD = 100 * INTER_CHECKPOINT_INTERVAL;

	// poke instance in the beginning to prompt coordinator election
	protected static final boolean POKE_ENABLED = true;

	// minimum interval before another sync decisions request can be issued
	protected static final long MIN_RESYNC_DELAY = 1000;

	private static enum SyncMode {
		DEFAULT_SYNC, FORCE_SYNC, SYNC_TO_PAUSE
	};

	/*
	 * Enabling this will slow down instance creation for null initialState as
	 * an initial checkpoint will still be made. It will make no difference if
	 * initialState is non-null as checkpointing non-null initial state is
	 * necessary for safety.
	 * 
	 * The default setting must be true. Not allowing null checkpoints can cause
	 * reconfiguration to stall as there is no way for the new epoch to
	 * distinguish between no previous epoch final state and null previous epoch
	 * final state.
	 */
	protected static final boolean ENABLE_NULL_CHECKPOINT_STATE = true;

	/************ final Paxos state that is unchangeable after creation ***************/
	private final int[] groupMembers;
	// Object to allow easy testing across byte[] and String
	private final Object paxosID;
	private final int version;
	private final PaxosManager<?> paxosManager;
	private final InterfaceReplicable clientRequestHandler;

	/************ Non-final paxos state that is changeable after creation *******************/
	// uses ~125B of empty space when not actively processing requests
	private PaxosAcceptor paxosState = null;
	// uses just a single pointer's worth of space unless I am a coordinator
	private PaxosCoordinator coordinator = null;
	/************ End of non-final paxos state ***********************************************/

	// static, so does not count towards space.
	private static Logger log = Logger
			.getLogger(PaxosInstanceStateMachine.class.getName());

	// FIXME: id is not needed as we can get that from paxosManager
	PaxosInstanceStateMachine(String groupId, int version, int id,
			Set<Integer> gms, InterfaceReplicable app, String initialState,
			PaxosManager<?> pm, HotRestoreInfo hri, boolean missedBirthing) {

		/*
		 * Final assignments: A paxos instance is born with a paxosID, version
		 * this instance's node ID, the application request handler, the paxos
		 * manager, and the group members.
		 */
		this.paxosID = PAXOS_ID_AS_STRING ? groupId : groupId.getBytes();
		this.version = version;
		this.clientRequestHandler = app;
		this.paxosManager = pm;
		assert (gms != null && gms.size() > 0);
		Arrays.sort(this.groupMembers = Util.setToIntArray(gms));
		/**************** End of final assignments *******************/

		/*
		 * All non-final state is store in PaxosInstanceState (for acceptors) or
		 * in PaxosCoordinatorState (for coordinators) that inherits from
		 * PaxosInstanceState.
		 */
		if (pm != null && hri == null) {
			initiateRecovery(initialState, missedBirthing);
		} else if (hri != null && initialState == null)
			hotRestore(hri);
		else
			testingNoRecovery(); // used only for testing size
		assert (hri == null || initialState == null) : "Can not specify initial state for existing, paused paxos instance";
		incrInstanceCount(); // for instrumentation

		// log creation only if the number of instances is small
		log.log((hri == null && notManyInstances()) ? Level.INFO : Level.FINER,
				"Node{0} initialized paxos {1} {2} with members {3}; {4} {5} {6}{7}",
				new Object[] {
						this.getNodeID(),
						(this.paxosState.getBallotCoordLog() == this.getMyID() ? "COORDINATOR"
								: "instance"),
						this.getPaxosIDVersion(),
						Util.arrayOfIntToString(groupMembers),
						this.paxosState,
						this.coordinator,
						(initialState == null ? "{recoveredState=["
								+ Util.prefix(this.getCheckpointState(), 64)
								: "{initialState=[" + initialState), "]}" });
	}

	/**
	 * @return Version or epoch number corresponding to this reconfigurable
	 *         paxos instance.
	 */
	protected int getVersion() {
		return this.version;
	}

	// one of only two public methods
	public String getKey() {
		return this.getPaxosID();
	}

	public String toString() {
		return this.getNodeState();
	}

	protected String toStringLong() {
		return this.getNodeState() + this.paxosState + this.coordinator;
	}

	/**
	 * @return Paxos instance name concatenated with the version number.
	 */
	protected String getPaxosIDVersion() {
		return this.getPaxosID() + ":" + this.getVersion();
	}

	protected String getPaxosID() {
		return (paxosID instanceof String ? (String) paxosID : new String(
				(byte[]) paxosID));
	}

	protected int[] getMembers() {
		return this.groupMembers;
	}

	protected String getNodeID() {
		return this.paxosManager != null ? this.paxosManager.intToString(this
				.getMyID()) : "" + getMyID();
	}

	protected InterfaceReplicable getApp() {
		return this.clientRequestHandler;
	}

	protected PaxosManager<?> getPaxosManager() {
		return this.paxosManager;
	}

	protected int getMyID() {
		return (this.paxosManager != null ? this.paxosManager.getMyID() : -1);
	}

	/**
	 * isStopped()==true means that this paxos instance is dead and completely
	 * harmless (even if the underlying object has not been garbage collected by
	 * the JVM. In particular, it can NOT make the app execute requests or send
	 * out paxos messages to the external world.
	 * 
	 * @return Whether this paxos instance has been stopped.
	 */
	protected boolean isStopped() {
		return this.paxosState.isStopped();
	}

	/**
	 * Forces a synchronization wait. PaxosManager needs this to ensure that an
	 * ongoing stop is fully executed.
	 * 
	 * @return True.
	 */
	protected synchronized boolean synchronizedNoop() {
		return true;
	}

	// not synchronized as coordinator can die anytime anyway
	protected boolean forceStop() {
		if (!this.paxosState.isStopped())
			decrInstanceCount(); // for instrumentation
		this.coordinator.forceStop();
		this.paxosState.forceStop(); //
		return true;
	}

	private boolean nullCheckpointStateEnabled() {
		return this.paxosManager.isNullCheckpointStateEnabled();
	}

	// removes all database and app state and can not be recovered anymore
	protected boolean kill(boolean clean) {
		// paxosState must be typically already stopped here
		this.forceStop();
		if (clean // clean kill implies reset app state
				&& this.updateState(this.getPaxosID(), null)
				// and remove database state
				&& AbstractPaxosLogger.kill(this.paxosManager.getPaxosLogger(),
						getPaxosID(), this.getVersion()))
			// paxos instance is "lost" now
			log.log(Level.INFO, "Paxos instance {0} cleanly terminated.",
					new Object[] { this });
		else
			// unclean "crash"
			log.severe(this
					+ " crashing paxos instance "
					+ getPaxosIDVersion()
					+ " likely because of an error while executing an application request. "
					+ "A paxos instance for "
					+ getPaxosIDVersion()
					+ " or a higher version must either be explicitly (re-)created "
					+ "or this \"crashed\" instance will recover safely upon a reboot.");
		return true;
	}

	private boolean updateState(String paxosID, String state) {
		for (int i = 0; !this.clientRequestHandler.updateState(getPaxosID(),
				null); i++)
			if (waitRetry(RETRY_TIMEOUT) && i < RETRY_LIMIT)
				log.warning(this
						+ " unable to delete application state; retrying");
			else
				throw new RuntimeException("Node" + getNodeID()
						+ " unable to delete " + this.getPaxosIDVersion());
		return true;
	}

	private static final long RETRY_TIMEOUT = 100;
	private static final long RETRY_LIMIT = 10;

	private static boolean waitRetry(long timeout) {
		try {
			Thread.sleep(timeout);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}

	protected void setActive() {
		this.paxosState.setActive();
	}

	protected boolean isActive() {
		return this.paxosState.isActive();
	}

	private String getCheckpointState() {
		SlotBallotState sbs = this.paxosManager != null ? this.paxosManager
				.getPaxosLogger()
				.getSlotBallotState(getPaxosID(), getVersion()) : null;
		return sbs != null ? sbs.state : null;
	}

	protected void handlePaxosMessage(JSONObject msg) throws JSONException {
		long methodEntryTime = System.currentTimeMillis();
		/*
		 * Note: Because incoming messages may be handled concurrently, some
		 * messages may continue to get processed for a little while after a
		 * stop has been executed and even after isStopped() is true (because
		 * isStopped() was false when those messages came in here). But that is
		 * okay coz these messages can not spawn unsafe outgoing messages (as
		 * messaging is turned off for all but DECISION or CHECKPOINT_STATE
		 * packets) and can not change any disk state.
		 */
		if (this.paxosState.isStopped())
			return;
		// every packet here must have a version, so we don't check has()
		if ((msg.getInt(PaxosPacket.PAXOS_VERSION)) != this.getVersion())
			return;
		if (TESTPaxosConfig.isCrashed(this.getMyID()))
			return; // Tester says I have crashed
		// recovery means we won't send any replies
		boolean recovery = PaxosPacket.isRecovery(msg);
		/*
		 * The reason we should not process regular messages until this instance
		 * has rolled forward is that it might respond to a prepare with a list
		 * of accepts fetched from disk that may be inconsistent with its
		 * acceptor state.
		 */
		if (!this.paxosManager.hasRecovered(this) && !recovery)
			return; // only process recovery message during rollForward

		PaxosPacketType msgType = PaxosPacket.getPaxosPacketType(msg);
		log.log(Level.FINEST, "{0}{1}{2}{3}{4}", new Object[] { this,
				" received ", msgType, ": ", msg });

		boolean isPoke = msgType.equals(PaxosPacketType.NO_TYPE);
		if (!isPoke)
			this.justActive();
		else
			log.log(Level.FINER,
					"{0} received a no_type poke {1};",
					new Object[] { this,
							msg.getString(PaxosPacket.SYNC_MODE.toString()) });

		MessagingTask[] mtasks = new MessagingTask[2];
		/*
		 * Check for coordinator'ing upon *every* message except poke messages.
		 * Pokes are primarily for sync'ing decisions and could be also used to
		 * resend accepts. There is little reason to send prepares proactively
		 * if no new activity is happening.
		 */
		mtasks[0] = (!recovery ?
		// check run for coordinator if not active
		(!this.coordinator.isActive()
		// ignore pokes unless not caught up
		&& (!isPoke || !this.coordinator.caughtUp()
		// &&
		// SyncMode.SYNC_TO_PAUSE.equals(SyncMode.valueOf(msg.getString(PaxosPacket.SYNC_MODE.toString())))
		)) ? checkRunForCoordinator()
		// else reissue long waiting accepts
				: this.pokeLocalCoordinator()
				// neither during recovery
				: null);

		MessagingTask mtask = null;
		switch (msgType) {
		case REQUEST:
			mtask = handleRequest(new RequestPacket(msg));
			// send RequestPacket to current coordinator
			break;
		// replica --> coordinator
		case PROPOSAL:
			mtask = handleProposal(new ProposalPacket(msg));
			// either send ProposalPacket to current coordinator or send
			// AcceptPacket to all
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
			// sync if needed on poke
			mtasks[0] = (mtasks[0] != null) ? mtasks[0] : this
					.fixLongDecisionGaps(null, SyncMode.valueOf(msg
							.getString(PaxosPacket.SYNC_MODE)));

			break;
		default:
			assert (false) : "Paxos instance received an unrecognizable packet: "
					+ msg;
		}
		mtasks[1] = mtask;

		DelayProfiler.updateDelay("handlePaxosMessage", methodEntryTime);

		this.checkIfTrapped(msg, mtasks[1]); // just to print a warning
		if (!recovery) {
			this.sendMessagingTask(mtasks);
		}
	}

	/************** Start of private methods ****************/

	/*
	 * Invoked both when a paxos instance is first created and when it recovers
	 * after a crash. It is all the same as far as the paxos instance is
	 * concerned (provided we ensure that the app state after executing the
	 * first request (slot 0) is checkpointed, which we do).
	 */
	private boolean initiateRecovery(String initialState, boolean missedBirthing) {
		String pid = this.getPaxosID();
		// only place where version is checked
		SlotBallotState slotBallot = this.paxosManager.getPaxosLogger()
				.getSlotBallotState(pid, this.getVersion());
		if (slotBallot != null)
			log.log(Level.FINE, "{0}{1}{2}", new Object[] { this,
					" recovered state: ",
					(slotBallot != null ? slotBallot.state : "NULL") });

		// update app state
		if (slotBallot != null && slotBallot.state != null)
			if (!this.clientRequestHandler.updateState(pid, slotBallot.state))
				throw new PaxosInstanceCreationException(
						"Unable to update app state with " + slotBallot.state);

		this.coordinator = new PaxosCoordinator(); // just a shell class
		// initial coordinator is assumed, not prepared
		if (slotBallot == null && roundRobinCoordinator(0) == this.getMyID())
			this.coordinator.makeCoordinator(0, this.getMyID(), getMembers(),
					(initialState != null || nullCheckpointStateEnabled() ? 1
							: 0), true); // slotBallot==null
		/*
		 * Note: We don't have to create coordinator state here. It will get
		 * created if needed when the first external (non-recovery) packet is
		 * received. But we create the very first coordinator here as otherwise
		 * it is possible that no coordinator gets elected as follows: the
		 * lowest ID node wakes up and either upon an external or self-poke
		 * message sends a prepare, but gets no responses because no other node
		 * is up yet. In this case, the other nodes when they boot up will not
		 * run for coordinator, and the lowest ID node will not resend its
		 * prepare if no more requests come, so the first request could be stuck
		 * in its pre-active queue for a long time.
		 */

		// allow null state without null checkpoints just for memory testing
		if (slotBallot == null && initialState == null
				&& !this.paxosManager.isNullCheckpointStateEnabled()
				&& !TESTPaxosConfig.isMemoryTesting())
			throw new PaxosInstanceCreationException(
					"A paxos instance with null initial state can be"
							+ " created only if null checkpoints are enabled");

		/*
		 * If this is a "missed-birthing" instance creation, we still set the
		 * acceptor nextSlot to 0 but don't checkpoint initialState. In fact,
		 * initialState better be null here in that case as we can't possibly
		 * have an initialState with missed birthing.
		 */
		assert (!(missedBirthing && initialState != null));
		/*
		 * If it is possible for there to be no initial state checkpoint, under
		 * missed birthing, an acceptor may incorrectly report its gcSlot as -1,
		 * and if a majority do so (because that majority consists all of missed
		 * birthers), a coordinator may propose a proposal for slot 0 even
		 * though an initial state does exist, which would end up overwriting
		 * the initial state. So we can not support ambiguity in whether there
		 * is initial state or not. If we force initial state checkpoints (even
		 * null state checkpoints) to always exist, missed birthers can always
		 * set the initial gcSlot to 0. The exception and assert above imply the
		 * assertion below.
		 */
		assert (!missedBirthing || this.paxosManager
				.isNullCheckpointStateEnabled());

		this.paxosState = new PaxosAcceptor(
				slotBallot != null ? slotBallot.ballotnum : 0,
				slotBallot != null ? slotBallot.coordinator : this
						.roundRobinCoordinator(0),
				slotBallot != null ? (slotBallot.slot + 1) : 0, null);
		if (slotBallot == null && !missedBirthing)
			this.putInitialState(initialState);
		if (missedBirthing)
			this.paxosState.setGCSlotAfterPuttingInitialSlot();

		if (slotBallot == null)
			TESTPaxosConfig.setRecovered(this.getMyID(), pid, true);

		return true; // return value will be ignored
	}

	private boolean hotRestore(HotRestoreInfo hri) {
		// called from constructor only, hence assert
		assert (this.paxosState == null && this.coordinator == null);
		log.log(Level.FINE, "{0}{1}{2}{3}", new Object[] { this,
				" hot restoring with ", hri });
		this.coordinator = new PaxosCoordinator();
		this.coordinator.hotRestore(hri);
		this.paxosState = new PaxosAcceptor(hri.accBallot.ballotNumber,
				hri.accBallot.coordinatorID, hri.accSlot, hri);
		this.paxosState.setActive(); // no recovery
		return true;
	}

	private boolean putInitialState(String initialState) {
		if (this.getPaxosManager() == null
				|| (initialState == null && !nullCheckpointStateEnabled()))
			return false;
		this.handleCheckpoint(new StatePacket(initialBallot(), 0, initialState));
		this.paxosState.setGCSlotAfterPuttingInitialSlot();
		return true;
	}

	private Ballot initialBallot() {
		return new Ballot(0, this.roundRobinCoordinator(0));
	}

	/*
	 * The one method for all message sending. Protected coz the logger also
	 * calls this.
	 */
	protected void sendMessagingTask(MessagingTask mtask) {
		if (mtask == null || mtask.isEmpty())
			return;
		if (this.paxosState != null
				&& this.paxosState.isStopped()
				&& !mtask.msgs[0].getType().equals(PaxosPacketType.DECISION)
				&& !mtask.msgs[0].getType().equals(
						PaxosPacketType.CHECKPOINT_STATE))
			return;
		if (TESTPaxosConfig.isCrashed(this.getMyID()))
			return;

		log.log(Level.FINEST, "{0}{1}{2}", new Object[] { this, " sending: ",
				mtask });
		mtask.putPaxosIDVersion(this.getPaxosID(), this.getVersion());
		try {
			// assert(this.paxosState.isActive());
			paxosManager.send(mtask);
		} catch (IOException ioe) {
			log.severe(this + " encountered IOException while sending " + mtask);
			ioe.printStackTrace();
			/*
			 * We can't throw this exception upward because it will get sent all
			 * the way back up to PacketDemultiplexer whose incoming packet
			 * initiated this whole chain of events. It seems silly for
			 * PacketDemultiplexer to throw an IOException caused by the sends
			 * resulting from processing that packet. So we should handle this
			 * exception right here. But what should we do? We can ignore it as
			 * the network does not need to be reliable anyway. Revisit as
			 * needed.
			 */
		} catch (JSONException je) {
			/* Same thing for other exceptions. Nothing useful to do here */
			log.severe(this + " encountered JSONException while sending "
					+ mtask);
			je.printStackTrace();
		}
	}

	private void sendMessagingTask(MessagingTask[] mtasks) throws JSONException {
		for (MessagingTask mtask : mtasks)
			this.sendMessagingTask(mtask);
	}

	// will send a noop message to self to force event-driven actions
	protected void poke(boolean forceSync) {
		if (POKE_ENABLED) {
			try {
				JSONObject msg = new JSONObject();
				msg.put(PaxosPacket.PAXOS_ID, this.getPaxosID());
				msg.put(PaxosPacket.PAXOS_VERSION, this.getVersion());
				msg.put(PaxosPacket.PAXOS_PACKET_TYPE,
						PaxosPacketType.NO_TYPE.getInt());
				msg.put(PaxosPacket.SYNC_MODE,
						forceSync ? SyncMode.FORCE_SYNC.toString()
								: SyncMode.SYNC_TO_PAUSE.toString());
				log.log(Level.FINE, "{0} being poked", new Object[] { this });
				this.handlePaxosMessage(msg);
			} catch (JSONException je) {
				je.printStackTrace();
			}
		}
	}

	/*
	 * "Phase0" Event: Received a request from a client.
	 * 
	 * Action: Call handleProposal which will send the corresponding proposal to
	 * the current coordinator.
	 */
	private MessagingTask handleRequest(RequestPacket msg) throws JSONException {
		log.log(Level.FINE, "{0}{1}{2}", new Object[] { this,
				" Phase0/CLIENT_REQUEST: ", msg.getSummary() });
		RequestInstrumenter.received(msg, msg.clientID, this.getMyID());
		msg.setEntryReplica(getMyID());
		return handleProposal(new ProposalPacket(0, msg));
	}

	/*
	 * "Phase0"->Phase2a: Event: Received a proposal [request, slot] from any
	 * node.
	 * 
	 * Action: If a non-coordinator node receives a proposal, send to the
	 * coordinator. Otherwise, propose it to acceptors with a good slot number
	 * (thereby initiating phase2a for this request).
	 * 
	 * Return: A send either to a coordinator of the proposal or to all replicas
	 * of the proposal with a good slot number.
	 */
	private MessagingTask handleProposal(ProposalPacket proposal)
			throws JSONException {
		MessagingTask mtask = null; // could be multicast or unicast to
									// coordinator.
		assert (proposal.getEntryReplica() != -1);
		if (proposal.getForwardCount() == 0)
			proposal.setReceiptTime(); // first receipt into the system
		else
			RequestInstrumenter.received(proposal, proposal.getForwarderID(),
					this.getMyID());
		if (this.coordinator.exists(this.paxosState.getBallot())) {
			// multicast ACCEPT to all
			AcceptPacket multicastAccept = null;
			proposal.addDebugInfo("a");
			multicastAccept = this.coordinator.propose(this.groupMembers,
					proposal);
			mtask = multicastAccept != null ? new MessagingTask(
					this.groupMembers, multicastAccept) : null; // multicast
			if (multicastAccept != null) {
				RequestInstrumenter.sent(multicastAccept, this.getMyID(), -1);
				log.log(Level.FINER,
						"{0} issuing (after {2} ms) {3} ",
						new Object[] {
								this,
								(System.currentTimeMillis() - proposal
										.getCreateTime()),
								multicastAccept.getSummary() });

			}
		} else { // else unicast to current coordinator
			log.log(Level.FINER, "{0}{1}{2}{3}{4}",
					new Object[] { this,
							" is not the coordinator, forwarding to ",
							this.paxosState.getBallotCoordLog(), " : ",
							proposal.getSummary() });
			mtask = new MessagingTask(this.paxosState.getBallotCoord(),
					proposal.setForwarderID(this.getMyID())); // unicast
			if (proposal.isPingPonging()) {
				log.warning(this + " dropping ping-ponging proposal: "
						+ proposal.getSummary());
				mtask = this.checkRunForCoordinator(true);
			} else
				proposal.addDebugInfo("f");
		}
		return mtask;
	}

	/*
	 * Phase1a Event: Received a prepare request for a ballot, i.e. that
	 * ballot's coordinator is acquiring proposing rights for all slot numbers
	 * (lowest uncommitted up to infinity)
	 * 
	 * Action: This node needs to check if it has accepted a higher numbered
	 * ballot already and if not, it can accept this ballot, thereby promising
	 * not to accept any lower ballots.
	 * 
	 * Return: Send prepare reply with proposal values previously accepted to
	 * the sender (the received ballot's coordinator).
	 */
	private MessagingTask handlePrepare(PreparePacket prepare)
			throws JSONException {
		paxosManager.heardFrom(prepare.ballot.coordinatorID); // FD optimization

		Ballot prevBallot = this.paxosState.getBallot();
		PrepareReplyPacket prepareReply = this.paxosState.handlePrepare(
				prepare, this.paxosManager.getMyID());
		if (prepareReply == null)
			return null; // can happen only if acceptor is stopped
		if (prepare.isRecovery())
			return null; // no need to get accepted pvalues from disk during
							// recovery as networking is disabled anyway

		// may also need to look into disk if ACCEPTED_PROPOSALS_ON_DISK is true
		if (PaxosAcceptor.GET_ACCEPTED_PVALUES_FROM_DISK
		// no need to gather pvalues if NACKing anyway
				&& prepareReply.ballot.compareTo(prepare.ballot) == 0)
			prepareReply.accepted.putAll(this.paxosManager.getPaxosLogger()
					.getLoggedAccepts(this.getPaxosID(), this.getVersion(),
							prepare.firstUndecidedSlot));

		for (PValuePacket pvalue : prepareReply.accepted.values())
			// if I accepted a pvalue, my acceptor ballot must reflect it
			assert (this.paxosState.getBallot().compareTo(pvalue.ballot) >= 0) : this
					+ ":" + pvalue;

		log.log(Level.FINE,
				"{0} {1} {2} with {3}",
				new Object[] {
						this,
						prepareReply.ballot.compareTo(prepare.ballot) > 0 ? "preempting"
								: "acking", prepare.ballot,
						prepareReply.getSummary() });

		MessagingTask mtask = prevBallot.compareTo(prepareReply.ballot) < 0 ?
		// log only if not already logged (if my ballot got upgraded)
		new LogMessagingTask(prepare.ballot.coordinatorID, prepareReply,
				prepare) :
		// else just send preempting prepareReply
				new MessagingTask(prepare.ballot.coordinatorID, prepareReply);
		return mtask;
	}

	/*
	 * Phase1b Event: Received a reply to my ballot preparation request.
	 * 
	 * Action: If the reply contains a higher ballot, we must resign. Otherwise,
	 * if we acquired a majority with the receipt of this reply, send all
	 * previously accepted (but uncommitted) requests reported in the prepare
	 * replies, each in its highest reported ballot, to all replicas. These are
	 * the proposals that get carried over across a ballot change and must be
	 * re-proposed.
	 * 
	 * Return: A list of messages each of which has to be multicast (proposed)
	 * to all replicas.
	 */
	private MessagingTask handlePrepareReply(PrepareReplyPacket prepareReply) {
		assert (prepareReply.getVersion() == this.getVersion());
		this.paxosManager.heardFrom(prepareReply.acceptor); // FD optimization,
		MessagingTask mtask = null;
		ArrayList<ProposalPacket> preActiveProposals = null;
		ArrayList<AcceptPacket> acceptList = null;

		if ((preActiveProposals = this.coordinator.getPreActivesIfPreempted(
				prepareReply, this.groupMembers)) != null) {
			log.log(Level.INFO, "{0} ({1}) election PREEMPTED by {2}",
					new Object[] { this, this.coordinator.getBallotStr(),
							prepareReply.ballot });
			if (!preActiveProposals.isEmpty())
				mtask = new MessagingTask(prepareReply.ballot.coordinatorID,
						MessagingTask.toPaxosPacketArray(preActiveProposals
								.toArray()));
		} else if ((acceptList = this.coordinator.handlePrepareReply(
				prepareReply, this.groupMembers)) != null
				&& !acceptList.isEmpty()) {
			mtask = new MessagingTask(this.groupMembers,
					MessagingTask.toPaxosPacketArray(acceptList.toArray()));
			log.log(Level.INFO, "{0} elected coordinator; sending {1}",
					new Object[] { this, mtask });
		} else
			log.log(Level.FINE, "{0} received {1}", new Object[] { this,
					prepareReply.getSummary() });

		return mtask; // Could be unicast or multicast
	}

	/*
	 * Phase2a Event: Received an accept message for a proposal with some
	 * ballot.
	 * 
	 * Action: Send back current or updated ballot to the ballot's coordinator.
	 */
	private MessagingTask handleAccept(AcceptPacket accept)
			throws JSONException {
		this.paxosManager.heardFrom(accept.ballot.coordinatorID); // FD
		RequestInstrumenter.received(accept, accept.sender, this.getMyID());
		Ballot ballot = this.paxosState.acceptAndUpdateBallot(accept,
				this.getMyID());
		if (ballot == null)
			return null;
		// can happen only if acceptor is stopped.

		this.garbageCollectAccepted(accept.getMedianCheckpointedSlot());
		if (accept.isRecovery())
			return null; // recovery ACCEPTS do not need any reply

		AcceptReplyPacket acceptReply = new AcceptReplyPacket(this.getMyID(),
				ballot, accept.slot,
				lastCheckpointSlot(this.paxosState.getSlot() - 1), accept);

		// no logging if NACking anyway
		AcceptPacket toLog = (accept.ballot.compareTo(ballot) >= 0
		// no logging if already garbage collected
		&& accept.slot - this.paxosState.getGCSlot() > 0) ? accept : null;

		MessagingTask mtask = toLog != null ? new LogMessagingTask(
				accept.sender, acceptReply, toLog) : new MessagingTask(
				accept.sender, acceptReply);
		RequestInstrumenter.sent(acceptReply, this.getMyID(), accept.sender);

		return mtask;
	}

	/*
	 * We don't need to implement this. Accept logs are pruned while
	 * checkpointing anyway, which is enough. Worse, it is probably inefficient
	 * to touch the disk for GC upon every new gcSlot (potentially every accept
	 * and decision).
	 */
	private void garbageCollectAccepted(int gcSlot) {
	}

	/*
	 * Phase2b Event: Received a reply to an accept request, i.e. to a request
	 * to accept a proposal from the coordinator.
	 * 
	 * Action: If this reply results in a majority for the corresponding
	 * proposal, commit the request and notify all. If this preempts a proposal
	 * being coordinated because it contains a higher ballot, forward to the
	 * preempting coordinator in the higher ballot reported.
	 * 
	 * Return: The committed proposal if any to be multicast to all replicas, or
	 * the preempted proposal if any to be unicast to the preempting
	 * coordinator. Null if neither.
	 */
	private MessagingTask handleAcceptReply(AcceptReplyPacket acceptReply) {
		this.paxosManager.heardFrom(acceptReply.acceptor); // FD optimization
		RequestInstrumenter.received(acceptReply, acceptReply.acceptor,
				this.getMyID());
		PValuePacket committedPValue = this.coordinator.handleAcceptReply(
				this.groupMembers, acceptReply);
		if (committedPValue == null)
			return null;

		MessagingTask multicastDecision = null;
		// separate variables only for code readability
		MessagingTask unicastPreempted = null;
		// Could also call handleCommittedRequest below or even just rely on
		// broadcast to all
		if (committedPValue.getType() == PaxosPacket.PaxosPacketType.DECISION) {
			committedPValue.addDebugInfo("d");
			// this.handleCommittedRequest(committedPValue);
			multicastDecision = new MessagingTask(this.groupMembers,
					committedPValue); // inform everyone of the decision
			log.log(Level.FINE, "{0} announcing decision {1}", new Object[] {
					this, committedPValue.getSummary() });
		} else if (committedPValue.getType() == PaxosPacket.PaxosPacketType.PREEMPTED) {
			/*
			 * Could drop the request, but we forward the preempted proposal as
			 * a no-op to the new coordinator for testing purposes. The new(er)
			 * coordinator information is within acceptReply. Note that our
			 * coordinator status may still be active and it will be so until
			 * all of its requests have been preempted. Note also that our local
			 * acceptor might still think we are the coordinator. The only
			 * evidence of a new coordinator is in acceptReply that must have
			 * reported a higher ballot if we are here, hence the assert.
			 * 
			 * Warning: Can not forward the preempted request as-is to the new
			 * coordinator as this can result in multiple executions of a
			 * request. Although the multiple executions will have different
			 * slot numbers and will not violate paxos safety, this is extremely
			 * undesirable for most applications.
			 */
			assert (committedPValue.ballot.compareTo(acceptReply.ballot) < 0 || committedPValue
					.hasTakenTooLong()) : (committedPValue + " >= "
					+ acceptReply + ", hasTakenTooLong=" + committedPValue
						.hasTakenTooLong());
			if (!committedPValue.isNoop() || shouldForwardNoops()) {
				// forward only if not already a no-op
				unicastPreempted = new MessagingTask(
						acceptReply.ballot.coordinatorID, committedPValue
								.makeNoop().setForwarderID(this.getMyID()));
				committedPValue.addDebugInfo("f");
				log.log(Level.INFO,
						"{0} forwarding preempted request as no-op to node {1}:{2}",
						new Object[] { this, acceptReply.ballot.coordinatorID,
								committedPValue.getSummary() });
			} else
				log.log(Level.WARNING,
						"{0} dropping no-op preempted by coordinator {1}: {2}",
						new Object[] { this, acceptReply.ballot.coordinatorID,
								committedPValue.getSummary() });
		}

		return committedPValue.getType() == PaxosPacket.PaxosPacketType.DECISION ? multicastDecision
				: unicastPreempted;
	}

	// whether to "save" a noop, i.e., an already preempted request
	private static final boolean shouldForwardNoops() {
		return false;
	}

	/*
	 * Phase3 Event: Received notification about a committed proposal.
	 * 
	 * Action: This method is responsible for executing a committed request. For
	 * this, it needs to call a handler implementing the PaxosInterface
	 * interface.
	 */
	private MessagingTask handleCommittedRequest(PValuePacket committed) {
		RequestInstrumenter.received(committed, committed.ballot.coordinatorID,
				this.getMyID());

		// Log, extract from or add to acceptor, and execute the request at app
		if (!committed.isRecovery())
			AbstractPaxosLogger.logDecision(this.paxosManager.getPaxosLogger(),
					committed, this);

		MessagingTask mtask = this.extractExecuteAndCheckpoint(committed);

		TESTPaxosConfig.commit(committed.requestID);

		if (this.paxosState.getSlot() - committed.slot < 0)
			log.log(Level.FINE, "{0}{1}{2}{3}{4} {5}", new Object[] { this,
					" expecting ", this.paxosState.getSlotLog(),
					" recieved out-of-order commit: ", committed.slot,
					committed.getSummary() });

		return mtask;
	}

	/*
	 * Typically invoked by handleCommittedRequest above. Also invoked at
	 * instance creation time if outOfOrderLimit low to deal with the
	 * progress-wise unsatisfying scenario where a paxos instance gets created
	 * just after other replicas have committed the first few decisions; if so,
	 * the newly starting replica will have no reason to suspect that anything
	 * is missing and may never catch up if no other decisions get committed
	 * (say, because the paxos instance gets stopped before any more decisions).
	 * It is good to prevent such scenarios (even though they don't affect
	 * safety), so we have shouldSync always return true at creation time i.e.,
	 * expected slot is 0 or 1.
	 * 
	 * forceSync is used only in the beginning in the case of missedBirthing.
	 */
	private MessagingTask fixLongDecisionGaps(PValuePacket committed,
			SyncMode syncMode) {

		MessagingTask fixGapsRequest = null;
		if (this.paxosState.canSync()
				&& (this.shouldSync((committed != null ? committed.slot
						: this.paxosState.getMaxCommittedSlot()), this
						.getPaxosManager().getOutOfOrderLimit(), syncMode))) {
			fixGapsRequest = this
					.requestMissingDecisions(committed != null ? committed.ballot.coordinatorID
							: this.paxosState.getBallotCoord());
			if (fixGapsRequest != null) {
				log.log(Level.INFO, "{0} fixing gaps {1}", new Object[] { this,
						fixGapsRequest });
				this.paxosState.justSyncd();
			}
		}
		return fixGapsRequest;
	}

	private MessagingTask fixLongDecisionGaps(PValuePacket committed) {
		return this.fixLongDecisionGaps(committed, SyncMode.DEFAULT_SYNC);
	}

	protected boolean isLongIdle() {
		return this.paxosState.isLongIdle();
	}

	protected void justActive() {
		this.paxosState.justActive();
	}

	private boolean checkIfTrapped(JSONObject incoming, MessagingTask mtask) {
		if (this.isStopped() && mtask != null) {
			log.log(Level.FINE,
					"{0} DROPPING message {1} trapped inside stopped instance",
					new Object[] { this, incoming, mtask });
			return true;
		}
		return false;
	}

	/*
	 * The three actions--(1) extracting the next slot request from the
	 * acceptor, (2) having the app execute the request, and (3) checkpoint if
	 * needed--need to happen atomically. If the app throws an error while
	 * executing the request, we need to retry until successful, otherwise, the
	 * replicated state machine will be stuck. So, essentially, the app has to
	 * support atomicity or the operations have to be idempotent for correctness
	 * of the replicated state machine.
	 * 
	 * This method is protected, not private, because it needs to be called by
	 * the logger after it is done logging the committed request. Having the
	 * logger call this method is only space-efficient design alternative.
	 */
	protected synchronized MessagingTask extractExecuteAndCheckpoint(
			PValuePacket loggedDecision) {
		long methodEntryTime = System.currentTimeMillis();
		if (this.paxosState.isStopped())
			return null;
		PValuePacket inorderDecision = null;
		int execCount = 0;
		// extract next in-order decision
		while ((inorderDecision = this.paxosState
				.putAndRemoveNextExecutable(loggedDecision)) != null) {
			log.log(inorderDecision.isStopRequest() ? Level.FINE : Level.FINE,
					"{0} received in-order commit {1} {2}",
					new Object[] { this, inorderDecision.slot,
							inorderDecision.getSummary() });
			String pid = this.getPaxosID();

			/*
			 * Execute it until successful, we are *by design* stuck otherwise.
			 * Execution must be atomic with extraction and possible
			 * checkpointing below.
			 */
			if (execute(this, this.clientRequestHandler,
					inorderDecision.getRequestValues(),
					inorderDecision.isRecovery()))
				// +1 for each batch, not for each constituent requestValue
				execCount++;
			// unclean kill
			else
				this.paxosManager.kill(this, false);

			// checkpoint if needed, must be atomic with the execution
			if (shouldCheckpoint(inorderDecision)
					&& !inorderDecision.isRecovery()) {
				this.paxosManager.getPaxosLogger().putCheckpointState(pid,
						this.version, this.groupMembers, inorderDecision.slot,
						this.paxosState.getBallot(),
						this.clientRequestHandler.getState(pid),
						this.paxosState.getGCSlot(),
						inorderDecision.getCreateTime());
				if (Util.oneIn(10))
					log.log(Level.INFO, "{0}",
							new Object[] { DelayProfiler.getStats() });
			}
			/*
			 * If stop request, copy epoch final state and kill self. If copy is
			 * not successful, we could get stuck trying to create future
			 * versions for this paxosID.
			 */
			if (inorderDecision.isStopRequest()
					&& this.paxosManager.getPaxosLogger()
							.copyEpochFinalCheckpointState(getPaxosID(),
									getVersion())
					&& logStop(inorderDecision.getCreateTime()))
				this.paxosManager.kill(this, true);
		}
		this.paxosState.assertSlotInvariant();
		// assert coz otherwise loggedDecision would have been executed
		assert (loggedDecision == null || this.paxosState.getSlot() != loggedDecision.slot);
		if (loggedDecision != null && !loggedDecision.isRecovery())
			DelayProfiler.updateDelay("EEC", methodEntryTime, execCount);
		return this.fixLongDecisionGaps(loggedDecision);
	}

	/**
	 * Helper method used above in EEC as well as by PaxosManager for emulating
	 * unreplicated execution for testing purposes.
	 */
	protected static boolean execute(PaxosInstanceStateMachine pism,
			InterfaceReplicable app, String[] requestValues,
			boolean doNotReplyToClient) {
		for (String requestValue : requestValues) {
			boolean executed = false;
			int retries = 0;
			do {
				try {
					executed = app.handleRequest(
							getInterfaceRequest(app, requestValue),
							doNotReplyToClient);
					if (pism != null && pism.isStopped())
						return true;
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (!executed)
					log.severe("App failed to execute request, retrying: "
							+ requestValue);
				/*
				 * We have to keep trying to execute until executed to preserve
				 * safety. We have removed the decision from the acceptor and
				 * there is no going back on that by design (as we assume that
				 * invariant at many places). One option here is to kill this
				 * paxos instance after a limited number of retries. The benefit
				 * of doing that is that we can free up this thread. But it is
				 * better to not delete the state on disk just yet as kill()
				 * would do by default.
				 */
				if (++retries > RETRY_LIMIT)
					return false;
			} while (!executed && waitRetry(RETRY_TIMEOUT));
		}
		return true;
	}

	// Like EEC but invoked upon checkpoint transfer
	private synchronized MessagingTask handleCheckpoint(StatePacket statePacket) {
		if (statePacket.slotNumber >= this.paxosState.getSlot()) {
			// put checkpoint in app (like execute)
			if (!this.clientRequestHandler.updateState(getPaxosID(),
					statePacket.state))
				return null;
			// update acceptor (like extract)
			this.paxosState.jumpSlot(statePacket.slotNumber + 1);
			// put checkpoint in logger (like checkpoint)
			this.paxosManager.getPaxosLogger().putCheckpointState(
					this.getPaxosID(), this.version, groupMembers,
					statePacket.slotNumber, statePacket.ballot,
					this.clientRequestHandler.getState(getPaxosID()),
					this.paxosState.getGCSlot());
			log.log(Level.FINE,
					"{0} inserted {1} checkpoint through handleCheckpoint; next slot = {2}",
					new Object[] { this,
							statePacket.slotNumber == 0 ? "initial state" : "",
							this.paxosState.getSlotLog() });
		}
		// coz otherwise we can get stuck as assertSlotInvariant() may not hold
		return extractExecuteAndCheckpoint(null);
	}

	/*
	 * This method is called by PaxosManager.hibernate that blocks on the
	 * checkpoint operation to finish (unlike regular checkpoints that are
	 * asynchronously handled by a helper thread). But hibernate is currently
	 * not really used as pause suffices. And PaxosManager methods are likely
	 * called by an executor task anyway, so blocking should be harmless.
	 */
	protected synchronized boolean tryForcedCheckpointAndStop() {
		boolean checkpointed = false;
		// Ugly nesting, not sure how else to do this correctly
		synchronized (this.paxosState) {
			synchronized (this.coordinator) {
				int cpSlot = this.paxosState.getSlot() - 1;
				if (this.paxosState.caughtUp() && this.coordinator.caughtUp()) {
					String pid = this.getPaxosID();
					this.paxosManager.getPaxosLogger().putCheckpointState(pid,
							this.getVersion(), this.groupMembers, cpSlot,
							this.paxosState.getBallot(),
							this.clientRequestHandler.getState(pid),
							this.paxosState.getGCSlot());
					checkpointed = true;
					log.log(Level.INFO,
							MyLogger.FORMAT[7],
							new Object[] {
									this,
									"forcing checkpoint at slot",
									cpSlot,
									"; garbage collected accepts upto slot",
									this.paxosState.getGCSlot(),
									"; max committed slot = ",
									this.paxosState.getMaxCommittedSlot(),
									(this.paxosState.getBallotCoordLog() == this
											.getMyID() ? "; maxCommittedFrontier="
											+ this.coordinator
													.getMajorityCommittedSlot()
											: "") });
					this.forceStop();
				}
			}
		}
		return checkpointed;
	}

	/*
	 * Needs to be synchronized so that extractExecuteAndCheckpoint does not
	 * happen concurrently. Likewise handleCheckpoint.
	 */
	protected synchronized boolean forceCheckpoint() {
		String pid = this.getPaxosID();
		int cpSlot = this.paxosState.getSlot() - 1;
		this.paxosManager.getPaxosLogger().putCheckpointState(pid,
				this.getVersion(), this.groupMembers, cpSlot,
				this.paxosState.getBallot(),
				this.clientRequestHandler.getState(pid),
				this.paxosState.getGCSlot());
		// need to acquire these without locking
		int gcSlot = this.paxosState.getGCSlot();
		int maxCommittedSlot = this.paxosState.getMaxCommittedSlot();
		String maxCommittedFrontier = (this.paxosState.getBallotCoordLog() == this
				.getMyID() ? "; maxCommittedFrontier="
				+ this.coordinator.getMajorityCommittedSlot() : "");
		log.log(Level.INFO,
				"{0} forcing checkpoint at slot {1}; garbage collected accepts up to slot {2}; max committed slot = {3} {4}",
				new Object[] { this, cpSlot, gcSlot, maxCommittedSlot,
						maxCommittedFrontier });
		return true;
	}

	/*
	 * A note on locking: The PaxosManager lock is typically the first to get
	 * acquired if it ever appears in a chain of locks with one exception as
	 * noted in the invariants below.
	 * 
	 * Invariants: There must be no lock chain
	 * 
	 * !!! PaxosManager -> PaxosInstanceStateMachine
	 * 
	 * because there is *by design* a lock chain
	 * 
	 * --> PaxosInstanceStateMachine -> PaxosManager
	 * 
	 * when this instance is being stopped.
	 * 
	 * There must be no lock chains as follows (an invariant is easy to adhere
	 * to or rather impossible to violate by design because acceptor and
	 * coordinator are unaware of and have no references to PaxosManager):
	 * 
	 * !!! nothing -> PaxosAcceptor -> PaxosManager
	 * 
	 * !!! nothing -> PaxosCoordinator -> PaxosManager
	 * 
	 * because there are lock chains of the form
	 * 
	 * --> PaxosManager -> PaxosAcceptor or PaxosCoordinator
	 */

	/*
	 * Same as tryForcedCheckpointAndStop but without the checkpoint.
	 * 
	 * Why this method is not synchronized: when this paxos instance is
	 * executing a request that takes a long time, this method might
	 * concurrently try to pause it and even succeed (!), say, because the
	 * decision being executed has been extracted and the acceptor looks all
	 * nicely caught up. Is this a problem? The forceStop in this method will
	 * stop the acceptor, but the thread executing EEC will go ahead and
	 * complete the execution and even checkpoint and kill if it is a stop
	 * request. The fact that the acceptor is in a stopped state won't matter
	 * for the current decision being executed. After that, the loop in EEC will
	 * break and return, so no harm done. When this instance gets eventually
	 * unpaused, it would seem exactly like just after having executed that last
	 * decision, so no harm done.
	 * 
	 * Conversely, this method might lock paxosState first and then EEC might
	 * get invoked. If so, the program counter could enter the synchronized EEC
	 * method but will block on paxosState.isStopped until this tryPause method
	 * finishes. If tryPuase is unsuccessful, nothing has changed, so no harm
	 * done. Else if tryPause successfully pauses, isStopped will return true
	 * and EEC will become a noop, so no harm done.
	 * 
	 * Note: If we make this method synchronized, the deactivator thread could
	 * be blocked on this instance for a long time.
	 */
	protected boolean tryPause() {
		boolean paused = false;
		synchronized (this.paxosState) {
			// Ugly nesting, not sure how else to do this correctly
			synchronized (this.coordinator) {
				if (this.paxosState.caughtUp() && this.coordinator.caughtUp()) {
					HotRestoreInfo hri = new HotRestoreInfo(this.getPaxosID(),
							this.getVersion(), this.groupMembers,
							this.paxosState.getSlot(),
							this.paxosState.getBallot(),
							this.paxosState.getGCSlot(),
							this.coordinator.getBallot(),
							this.coordinator.getNextProposalSlot(),
							this.coordinator.getNodeSlots());
					log.log(Level.FINE, "{0} pausing [{1}]", new Object[] {
							this, hri });
					// stop only if pause successful.
					if (paused = this.paxosManager.getPaxosLogger().pause(
							getPaxosID(), hri.toString()))
						this.forceStop();
				} else
					log.log(Level.FINE,
							"{0} not pausing because it is not caught up: {1} {2}",
							new Object[] { this, this.paxosState,
									this.coordinator });
			}
		}
		return paused;
	}

	private boolean shouldCheckpoint(PValuePacket decision) {
		return (decision.slot % this.paxosManager.getInterCheckpointInterval() == 0 || decision
				.isStopRequest());
	}

	private static InterfaceRequest getInterfaceRequest(
			InterfaceReplicable app, String value) {
		try {
			return app.getRequest(value);
		} catch (RequestParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/*************************** End of phase 3 methods ********************************/

	/********************** Start of failure detection and recovery methods *****************/

	/*
	 * FIXED: If a majority miss a prepare, the coordinator may never get
	 * elected as follows. The minority of acceptors who did receive the prepare
	 * will assume the prepare's sender as the current coordinator. The rest
	 * might still think the previous coordinator is the current coordinator.
	 * All acceptors could be thinking that their up, so nobody will bother
	 * running for coordinator. To break this impasse, we need to resend the
	 * prepare. This has been now incorporated in the handleMessage that quickly
	 * checks upon every message if we need to "(re)run for coordinator" (for
	 * the same ballot) if we have been waiting for too long (having neither
	 * received a prepare majority nor a preemption) for the ballot to complete.
	 */

	/*
	 * Checks whether current ballot coordinator is alive. If not, it checks if
	 * it should try to be the nest coordinator and if so, it becomes the next
	 * coordinator. This method can be called any time safely by any thread.
	 */
	private MessagingTask checkRunForCoordinator() {
		return this.checkRunForCoordinator(false);
	}

	private MessagingTask checkRunForCoordinator(boolean forceRun) {
		Ballot curBallot = this.paxosState.getBallot();
		MessagingTask multicastPrepare = null;

		/*
		 * curBallot is my acceptor's ballot; "my acceptor's coordinator" is
		 * that ballot's coordinator.
		 * 
		 * If I am not already a coordinator with a ballot at least as high as
		 * my acceptor's ballot's coordinator
		 * 
		 * AND
		 * 
		 * I didn't run too recently
		 * 
		 * AND
		 * 
		 * (I am my acceptor's coordinator OR (my acceptor's coordinator is dead
		 * AND (I am next in line OR the current coordinator has been dead for a
		 * really long time)))
		 * 
		 * OR forceRun
		 */
		if ((
		/*
		 * I am not already a coordinator with a ballot at least as high as my
		 * acceptor's ballot's coordinator && I didn't run too recently
		 */
		!this.coordinator.exists(curBallot) && !this.coordinator.ranRecently() &&
		// I am my acceptor's coordinator (can happen during recovery)
		(curBallot.coordinatorID == this.getMyID() ||
		// my acceptor's coordinator is dead
		(!this.paxosManager.isNodeUp(curBallot.coordinatorID) &&
		// I am next in line
		(this.getMyID() == getNextCoordinator(curBallot.ballotNumber + 1,
				this.groupMembers) ||
		// current coordinator has been long dead
				paxosManager.lastCoordinatorLongDead(curBallot.coordinatorID)))))
				|| forceRun) {
			/*
			 * We normally round-robin across nodes for electing coordinators,
			 * e.g., node 7 will try to become coordinator in ballotnum such
			 * that ballotnum%7==0 if it suspects that the current coordinator
			 * is dead. But it is more robust to check if it has been a long
			 * time since we heard anything from the current coordinator and if
			 * so, try to become a coordinator ourself even though it is not our
			 * turn. Otherwise, weird partitions can result in loss of liveness,
			 * e.g., the next-in-line coordinator thinks the current coordinator
			 * is up but most everyone else thinks the current coordinator is
			 * down. Or the next-in-line coordinator itself could be dead. The
			 * downside of this lastCoordinatorLongDead check is that many nodes
			 * might near simultaneously try to become coordinator with no one
			 * succeeding for a while, but this is unlikely to be a problem if
			 * we rely on the deterministic round-robin rule in the common case
			 * and rely on the lasCoordinatorLongDead with a longer timeout
			 * (much longer than the typical node failure detection timeout).
			 */
			log.log(Level.INFO,
					"{0} running for coordinator as node {1} {2}",
					new Object[] {
							this,
							curBallot.coordinatorID,
							(curBallot.coordinatorID != this.getMyID() ? " seems dead (last pinged "
									+ (this.paxosManager
											.getDeadTime(curBallot.coordinatorID) / 1000)
									+ " secs back)"
									: " has not yet initialized its coordinator") });
			Ballot newBallot = new Ballot(curBallot.ballotNumber + 1,
					this.getMyID());
			if (this.coordinator.makeCoordinator(newBallot.ballotNumber,
					newBallot.coordinatorID, this.groupMembers,
					this.paxosState.getSlot(), false) != null) {
				multicastPrepare = new MessagingTask(this.groupMembers,
						new PreparePacket(newBallot, this.paxosState.getSlot()));
			}
		} else if (this.coordinator.waitingTooLong()) {
			assert (!this.coordinator.waitingTooLong()) : this + " "
					+ this.coordinator;
			log.warning(this + " resending timed out PREPARE "
					+ this.coordinator.getBallot()
					+ "; this is only needed under high congestion");
			Ballot newBallot = this.coordinator.remakeCoordinator(groupMembers);
			if (newBallot != null) {
				multicastPrepare = new MessagingTask(this.groupMembers,
						new PreparePacket(newBallot, this.paxosState.getSlot()));
			}
		} else if (!this.paxosManager.isNodeUp(curBallot.coordinatorID)
				&& !this.coordinator.exists(curBallot)) { // not my job
			log.log(Level.FINE,
					"{0} thinks current coordinator {1} is {2} dead, the next-in-line is {3}{4}",
					new Object[] {
							this,
							curBallot.coordinatorID,
							(paxosManager
									.lastCoordinatorLongDead(curBallot.coordinatorID) ? "*long*"
									: ""),
							getNextCoordinator(curBallot.ballotNumber + 1,
									this.groupMembers),
							(this.coordinator.ranRecently() ? ", and I ran too recently to try again"
									: "") });
		}
		return multicastPrepare;
	}

	private String getBallots() {
		return "[C:("
				+ (this.coordinator != null ? this.coordinator.getBallotStr()
						: "null")
				+ "), A:("
				+ (this.paxosState != null ? this.paxosState.getBallotSlot()
						: "null") + ")]";
	}

	private String getNodeState() {
		return "Node" + this.getNodeID() + ":" + this.getPaxosIDVersion() + ":"
				+ this.getBallots();
	}

	/*
	 * Computes the next coordinator as the node with the smallest ID that is
	 * still up. We could plug in any deterministic policy here. But this policy
	 * should somehow take into account whether nodes are up or down. Otherwise,
	 * paxos will be stuck if both the current and the next-in-line coordinator
	 * are both dead.
	 * 
	 * It is important to choose the coordinator in a deterministic way when
	 * recovering, e.g., the lowest numbered node. Otherwise different nodes may
	 * have different impressions of who the coordinator is with unreliable
	 * failure detectors, but no one other than the current coordinator may
	 * actually ever run for coordinator. E.g., with three nodes 100, 101, 102,
	 * if 102 thinks 101 is the coordinator, and the other two start by assuming
	 * 100 is the coordinator, then 102's accept replies will keep preempting
	 * 100's accepts but 101 may never run for coordinator as it has no reason
	 * to think there is any problem with 100.
	 */
	private int getNextCoordinator(int ballotnum, int[] members,
			boolean recovery) {
		for (int i = 1; i < members.length; i++)
			assert (members[i - 1] < members[i]);
		if (recovery)
			return members[0];
		for (int i = 0; i < members.length; i++) {
			if (this.paxosManager != null
					&& this.paxosManager.isNodeUp(members[i]))
				return members[i];
		}
		return roundRobinCoordinator(ballotnum);
	}

	private int getNextCoordinator(int ballotnum, int[] members) {
		return this.getNextCoordinator(ballotnum, members, false);
	}

	private int roundRobinCoordinator(int ballotnum) {
		return this.getMembers()[ballotnum % this.getMembers().length];
	}

	/*
	 * FIXED: If a majority miss an accept, but any messages are still being
	 * received at all, then the loss will eventually get fixed by a check
	 * similar to checkRunForCoordinator that upon receipt of every message will
	 * poke the local coordinator to recommander the next-in-line accept if the
	 * accept has been waiting for too long (for a majority or preemption). Both
	 * the prepare and accept waiting checks are quick O(1) operations.
	 */

	private MessagingTask pokeLocalCoordinator() {
		AcceptPacket accept = this.coordinator
				.reissueAcceptIfWaitingTooLong(this.paxosState.getSlot());
		if (accept != null)
			log.log(Level.INFO, MyLogger.FORMAT[2], new Object[] { this,
					" resending timed out ACCEPT ", accept.getSummary() });
		else
			log.log(Level.FINEST, "{0} coordinator {1} is good for now",
					new Object[] { this, this.coordinator });
		MessagingTask reAccept = (accept != null ? new MessagingTask(
				this.groupMembers, accept) : null);
		return reAccept;
	}

	private boolean logStop(long createTime) {
		DelayProfiler.updateDelay("stopRequestCoordinationTime", createTime);
		log.log(Level.INFO, "{0} >>>>STOPPED||||||||||", new Object[] { this });
		return true;
	}

	/*
	 * Event: Received or locally generated a sync request. Action: Send a sync
	 * reply containing missing committed requests to the requester. If the
	 * requester is myself, multicast to all.
	 */
	private MessagingTask requestMissingDecisions(int coordinatorID) {
		ArrayList<Integer> missingSlotNumbers = this.paxosState
				.getMissingCommittedSlots(this.paxosManager
						.getMaxSyncDecisionsGap());
		// initially we might want to send an empty sync request
		if (missingSlotNumbers == null)
			return null; // if stopped
		else if (missingSlotNumbers.isEmpty())
			missingSlotNumbers.add(this.paxosState.getSlot());

		int maxDecision = this.paxosState.getMaxCommittedSlot();
		SyncDecisionsPacket srp = new SyncDecisionsPacket(this.getMyID(),
				maxDecision, missingSlotNumbers, this.isMissingTooMuch());

		// send sync request to coordinator or multicast to all but me if I am
		// the coordinator myself
		MessagingTask mtask = coordinatorID != this.getMyID() ? new MessagingTask(
				coordinatorID, srp) : new MessagingTask(otherGroupMembers(),
				srp);
		return mtask;
	}

	/*
	 * We normally sync decisions if the gap between the maximum decision slot
	 * and the expected slot is at least as high as the threshold. But we also
	 * sync in the beginning when the expected slot is 0 (if we disable null
	 * checkpoints) or 1 and there is either a nonzero gap or simply if the
	 * threshold is 1. The initial nonzero gap is an understandable
	 * optimization. But we also sync in the special case when the threshold is
	 * low and this paxos instance has just gotten created (even when there is
	 * no gap) because it is possible that other replicas have committed
	 * decisions that I don't even know have happened. This optimizaiton is not
	 * necessary for safety, but it is useful for liveness especially in the
	 * case when an epoch start (in reconfiguration) is not considered complete
	 * until all replicas have committed the first decision (as in the special
	 * case of reconfigurator node reconfigurations).
	 */
	private static final int INITIAL_SYNC_THRESHOLD = 1;

	private boolean shouldSync(int maxDecisionSlot, int threshold,
			SyncMode syncMode) {
		int expectedSlot = this.paxosState.getSlot();
		boolean nonzeroGap = maxDecisionSlot - expectedSlot > 0;
		boolean smallGapThreshold = threshold <= INITIAL_SYNC_THRESHOLD;

		return
		// typical legitimate sync criterion
		(maxDecisionSlot - expectedSlot >= threshold)
		// sync decisions initially if nonzer gap or small threshold
				|| ((expectedSlot == 0 || expectedSlot == 1) && (nonzeroGap || smallGapThreshold))
				// non-zero gap and syncMode is SYNC_IF_NONZERO_GAP
				|| (nonzeroGap && SyncMode.SYNC_TO_PAUSE.equals(syncMode))
				// force sync
				|| SyncMode.FORCE_SYNC.equals(syncMode);

	}

	private boolean shouldSync(int maxDecisionSlot, int threshold) {
		return shouldSync(maxDecisionSlot, threshold, SyncMode.DEFAULT_SYNC);
	}

	private boolean isMissingTooMuch() {
		return this.shouldSync(this.paxosState.getMaxCommittedSlot(),
				this.paxosManager.getMaxSyncDecisionsGap());
	}

	// point here is really to request initial state
	protected MessagingTask requestZerothMissingDecision() {
		ArrayList<Integer> missingSlotNumbers = new ArrayList<Integer>();
		missingSlotNumbers.add(0);

		SyncDecisionsPacket srp = new SyncDecisionsPacket(this.getMyID(), 1,
				missingSlotNumbers, true);

		log.log(Level.INFO, MyLogger.FORMAT[1], new Object[] { this,
				"requesting missing zeroth checkpoint." });
		// send sync request to coordinator or multicast to others if I am
		// coordinator
		MessagingTask mtask = this.paxosState.getBallotCoord() != this
				.getMyID() ? new MessagingTask(
				this.paxosState.getBallotCoord(), srp) : new MessagingTask(
				otherGroupMembers(), srp);
		return mtask;
	}

	// Utility method to get members except myself
	private int[] otherGroupMembers() {
		int[] others = new int[this.groupMembers.length - 1];
		int j = 0;
		for (int i = 0; i < this.groupMembers.length; i++) {
			if (this.groupMembers[i] != this.getMyID())
				others[j++] = this.groupMembers[i];
		}
		return others;
	}

	/*
	 * Event: Received a sync reply packet with a list of missing committed
	 * requests Action: Send back all missing committed requests from the log to
	 * the sender (replier).
	 * 
	 * We could try to send some from acceptor memory instead of the log, but in
	 * general, it is not worth the effort. Furthermore, if the sync gap is too
	 * much, do a checkpoint transfer.
	 */
	private MessagingTask handleSyncDecisionsPacket(
			SyncDecisionsPacket syncReply) throws JSONException {
		int minMissingSlot = syncReply.missingSlotNumbers.get(0);
		log.log(Level.INFO, "{0} handling sync decisions request {1}",
				new Object[] { this, syncReply.getSummary() });

		// try to get checkpoint
		MessagingTask checkpoint = null;
		if (this.paxosState.getSlot() - minMissingSlot <= 0)
			return null; // I am worse than you
		else if (minMissingSlot - lastCheckpointSlot(this.paxosState.getSlot()) <= 0) {
			checkpoint = handleCheckpointRequest(syncReply);
			if (checkpoint != null)
				minMissingSlot = ((StatePacket) (checkpoint.msgs[0])).slotNumber + 1;
		}

		// get decisions from database as unlikely to have all of them in memory
		ArrayList<PValuePacket> missingDecisions = this.paxosManager
				.getPaxosLogger()
				.getLoggedDecisions(
						this.getPaxosID(),
						this.getVersion(),
						minMissingSlot,
						/*
						 * If maxDecision <= minMissingSlot, sender is probably
						 * doing a creation sync. But we need min < max for the
						 * database query to return nonzero results, so we
						 * adjust up the max if needed. Note that
						 * getMaxCommittedSlot() at this node may not be greater
						 * than minMissingDecision either. For example, the
						 * sender may be all caught up at slot 0 and request a
						 * creation sync for 1 and this node may have committed
						 * up to 1; if so, it should return decision 1.
						 */
						syncReply.maxDecisionSlot > minMissingSlot ? syncReply.maxDecisionSlot
								: Math.max(
										minMissingSlot + 1,
										this.paxosState.getMaxCommittedSlot() + 1));

		for (Iterator<PValuePacket> pvalueIterator = missingDecisions
				.iterator(); pvalueIterator.hasNext();) {
			PValuePacket pvalue = pvalueIterator.next();
			if (!syncReply.missingSlotNumbers.contains(pvalue.slot))
				pvalueIterator.remove(); // filter non-missing
			assert (!pvalue.isRecovery()); // isRecovery() only in rollForward
		}
		MessagingTask unicasts = missingDecisions.isEmpty() ? null
				: new MessagingTask(syncReply.nodeID,
						MessagingTask.toPaxosPacketArray(missingDecisions
								.toArray()));
		if (unicasts != null)
			log.log(Level.INFO,
					"{0} sending {1} missing decisions to node {2}",
					new Object[] { this, unicasts.msgs.length, syncReply.nodeID });
		if (checkpoint != null)
			log.log(Level.INFO,
					"{0} sending checkpoint for slot {1} to node {2}",
					new Object[] { this, minMissingSlot - 1, syncReply.nodeID });

		// combine checkpoint and missing decisions in unicasts
		MessagingTask mtask =
		// both nonempty => combine
		(checkpoint != null && unicasts != null && !checkpoint.isEmpty() && !unicasts
				.isEmpty()) ? mtask = new MessagingTask(syncReply.nodeID,
				MessagingTask
						.toPaxosPacketArray(checkpoint.msgs, unicasts.msgs)) :
		// nonempty checkpoint
				(checkpoint != null && !checkpoint.isEmpty()) ? checkpoint :
				// unicasts (possibly also empty or null)
						unicasts;
		log.info(this + " sending mtask " + mtask);
		return mtask;
	}

	private int lastCheckpointSlot(int slot) {
		return lastCheckpointSlot(slot,
				this.paxosManager.getInterCheckpointInterval());
	}

	private static int lastCheckpointSlot(int slot, int checkpointInterval) {
		int lcp = slot - slot % checkpointInterval;
		if (lcp < 0 && ((lcp -= checkpointInterval) > 0)) // wraparound-arithmetic
			lcp = lastCheckpointSlot(Integer.MAX_VALUE, checkpointInterval);
		return lcp;
	}

	/*
	 * Event: Received a request for a recent checkpoint presumably from a
	 * replica that has recovered after a long down time. Action: Send
	 * checkpoint to requester.
	 */
	private MessagingTask handleCheckpointRequest(SyncDecisionsPacket syncReply) {
		/*
		 * The assertion below does not mean that the state we actually get will
		 * be at lastCheckpointSlot() or higher because, even though getSlot()
		 * has gotten updated, the checkpoint to disk may not yet have finished.
		 * We have no way of knowing other than reading the disk. So we first do
		 * a read to check if the checkpointSlot is at least higher than the
		 * minMissingSlot in syncReply. If the state is tiny, this will double
		 * the state fetching overhead as we are doing two database reads.
		 */
		assert (syncReply.missingSlotNumbers.get(0)
				- lastCheckpointSlot(this.paxosState.getSlot()) <= 0);
		int checkpointSlot = this.paxosManager.getPaxosLogger()
				.getCheckpointSlot(getPaxosID());
		StatePacket statePacket = (checkpointSlot >= syncReply.missingSlotNumbers
				.get(0) ? StatePacket.getStatePacket(this.paxosManager
				.getPaxosLogger().getSlotBallotState(this.getPaxosID())) : null);
		if (statePacket != null)
			log.log(Level.INFO,
					"{0} sending checkpoint to node {1}: {2}",
					new Object[] { this, syncReply.nodeID,
							statePacket.getSummary() });
		else {
			String myStatus = (!this.coordinator.exists() ? "[acceptor]"
					: this.coordinator.isActive() ? "[coordinator]"
							: "[preactive-coordinator]");
			log.info(this + myStatus + " has no state (yet) for "
					+ syncReply.getSummary());
		}

		return statePacket != null ? new MessagingTask(syncReply.nodeID,
				statePacket) : null;
	}

	/*************** End of failure detection and recovery methods ***************/

	/************************ Start of testing and instrumentation methods *****************/
	/*
	 * Used only to test paxos instance size. We really need a paxosManager to
	 * do anything real with paxos.
	 */
	private void testingNoRecovery() {
		int initSlot = 0;
		this.coordinator = new PaxosCoordinator();
		if (this.groupMembers[0] == this.getMyID())
			coordinator.makeCoordinator(0, this.groupMembers[0], groupMembers,
					initSlot, true);
		this.paxosState = new PaxosAcceptor(0, this.groupMembers[0], initSlot,
				null);
	}

	private static int CREATION_LOG_THRESHOLD = 100000;
	private static int creationCount = 0;

	private static void incrInstanceCount() {
		creationCount++;
	}

	protected static void decrInstanceCount() {
		creationCount--;
	}

	// only an approximate count for instrumentation purposes
	private static int getInstanceCount() {
		return creationCount;
	}

	private static boolean notManyInstances() {
		return getInstanceCount() < CREATION_LOG_THRESHOLD;
	}

	protected void testingInit(int load) {
		this.coordinator.testingInitCoord(load);
		this.paxosState.testingInitInstance(load);
	}
}
