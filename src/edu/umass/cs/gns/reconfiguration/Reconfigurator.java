package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.gigapaxos.InterfaceRequest;
import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.Stringifiable;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.ReconfiguratorProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.WaitAckDropEpoch;
import edu.umass.cs.gns.reconfiguration.json.WaitAckStartEpoch;
import edu.umass.cs.gns.reconfiguration.json.WaitAckStopEpoch;
import edu.umass.cs.gns.reconfiguration.json.WaitCoordinatedCommit;
import edu.umass.cs.gns.reconfiguration.json.WaitPrimaryExecution;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RCRecordRequest.RequestTypes;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigureRCNodeConfig;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.AggregateDemandProfiler;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 */
public class Reconfigurator<NodeIDType> implements
		InterfacePacketDemultiplexer, InterfaceReconfiguratorCallback {

	private final JSONMessenger<NodeIDType> messenger;
	private final ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String> protocolExecutor;
	protected final ReconfiguratorProtocolTask<NodeIDType> protocolTask;
	private final RepliconfigurableReconfiguratorDB<NodeIDType> DB;
	private final ConsistentReconfigurableNodeConfig<NodeIDType> consistentNodeConfig;
	private final AggregateDemandProfiler demandProfiler = new AggregateDemandProfiler();

	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	/*
	 * Any id-based communication requires NodeConfig and Messenger. In general,
	 * the former may be a subset of the NodeConfig used by the latter, so they
	 * are separate arguments.
	 */
	public Reconfigurator(InterfaceReconfigurableNodeConfig<NodeIDType> nc,
			JSONMessenger<NodeIDType> m) {
		this.messenger = m;
		this.consistentNodeConfig = new ConsistentReconfigurableNodeConfig<NodeIDType>(
				nc);
		this.protocolExecutor = new ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String>(
				messenger);
		this.protocolTask = new ReconfiguratorProtocolTask<NodeIDType>(
				getMyID(), this);
		this.protocolExecutor.register(this.protocolTask.getDefaultTypes(),
				this.protocolTask); // non default types will be registered by
									// spawned tasks
		this.DB = new RepliconfigurableReconfiguratorDB<NodeIDType>(
				new DerbyPersistentReconfiguratorDB<NodeIDType>(
						this.messenger.getMyID(), this.consistentNodeConfig),
				getMyID(), this.consistentNodeConfig, this.messenger);
		this.DB.setCallback(this);
		this.finishPendingReconfigurations();
	}

	/*
	 * This treats the reconfigurator itself as an "active replica" in order to
	 * be able to reconfigure reconfigurator groups.
	 */
	public ActiveReplica<NodeIDType> getReconfigurableReconfiguratorAsActiveReplica() {
		return new ActiveReplica<NodeIDType>(this.DB,
				this.consistentNodeConfig.getUnderlyingNodeConfig(),
				this.messenger);
	}

	@Override
	public boolean handleJSONObject(JSONObject jsonObject) {
		try {
			ReconfigurationPacket.PacketType rcType = ReconfigurationPacket
					.getReconfigurationPacketType(jsonObject);
			log.log(Level.INFO, MyLogger.FORMAT[3], new Object[] { this,
					"received", rcType, jsonObject });
			// try handling as reconfiguration packet through protocol task
			assert (rcType != null); // not "unchecked"
			@SuppressWarnings("unchecked")
			BasicReconfigurationPacket<NodeIDType> rcPacket = (BasicReconfigurationPacket<NodeIDType>) ReconfigurationPacket
					.getReconfigurationPacket(jsonObject, this.getUnstringer());
			// all packets are handled through executor, nice and simple
			if (!this.protocolExecutor.handleEvent(rcPacket))
				// do nothing
				log.log(Level.INFO, MyLogger.FORMAT[2], new Object[] { this,
						"unable to handle packet", jsonObject });
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return false; // neither reconfiguration packet nor app request
	}

	public Set<ReconfigurationPacket.PacketType> getPacketTypes() {
		return this.protocolTask.getEventTypes();
	}

	public String toString() {
		return "RC" + getMyID();
	}

	public void close() {
		this.protocolExecutor.stop();
		this.messenger.stop();
		this.DB.close();
	}

	/****************************** Start of protocol task handler methods *********************/
	/*
	 * Incorporates demand reports (possibly but not necessarily with replica
	 * coordination), checks for reconfiguration triggers, and initiates
	 * reconfiguration if needed.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleDemandReport(
			DemandReport<NodeIDType> report,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(Level.FINEST, MyLogger.FORMAT[3], new Object[] { this,
				"received", report.getType(), report });
		if (report.needsCoordination())
			this.DB.handleIncoming(report); // coordinated
		else
			this.updateDemandProfile(report); // no coordination
		// coordinate and commit reconfiguration intent
		this.initiateReconfiguration(report.getServiceName(),
				shouldReconfigure(report.getServiceName())); // coordinated
		trimAggregateDemandProfile();
		return null; // never any messaging or ptasks
	}

	/*
	 * Create service name is a special case of reconfiguration where the
	 * previous group is non-existent.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleCreateServiceName(
			CreateServiceName event,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		CreateServiceName create = (CreateServiceName) event;
		log.log(Level.FINEST, MyLogger.FORMAT[3], new Object[] { this,
				"received", event.getType(), create });

		/*
		 * Commit initial "reconfiguration" intent. If the record can be created
		 * at all default actives, this operation will succeed, and fail
		 * otherwise; in either case, the reconfigurators will have an
		 * eventually consistent view of whether the record got created or not.
		 * 
		 * Note that we need to maintain this consistency property even when
		 * nodeConfig may be in flux, i.e., different reconfigurators may have
		 * temporarily different views of what the current set of
		 * reconfigurators is. But this is okay as app record creations (as well
		 * as all app record reconfigurations) are done by an RC paxos group
		 * that agrees on whether the creation completed or not; this claim is
		 * true even if that RC group is itself undergoing reconfiguration. If
		 * nodeConfig is outdated at some node, that only affects the choice of
		 * active replicas below, not their consistency.
		 * 
		 * FIXME: return an error message if the creation failed.
		 */
		if (this.DB.getReconfigurationRecord(create.getServiceName()) == null)
			this.initiateReconfiguration(create.getServiceName(),
					this.consistentNodeConfig.getReplicatedActives(create
							.getServiceName()), create.getSender(), create
							.getInitialState(), null);
		else
			// return error message
			this.sendCreationError(create);

		return null;
	}

	/*
	 * Simply hand over DB request to DB. The only type of RC record that can
	 * come here is one announcing reconfiguration completion. Reconfiguration
	 * initiation messages are derived locally and coordinated through paxos,
	 * not received from outside.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleRCRecordRequest(
			RCRecordRequest<NodeIDType> rcRecReq,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(Level.INFO, MyLogger.FORMAT[3], new Object[] { this,
				"received", rcRecReq.getType(), rcRecReq });
		// just handle locally, nothing else
		if (rcRecReq.isReconfigurationComplete()
				|| (rcRecReq.isDeleteConfirmation() /* && replica group exists */)) {
			/*
			 * We should not spawn a task to commit the reconfiguration complete
			 * message as the intent/complete two-step is not needed for RC
			 * group reconfiguration because each affected reconfigurator is
			 * redundantly executing the group change. In fact, it is possible
			 * that a reconfigurator has completed reconfiguring a group but the
			 * new paxos group is not locally initiated yet (e.g., the node gets
			 * ackStartEpochs from other replicas), in which case, the propose
			 * will locally fail if we try to coordinate.
			 */
			this.repeatUntilObviated(rcRecReq);

			// remove stop and start tasks known to be obviated
			garbageCollectStopAndStartTasks(rcRecReq);
		} else if (rcRecReq.isReconfigurationIntent()
				&& this.DB.isRCGroupName(rcRecReq.getServiceName())) {
			this.repeatUntilObviated(rcRecReq);
		} else if(rcRecReq.isReconfigurationMerge()) {
			this.repeatUntilObviated(rcRecReq);
		}
		else
			assert (false);
		return null;
	}

	/*
	 * We need to ensure that both the stop/drop at actives happens atomically
	 * with the removal of the record at reconfigurators. To accomplish this, we
	 * first mark the record as stopped at reconfigurators, then wait for the
	 * stop/drop tasks to finish, and finally coordinate the completion
	 * notification so that reconfigurators can completely remove the record
	 * from their DB.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleDeleteServiceName(
			DeleteServiceName delete,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
				this.getMyID(), this.formStartEpoch(delete.getServiceName(),
						null, delete.getSender(), null),
				RequestTypes.RECONFIGURATION_INTENT);
		// coordinate intent with replicas
		if (this.isReadyForReconfiguration(rcRecReq))
			this.DB.handleIncoming(rcRecReq);
		else {
			log.log(Level.INFO, MyLogger.FORMAT[1], new Object[] {
					"Discarding ", rcRecReq.getSummary() });
			this.sendDeletionError(delete);
		}
		return null;
	}

	/*
	 * This method simply looks up and returns the current set of active
	 * replicas. Maintaining this state consistently is the primary and only
	 * existential purpose of reconfigurators.
	 */
	@SuppressWarnings("unchecked")
	public GenericMessagingTask<NodeIDType, ?>[] handleRequestActiveReplicas(
			RequestActiveReplicas request,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(request.getServiceName());
		if (record == null)
			return null;
		// else
		Set<NodeIDType> actives = record.getActiveReplicas();
		Set<InetSocketAddress> activeIPs = new HashSet<InetSocketAddress>();
		/*
		 * It is important to ensure that the mapping between active nodeIDs and
		 * their socket addresses does not change or changes very infrequently.
		 * Otherwise, in-flux copies of nodeConfig can produce wrong answers
		 * here. This assumption is reasonable and will hold as long as active
		 * nodeIDs are re-used with the same socket address or removed and
		 * re-added after a long time if at all by which time all nodes have
		 * forgotten about the old id-to-address mapping.
		 */
		for (NodeIDType node : actives)
			activeIPs.add(this.consistentNodeConfig.getNodeSocketAddress(node));
		request.setActives(activeIPs);
		GenericMessagingTask<InetSocketAddress, ?> mtask = new GenericMessagingTask<InetSocketAddress, Object>(
				request.getSender(), request);
		/*
		 * Note: casting GenericMessagingTask<InetSocketAddress, ?>[] to
		 * GenericMessagingTask<NodeIDType, ?>[] below, which is absurd, but
		 * works only because JSONMessenger is designed to treat
		 * InetSocketAddress as a special case where it won't try to treat it as
		 * NodeIDType and needlessly try to convert it to an InetSocketAddress
		 * that it already is anyway. The compiler only throws a warning
		 * (suppressed above) and the runtime doesn't seem to mind this
		 * absurdity.
		 */
		return (GenericMessagingTask<NodeIDType, ?>[]) (mtask.toArray());
	}

	/*
	 * Handles a request to add or delete a reconfigurator from the set of all
	 * reconfigurators in NodeConfig. The reconfiguration record corresponding
	 * to NodeConfig is stored in the RC records table and the
	 * "active replica state" or the NodeConfig info itself is stored in a
	 * separate NodeConfig table in the DB.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleReconfigureRCNodeConfig(
			ReconfigureRCNodeConfig<NodeIDType> changeRC,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		assert (changeRC.getServiceName()
				.equals(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString()));
		if (!this.isPermitted(changeRC)) {
			log.severe(this
					+ " received request to conduct impermissible node config change");
			return null;
		}
		// check first if NC is ready for reconfiguration
		ReconfigurationRecord<NodeIDType> ncRecord = this.DB
				.getReconfigurationRecord(changeRC.getServiceName());
		assert (ncRecord != null);
		if (!ncRecord.getState().equals(RCStates.READY)) {
			log.warning(this
					+ " trying to conduct concurrent node config changes");
			return null;
		}
		// try to reconfigure even though it may still fail
		Set<NodeIDType> curRCs = ncRecord.getActiveReplicas();
		Set<NodeIDType> newRCs = new HashSet<NodeIDType>(curRCs);
		newRCs.addAll(changeRC.getAddedRCNodeIDs());
		newRCs.removeAll(changeRC.getDeletedRCNodeIDs());
		// will use the nodeConfig before the change below.
		if (changeRC.newlyAddedNodes != null || changeRC.deletedNodes != null)
			this.initiateReconfiguration(
					AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString(),
					newRCs, // this.consistentNodeConfig.getNodeSocketAddress
					(changeRC.getRequester()), null, changeRC.newlyAddedNodes);
		return null;
	}

	/*
	 * Reconfiguration is initiated using a callback because the intent to
	 * conduct a reconfiguration must be persistently committed before
	 * initiating the reconfiguration. Otherwise, the failure of say the
	 * initiating reconfigurator can leave an active replica group stopped
	 * indefinitely. Exactly one reconfigurator, the one that proposes the
	 * request initiating reconfiguration registers the callback. This
	 * initiating reconfigurator will spawn a WaitAckStopEpoch task when the
	 * initiating request is locally executed. The other replicas only spawn a
	 * WaitPrimaryExecution task as a double check that the initiating
	 * reconfigurator does complete the reconfiguration; if it does not, they
	 * will follow up with their own attempt after a timeout. This works because
	 * all three steps: WaitAckStopEpoch, WaitAckStartEpoch, and
	 * WaitAckDropEpoch are idempotent operations.
	 * 
	 * A reconfiguration attempt can still get stuck if all reconfigurators
	 * crash or the only reconfigurators that committed the intent crash. So, a
	 * replica recovery procedure should ensure that replicas eventually execute
	 * committed but unexecuted requests. This naturally happens with paxos.
	 */
	@Override
	public void executed(InterfaceRequest request, boolean handled) {
		BasicReconfigurationPacket<?> rcPacket = null;
		try {
			rcPacket = ReconfigurationPacket.getReconfigurationPacket(request,
					getUnstringer());
		} catch (JSONException e) {
			e.printStackTrace();
		}
		if (rcPacket == null
				|| !rcPacket.getType().equals(
						ReconfigurationPacket.PacketType.RC_RECORD_REQUEST))
			return;
		@SuppressWarnings("unchecked")
		// checked right above
		RCRecordRequest<NodeIDType> rcRecReq = (RCRecordRequest<NodeIDType>) rcPacket;

		log.info(this + " executing executed() callback for " + rcRecReq);
		// handled is true when reconfiguration intent causes state change
		if (handled && rcRecReq.isReconfigurationIntent()
				&& !rcRecReq.isNodeConfigChange()) {
			// if I initiated this, spawn reconfiguration task
			if (rcRecReq.startEpoch.getInitiator().equals(getMyID())
			// but spawn anyway for my RC group reconfigurations
					|| (this.DB.isRCGroupName(rcRecReq.getServiceName()) && rcRecReq.startEpoch
							.getCurEpochGroup().contains(getMyID())))
				this.spawnPrimaryReconfiguratorTask(rcRecReq);
			// else I am secondary, so wait for primary's execution
			else if (!this.DB.isRCGroupName(rcRecReq.getServiceName()))
				this.spawnSecondaryReconfiguratorTask(rcRecReq);
			// record intent commit and cancel task
			if (this.DB.isRCGroupName(rcRecReq.getServiceName()))
				this.removePendingRCTask(this.protocolExecutor.remove(this
						.getCommitTaskKey(rcRecReq)));
		} else if (handled
				&& (rcRecReq.isReconfigurationComplete() || rcRecReq
						.isDeleteConfirmation())) {
			// send delete confirmation to client
			if (rcRecReq.isDeleteConfirmation())
				sendDeleteConfirmationToClient(rcRecReq);
			// send response back to initiator
			else if (rcRecReq.isReconfigurationComplete()
					&& rcRecReq.isNodeConfigChange()) {
				this.sendRCReconfigurationConfirmationToInitiator(rcRecReq);
			}
			/*
			 * If reconfiguration is complete, remove any previously spawned
			 * secondary tasks for the same reconfiguration. We do not remove
			 * WaitAckDropEpoch here because that might still be waiting for
			 * drop ack messages. If they don't arrive in a reasonable period of
			 * time, WaitAckDropEpoch is designed to self-destruct. But we do
			 * remove all tasks corresponding to the previous epoch at this
			 * point.
			 */
			this.garbageCollectPendingTasks(rcRecReq);
		} else if (handled && rcRecReq.isReconfigurationIntent()
				&& rcRecReq.isNodeConfigChange()) {
			// initiate the process of reconfiguring RC groups here
			executeNodeConfigChange(rcRecReq);
		} else if (handled && rcRecReq.isReconfigurationMerge())
			this.protocolExecutor.spawnIfNotRunning(new WaitAckDropEpoch<NodeIDType>(
					rcRecReq.startEpoch, this.DB));
	}

	/****************************** End of protocol task handler methods *********************/

	/*********************** Private methods below **************************/

	private void spawnPrimaryReconfiguratorTask(
			RCRecordRequest<NodeIDType> rcRecReq) {
		/*
		 * This assert follows from the fact that the return value handled can
		 * be true for a reconfiguration intent packet exactly once.
		 */
		assert (!this.isTaskRunning(this.getTaskKey(WaitAckStopEpoch.class,
				rcRecReq)));
		log.log(Level.INFO,
				MyLogger.FORMAT[8],
				new Object[] { this, "spawning WaitAckStopEpoch for",
						rcRecReq.startEpoch.getPrevGroupName(), ":",
						rcRecReq.getEpochNumber() - 1, "for starting",
						rcRecReq.getServiceName(), ":",
						rcRecReq.getEpochNumber() });
		boolean spawned = this.protocolExecutor
				.spawnIfNotRunning(new WaitAckStopEpoch<NodeIDType>(
						rcRecReq.startEpoch, this.DB));
		if (!spawned)
			log.info(this + " has running " + "WaitAckStopEpoch" + getMyID()
					+ " for " + rcRecReq.getSummary());
	}

	private void spawnSecondaryReconfiguratorTask(
			RCRecordRequest<NodeIDType> rcRecReq) {
		/*
		 * This assert follows from the fact that the return value handled can
		 * be true for a reconfiguration intent packet exactly once.
		 */
		assert (!this.isTaskRunning(this.getTaskKey(WaitPrimaryExecution.class,
				rcRecReq)));

		log.log(Level.INFO, MyLogger.FORMAT[3],
				new Object[] { this, " spawning WaitPrimaryExecution for ",
						rcRecReq.getServiceName(),
						rcRecReq.getEpochNumber() - 1 });
		/*
		 * If nodeConfig is under flux, we could be wrong on the set of peer
		 * reconfigurators below, but this information is only used to get
		 * confirmation from the primary, so in the worst case, the secondary
		 * will not hear from any primary and will itself complete the
		 * reconfiguration, which will be consistent thanks to paxos.
		 */
		this.protocolExecutor.schedule(new WaitPrimaryExecution<NodeIDType>(
				getMyID(), rcRecReq.startEpoch, this.DB,
				this.consistentNodeConfig.getReplicatedReconfigurators(rcRecReq
						.getServiceName())));
	}

	private boolean removePendingRCTask(ProtocolTask<?, ?, ?> task) {
		if (task == null)
			return false;
		return this.DB.removeRCTask((String) task.getKey());
	}

	private boolean isTaskRunning(String key) {
		return this.protocolExecutor.isRunning(key);
	}

	/*
	 * Check for and invoke reconfiguration policy. The reconfiguration policy
	 * is in AbstractDemandProfile and by design only deals with IP addresses,
	 * not node IDs, so we have utility methods in ConsistentNodeConfig to go
	 * back and forth between collections of NodeIDType and InetAddress taking
	 * into account the many-to-one mapping from the former to the latter. A
	 * good reconfiguration policy should try to return a set of IPs that only
	 * minimally modifies the current set of IPs; if so, ConsistentNodeConfig
	 * will ensure a similar property for the corresponding NodeIDType set.
	 * 
	 * If nodeConfig is under flux, this will affect the selection of actives,
	 * but not correctness.
	 */
	private Set<NodeIDType> shouldReconfigure(String name) {
		// return null if no current actives
		Set<NodeIDType> oldActives = this.DB.getActiveReplicas(name);
		if (oldActives == null || oldActives.isEmpty())
			return null;
		// get new IP addresses (via consistent hashing if no oldActives
		ArrayList<InetAddress> newActiveIPs = this.demandProfiler
				.testAndSetReconfigured(name,
						this.consistentNodeConfig.getNodeIPs(oldActives));
		assert (newActiveIPs != null);
		// get new actives based on new IP addresses
		Set<NodeIDType> newActives = this.consistentNodeConfig
				.getIPToActiveReplicaIDs(newActiveIPs, oldActives);
		return (newActives.equals(oldActives)) ? null : newActives;
	}

	// combine json stats from report into existing demand profile
	private void updateDemandProfile(DemandReport<NodeIDType> report) {
		// if no entry for name, try to read and refresh from DB
		if (!this.demandProfiler.contains(report.getServiceName())) {
			String statsStr = this.DB.getDemandStats(report.getServiceName());
			JSONObject statsJSON = null;
			try {
				if (statsStr != null)
					statsJSON = new JSONObject(statsStr);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			if (statsJSON != null)
				this.demandProfiler.putIfEmpty(AbstractDemandProfile
						.createDemandProfile(statsJSON));
		}
		this.demandProfiler.combine(AbstractDemandProfile
				.createDemandProfile(report.getStats()));
	}

	/*
	 * Stow away to disk if the size of the memory map becomes large. We will
	 * refresh in the updateDemandProfile method if needed.
	 */
	private void trimAggregateDemandProfile() {
		Set<AbstractDemandProfile> profiles = this.demandProfiler.trim();
		for (AbstractDemandProfile profile : profiles) {
			// initiator and epoch are irrelevant in this report
			DemandReport<NodeIDType> report = new DemandReport<NodeIDType>(
					this.getMyID(), profile.getName(), 0, profile);
			// will update stats in DB
			this.DB.handleRequest(report);
		}
	}

	private void initiateReconfiguration(String name, Set<NodeIDType> newActives) {
		this.initiateReconfiguration(name, newActives, null, null, null);
	}

	// coordinate reconfiguration intent
	private boolean initiateReconfiguration(String name,
			Set<NodeIDType> newActives, InetSocketAddress sender,
			String initialState,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes) {
		if (newActives == null)
			return false;
		// request to persistently log the intent to reconfigure
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
				this.getMyID(), formStartEpoch(name, newActives, sender,
						initialState, newlyAddedNodes),
				RequestTypes.RECONFIGURATION_INTENT);
		// coordinate intent with replicas
		if (this.isReadyForReconfiguration(rcRecReq))
			return this.DB.handleIncoming(rcRecReq);
		return false;
	}

	/*
	 * We check for ongoing reconfigurations to avoid multiple paxos
	 * coordinations by different nodes each trying to initiate a
	 * reconfiguration. Although only one will succeed at the end, it is still
	 * useful to limit needless paxos coordinated requests. Nevertheless, one
	 * problem with the check in this method is that multiple nodes can still
	 * try to initiate a reconfiguration as it only checks based on the DB
	 * state. Ideally, some randomization should make the likelihood of
	 * redundant concurrent reconfigurations low.
	 * 
	 * It is not important for this method to be atomic. Even if an RC group or
	 * a service name reconfiguration is initiated concurrently with the ready
	 * checks, paxos ensures that no more requests can be committed after the
	 * group has been stopped. If the group becomes non-ready immediately after
	 * this method returns true, the request for which this method is being
	 * called will either not get committed or be rendered a no-op.
	 */
	private boolean isReadyForReconfiguration(
			BasicReconfigurationPacket<NodeIDType> rcPacket) {
		ReconfigurationRecord<NodeIDType> recordServiceName = this.DB
				.getReconfigurationRecord(rcPacket.getServiceName());
		ReconfigurationRecord<NodeIDType> recordGroupName = this.DB
				.getReconfigurationRecord(this.DB.getRCGroupName(rcPacket
						.getServiceName()));
		assert (recordGroupName != null) : this.DB.getRCGroupName(rcPacket
				.getServiceName());
		/*
		 * We need to check both if the RC group record is ready and the service
		 * name record is either also ready or null (possible during name
		 * creation).
		 */
		boolean ready = recordGroupName != null
				&& recordGroupName.getState().equals(RCStates.READY)
				&& (recordServiceName == null || recordServiceName.getState()
						.equals(RCStates.READY));
		if (!ready)
			log.log(Level.INFO, MyLogger.FORMAT[3], new Object[] {
					this,
					"not ready for reconfiguring",
					rcPacket.getServiceName(),
					"\n  recordServiceName = " + recordServiceName
							+ "\n  recordGroupName = " + recordGroupName });
		return ready;
	}

	private NodeIDType getMyID() {
		return this.messenger.getMyID();
	}

	private Stringifiable<NodeIDType> getUnstringer() {
		return this.consistentNodeConfig;
	}

	private StartEpoch<NodeIDType> formStartEpoch(String name,
			Set<NodeIDType> newActives, InetSocketAddress sender,
			String initialState,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes) {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(name);
		StartEpoch<NodeIDType> startEpoch = (record != null) ? new StartEpoch<NodeIDType>(
				getMyID(), name, record.getEpoch() + 1, newActives,
				record.getActiveReplicas(record.getName(), record.getEpoch()),
				sender, initialState, newlyAddedNodes)
				: new StartEpoch<NodeIDType>(getMyID(), name, 0, newActives,
						null, sender, initialState, newlyAddedNodes);
		return startEpoch;
	}

	private StartEpoch<NodeIDType> formStartEpoch(String name,
			Set<NodeIDType> newActives, InetSocketAddress sender,
			String initialState) {
		return this
				.formStartEpoch(name, newActives, sender, initialState, null);
	}

	/************ Start of key construction utility methods *************/
	private String getCommitTaskKey(RCRecordRequest<?> rcPacket) {
		return getCommitTaskKey(rcPacket, getMyID().toString());
	}

	public static String getCommitTaskKey(RCRecordRequest<?> rcPacket,
			String myID) {
		return WaitCoordinatedCommit.class.getSimpleName()
				+ rcPacket.getRCRequestTypeCompact() + myID + ":"
				+ rcPacket.getServiceName() + ":" + rcPacket.getEpochNumber();
	}

	private String getTaskKey(Class<?> C, BasicReconfigurationPacket<?> rcPacket) {
		return getTaskKey(C, rcPacket, getMyID().toString());
	}

	public static String getTaskKey(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket, String myID) {
		return C.getSimpleName() + myID + ":" + rcPacket.getServiceName() + ":"
				+ rcPacket.getEpochNumber();
	}

	private String getTaskKeyPrev(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket) {
		return getTaskKeyPrev(C, rcPacket, getMyID().toString());
	}

	protected static String getTaskKeyPrev(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket, String myID) {
		return C.getSimpleName() + myID + ":" + rcPacket.getServiceName() + ":"
				+ (rcPacket.getEpochNumber() - 1);
	}

	/************ End of key construction utility methods *************/

	/*
	 * Remove all obviated tasks upon reconfiguration complete commit.
	 * 
	 * FIXME: We need to handle the case of epoch jumps better. A task may not
	 * get garbage collected below if the task doesn't naturally terminate and
	 * the replica jumps more than one epoch forward. This would be rare but can
	 * happen. In these cases, the task will still eventually terminate but will
	 * take longer at least until the restart period. So a large flurry of
	 * reconfigurations coupled with replica failures could end up using a large
	 * amount of memory for pending (but unnecessary) tasks.
	 */
	private void garbageCollectPendingTasks(RCRecordRequest<NodeIDType> rcRecReq) {
		// remove commit start epoch task
		this.removePendingRCTask(this.protocolExecutor.remove(this
				.getCommitTaskKey(rcRecReq)));
		// remove secondary task, primary will take care of itself
		this.protocolExecutor.remove(getTaskKey(WaitPrimaryExecution.class,
				rcRecReq));

		// remove previous epoch's primary in case it exists here
		this.protocolExecutor.remove(getTaskKeyPrev(WaitPrimaryExecution.class,
				rcRecReq));
		// don't garbage collect WaitAckDrop as it should clean up after itself
	}

	// just before coordinating reconfiguration complete
	private void garbageCollectStopAndStartTasks(
			RCRecordRequest<NodeIDType> rcRecReq) {
		// stop task is obviated just before reconfiguration complete propose
		this.protocolExecutor.remove(this.getTaskKeyPrev(
				WaitAckStopEpoch.class, rcRecReq));
		// start task is obviated just before reconfiguration complete propose
		this.protocolExecutor.remove(this.getTaskKey(WaitAckStartEpoch.class,
				rcRecReq));

		// remove previous epoch's start task in case it exists here
		this.protocolExecutor.remove(this.getTaskKeyPrev(
				WaitAckStopEpoch.class, rcRecReq));
		// remove previous epoch's start task in case it exists here
		this.protocolExecutor.remove(this.getTaskKeyPrev(
				WaitAckStartEpoch.class, rcRecReq));

	}

	/*
	 * FIXME: This method is designed for regular RC records, and may not work
	 * for RC group records or the NC record. Furthermore, redoing the intent
	 * can fail if the intent is already committed. Need to check if this is the
	 * case.
	 */
	private void finishPendingReconfigurations() {
		String[] pending = this.DB.getPendingReconfigurations();
		for (String name : pending) {
			ReconfigurationRecord<NodeIDType> record = this.DB
					.getReconfigurationRecord(name);
			/*
			 * Note; The fact that the RC record request is an intent is
			 * immaterial. It is really only used to construct the corresponding
			 * WaitAckStopEpoch task, i.e., the intent itself will not be
			 * committed again (and indeed can not be by design).
			 */
			RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
					this.getMyID(), this.formStartEpoch(name,
							record.getNewActives(), null, null),
					RCRecordRequest.RequestTypes.RECONFIGURATION_INTENT);
			/*
			 * We spawn primary even though that may be unnecessary because we
			 * don't know if or when any other reconfigurator might finish this
			 * pending reconfiguration. Having multiple reconfigurators push a
			 * reconfiguration is okay as stop, start, and drop are all
			 * idempotent operations.
			 */
			this.spawnPrimaryReconfiguratorTask(rcRecReq);
		}
	}

	private void sendCreationError(CreateServiceName create) {
		try {
			this.messenger.sendToAddress(
					create.getSender(),
					new CreateServiceName(create.getInitiator(), create
							.getServiceName(), create.getEpochNumber(),
							CreateServiceName.Keys.NO_OP.toString())
							.toJSONObject());
		} catch (IOException | JSONException e) {
			log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	private void sendDeletionError(DeleteServiceName delete) {
		try {
			this.messenger.sendToAddress(delete.getSender(), delete.setFailed()
					.toJSONObject());
		} catch (IOException | JSONException e) {
			log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	private void sendDeleteConfirmationToClient(
			RCRecordRequest<NodeIDType> rcRecReq) {
		try {
			if (rcRecReq.startEpoch.creator != null)
				this.messenger.sendToAddress(
						rcRecReq.startEpoch.creator,
						new DeleteServiceName(rcRecReq.startEpoch.creator,
								rcRecReq.getServiceName(), rcRecReq
										.getEpochNumber() - 1).toJSONObject());
		} catch (IOException | JSONException e) {
			log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	private void sendRCReconfigurationConfirmationToInitiator(
			RCRecordRequest<NodeIDType> rcRecReq) {
		try {
			log.log(Level.INFO, MyLogger.FORMAT[2], new Object[] { this,
					"sending ReconfigureRCNodeConfig confirmation to",
					rcRecReq.startEpoch.creator });
			this.messenger.sendToAddress(
					rcRecReq.startEpoch.creator,
					new ReconfigureRCNodeConfig<NodeIDType>(this.DB.getMyID(),
							rcRecReq.startEpoch.newlyAddedNodes, this.diff(
									rcRecReq.startEpoch.prevEpochGroup,
									rcRecReq.startEpoch.curEpochGroup))
							.toJSONObject());
		} catch (IOException | JSONException e) {
			log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	/*************** Reconfigurator reconfiguration related methods ***************/
	// return s1 - s2
	private Set<NodeIDType> diff(Set<NodeIDType> s1, Set<NodeIDType> s2) {
		Set<NodeIDType> diff = new HashSet<NodeIDType>();
		for (NodeIDType node : s1)
			if (!s2.contains(node))
				diff.add(node);
		return diff;
	}

	/*
	 * This method conducts the actual reconfiguration assuming that the
	 * "intent" has already been committed in the NC record. Spawning each
	 * constituent reconfiguration is equivalent to executing the corresponding
	 * reconfiguration intent commit, i.e., spawning WaitAckStop etc. It is not
	 * important to worry about "completing" the NC change intent under failures
	 * as paxos will ensure safety. We do need a trigger to indicate the
	 * completion of all constituent reconfigurations so that the NC record
	 * change can be considered and marked as complete. For this, upon every
	 * reconfiguration complete commit, we could simply check if any of the new
	 * groups are still pending and if not, consider the NC change as complete.
	 * That is what we do (in AbstractReconfiguratorDB.handleRCRecordRequest(.).
	 */
	private boolean executeNodeConfigChange(RCRecordRequest<NodeIDType> rcRecReq) {
		boolean allDone = true;

		// change soft copy of node config
		boolean ncChanged = changeSoftNodeConfig(rcRecReq.startEpoch);
		// change persistent copy of node config
		ncChanged = ncChanged && this.changeDBNodeConfig(rcRecReq.startEpoch);
		if (!ncChanged)
			throw new RuntimeException("Unable to change node config");

		// to track epoch numbers of RC groups correctly
		Set<NodeIDType> affectedNodes = this.DB.setRCEpochs(
				rcRecReq.startEpoch.getNewlyAddedNodes(),
				diff(rcRecReq.startEpoch.prevEpochGroup,
						rcRecReq.startEpoch.curEpochGroup));

		allDone = this.changeSplitMergeGroups(
				affectedNodes,
				rcRecReq.startEpoch.getNewlyAddedNodes(),
				diff(rcRecReq.startEpoch.prevEpochGroup,
						rcRecReq.startEpoch.curEpochGroup));

		// wait to commit all done, no coordination needed
		this.DB.handleRequest(new RCRecordRequest<NodeIDType>(rcRecReq
				.getInitiator(), rcRecReq.startEpoch,
				RCRecordRequest.RequestTypes.RECONFIGURATION_COMPLETE));

		/*
		 * We need to checkpoint the NC record after every NC change. Unlike
		 * other records for RC groups where we can roll forward quickly by
		 * simply applying state changes specified in the logged decisions
		 * (instead of actually re-conducting the corresponding
		 * reconfigurations), NC group changes are more complex and have to be
		 * re-conducted at each node redundantly, however that may not even be
		 * possible as deleted nodes or even existing nodes may no longer have
		 * the final state corresponding to older epochs. Checkpointing after
		 * every NC change ensures that, upon recovery, each node has to try to
		 * re-conduct at most only the most recent NC change.
		 * 
		 * What if this forceCheckpoint operation fails? If the next NC change
		 * successfully completes at this node before the next crash, there is
		 * no problem. Else, upon recovery, this node will try to re-conduct the
		 * NC change corresponding to the failed forceCheckpoint and might be
		 * unable to do so. This is equivalent to this node having missed long 
		 * past NC changes. At this point, this node must be deleted and
		 * re-added to NC.
		 */
		this.DB.forceCheckpoint(rcRecReq.getServiceName());

		// all done
		return allDone;
	}

	// change soft copy of node config
	private boolean changeSoftNodeConfig(StartEpoch<NodeIDType> startEpoch) {
		/*
		 * Do adds immediately. This means that if we ever need the old
		 * "world view" again, e.g., to know which group a name maps to, we have
		 * to reconstruct the consistent hash ring on demand based on the old
		 * set of nodes in the DB. We could optimize this slightly by just
		 * storing also an in-memory copy of the old consistent hash ring, but
		 * this is probably unnecessary given that nodeConfig changes are rare,
		 * slow operations anyway.
		 */
		if (startEpoch.hasNewlyAddedNodes())
			for (Map.Entry<NodeIDType, InetSocketAddress> entry : startEpoch.newlyAddedNodes
					.entrySet()) {
				this.consistentNodeConfig.addReconfigurator(entry.getKey(),
						entry.getValue());
			}
		/*
		 * Deletes, not so fast. If we delete entries from nodeConfig right
		 * away, we don't have those nodes' socket addresses, so we can't
		 * communicate with them any more, but we need to be able to communicate
		 * with them in order to do the necessary reconfigurations to cleanly
		 * eliminate them from the consistent hash ring.
		 */
		for (NodeIDType node : this.diff(startEpoch.prevEpochGroup,
				startEpoch.curEpochGroup)) {
			this.consistentNodeConfig.slateForRemovalReconfigurator(node);
		}
		return true;
	}

	// change persistent copy of node config
	private boolean changeDBNodeConfig(StartEpoch<NodeIDType> startEpoch) {
		return this.DB.changeDBNodeConfig(startEpoch.getEpochNumber());
	}

	// FIXME: derive and include provably correct constraints here
	private boolean isPermitted(ReconfigureRCNodeConfig<NodeIDType> changeRC) {
		return changeRC.getDeletedRCNodeIDs().size() < this.consistentNodeConfig
				.getReplicatedReconfigurators("0").size();
	}

	private boolean amAffected(Set<NodeIDType> addNodes,
			Set<NodeIDType> deleteNodes) {
		boolean affected = false;
		for (NodeIDType node : addNodes)
			if (this.DB.amAffected(node))
				affected = true;
		for (NodeIDType node : deleteNodes)
			if (this.DB.amAffected(node))
				affected = true;
		return affected;
	}

	private boolean changeSplitMergeGroups(Set<NodeIDType> affectedNodes,
			Set<NodeIDType> addNodes, Set<NodeIDType> deleteNodes) {
		if (!amAffected(addNodes, deleteNodes))
			return false;

		// get list of current RC groups from DB.
		Map<String, Set<NodeIDType>> curRCGroups = this.DB.getOldRCGroups();
		// get list of new RC groups from NODE_CONFIG record in DB
		Map<String, Set<NodeIDType>> newRCGroups = this.DB.getNewRCGroups();
		// get NC record from DB
		ReconfigurationRecord<NodeIDType> ncRecord = this.DB
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString());

		String changed = this.changeExistingGroups(curRCGroups, newRCGroups,
				ncRecord, affectedNodes);
		String split = this.splitExistingGroups(curRCGroups, newRCGroups,
				ncRecord);
		String merged = this.mergeExistingGroups(curRCGroups, newRCGroups,
				ncRecord);

		log.info(changed + split + merged);
		return !(changed + split + merged).isEmpty();
	}

	private boolean isPresent(String rcGroupName, Set<NodeIDType> affectedNodes) {
		for (NodeIDType node : affectedNodes) {
			if (this.DB.getRCGroupName(node).equals(rcGroupName))
				return true;
		}
		return false;
	}

	/*
	 * This method reconfigures groups that exist locally both in the old and
	 * new rings, i.e., this node just has to do a standard reconfiguration
	 * operation because the membership of the paxos group is changing.
	 */
	private String changeExistingGroups(
			Map<String, Set<NodeIDType>> curRCGroups,
			Map<String, Set<NodeIDType>> newRCGroups,
			ReconfigurationRecord<NodeIDType> ncRecord,
			Set<NodeIDType> affectedNodes) {
		String debug = ""; // just for prettier clustered printing
		// for each new group, initiate group change if and as needed
		for (String newRCGroup : newRCGroups.keySet()) {
			if (curRCGroups.keySet().contains(newRCGroup)) {
				if (!isPresent(newRCGroup, affectedNodes))
					continue;

				int ncEpoch = ncRecord.getRCEpoch(newRCGroup);
				// change current group
				debug += (this + " changing current group {" + newRCGroup + ":"
						+ (ncEpoch - 1) + "=" + newRCGroups.get(newRCGroup)
						+ "} to {" + newRCGroup + ":" + (ncEpoch) + "=" + newRCGroups
							.get(newRCGroup)) + "}\n";
				this.repeatUntilObviated(new RCRecordRequest<NodeIDType>(this
						.getMyID(), new StartEpoch<NodeIDType>(this.getMyID(),
						newRCGroup, ncEpoch, newRCGroups.get(newRCGroup),
						curRCGroups.get(newRCGroup)),
						RequestTypes.RECONFIGURATION_INTENT));
			}
		}
		return debug;
	}

	private void repeatUntilObviated(RCRecordRequest<NodeIDType> rcRecReq) {
		this.DB.coordinateRequestSuppressExceptions(rcRecReq);
		this.protocolExecutor
				.spawnIfNotRunning(new WaitCoordinatedCommit<NodeIDType>(
						rcRecReq, this.DB));
	}

	/*
	 * This method "reconfigures" groups that will exist locally in the new ring
	 * but do not currently exist in the old ring. This "reconfiguration" is
	 * actually a group split operation, wherein an existing group is stopped
	 * and two new groups are created by splitting the final state of the
	 * stopped group, one with membership identical to the stopped group and the
	 * other corresponding to the new but currently non-existent group. A
	 * detailed example is described below.
	 */
	private String splitExistingGroups(
			Map<String, Set<NodeIDType>> curRCGroups,
			Map<String, Set<NodeIDType>> newRCGroups,
			ReconfigurationRecord<NodeIDType> ncRecord) {
		String debug = ""; // just for prettier clustered printing
		// for each new group, initiate group change if and as needed
		for (String newRCGroup : newRCGroups.keySet()) {
			if (!curRCGroups.keySet().contains(newRCGroup)) {
				/*
				 * Create new group from scratch by splitting existing group.
				 * 
				 * Example: Suppose we have nodes Y, Z, A, C, D, E as
				 * consecutive RC nodes along the ring and we add B between A
				 * and C, and all groups are of size 3. Then, the group BCD is a
				 * new group getting added at nodes B, C, and D. This new group
				 * BCD must obtain state from the existing group CDE, i.e., the
				 * group CDE is getting split into two groups, BCD and CDE. One
				 * way to accomplish creation of the group BCD is to specify the
				 * previous group as CDE and just select the subset of state
				 * that gets remapped to BCD as the initial state. Below, we
				 * just acquire all of CDE's final state and simply choose what
				 * belongs to BCD while updating BCD's state at replica group
				 * creation time.
				 * 
				 * This operation will happen at C, and D, but not at B and E
				 * because E has no new group BCD that is not part of its
				 * existing groups, and B has nothing at all, not even a node
				 * config.
				 */
				Map<String, Set<NodeIDType>> oldGroup = this.DB
						.getOldGroup(newRCGroup);
				assert (oldGroup != null && oldGroup.size() == 1);
				String oldGroupName = oldGroup.keySet().iterator().next();
				debug += this + " creating new group {" + newRCGroup + ":"
						+ ncRecord.getRCEpoch(newRCGroup) + "="
						+ newRCGroups.get(newRCGroup) + "} by splitting {"
						+ oldGroupName + ":"
						+ ncRecord.getRCEpoch(oldGroupName) + "="
						+ oldGroup.get(oldGroupName) + "}\n";
				if (this.DB.getEpoch(oldGroupName) == null)
					log.warning(this + " can not find supposedly local group "
							+ oldGroupName);
				/*
				 * Uncoordinated coz no group yet exists. But we need to make
				 * sure that the new group does indeed not yet exist and the old
				 * group is at the right epoch. ncEpoch comes to rescue.
				 */
				this.DB.handleRequest(new RCRecordRequest<NodeIDType>(this
						.getMyID(), new StartEpoch<NodeIDType>(this.getMyID(),
						newRCGroup, ncRecord.getRCEpoch(newRCGroup),
						newRCGroups.get(newRCGroup),
						oldGroup.get(oldGroupName), oldGroupName, false,
						ncRecord.getRCEpoch(oldGroupName) - 1),
						RequestTypes.RECONFIGURATION_INTENT));
			}
		}
		return debug;
	}

	/*
	 * This method "reconfigures" groups that will not exist locally in the new
	 * ring but do currently exist locally in the old ring. This
	 * "reconfiguration" is actually a group merge operation, wherein the old
	 * "mergee" group is stopped, the group which with the old group is supposed
	 * to merge (and will continue to exist locally in the new ring) is stopped,
	 * and the mergee group's final state is merged into the latter group simply
	 * through a paxos update operation. A detailed example and a discussion of
	 * relevant concerns is described below.
	 */
	private String mergeExistingGroups(
			Map<String, Set<NodeIDType>> curRCGroups,
			Map<String, Set<NodeIDType>> newRCGroups,
			ReconfigurationRecord<NodeIDType> ncRecord) {
		/*
		 * Delete groups that no longer should exist at this node.
		 * 
		 * Example: Suppose we have nodes Y, Z, A, B, C, D, E as consecutive RC
		 * nodes along the ring and we are removing B between A and C, and all
		 * groups are of size 3.
		 * 
		 * Basic idea: For each node being deleted, if I belong to the deleted
		 * node's group, I need to reconfigure the deleted node's group by
		 * merging it with the node in the new ring to which the deleted node
		 * hashes.
		 * 
		 * In the example above, we need to remove group B at C by changing BCD
		 * to CDE. Likewise, at nodes D and E, we need to change group BCD to
		 * CDE.
		 * 
		 * C: BCD -> CDE (merge)
		 * 
		 * A merge is implemented as a reconfiguration that starts with
		 * WaitAckStopEpoch for the old group, but instead of starting the new
		 * group, it simply calls updateState on the new group to merge the
		 * stopped mergee group's final state into the new group.
		 * 
		 * Furthermore, the group ZAC is a new group getting added at node C
		 * because of the removal of B. There is no current group at C that
		 * needs to be stopped, however, one does need to stop the old group ZAB
		 * in order to reconfigure it to ZAC. One issue is that C doesn't even
		 * know ZAB's epoch number as the group doesn't exist locally at C. So
		 * we just let one of Z or A, not C, reconfigure ZAB in this case.
		 * 
		 * What if we are deleting B1, B2, and B3 from Y, Z, A, B1, B2, B3, C,
		 * D, E? The group ZAC has to get created at C, which can still be done
		 * by Z or A. Similarly, AB1B2 can be moved to ACD by A. However, B1B2B3
		 * can not be moved to CDE at C because CDE has to merge B1B2B3, B2B3C,
		 * and B3CD. C can conduct the latter two merges but not the first. To
		 * merge B1B2B3, at least one of B1, B2, or B3 must be up. The only
		 * compelling reason to delete all three of B1,B2, and B3 together is
		 * that they are all down, but in that case we can not delete them
		 * anyway until at least one of them comes back up. So we can delete at
		 * most as many nodes as the size of the reconfigurator replica group.
		 * 
		 * FIXME: Actually, the exact condition is weaker (something like we can
		 * delete at most as many consecutive nodes as the size of the
		 * reconfigurator replica group, but we need to formally prove the
		 * necessity/sufficiency of this constraint).
		 */

		String debug = "";
		for (String curRCGroup : curRCGroups.keySet()) {
			if (!newRCGroups.containsKey(curRCGroup)
					&& this.DB.isBeingDeleted(curRCGroup)) {
				Map<String, Set<NodeIDType>> mergeGroup = this.DB
						.getNewGroup(curRCGroup);
				assert (mergeGroup != null && mergeGroup.size() == 1);
				String mergeGroupName = mergeGroup.keySet().iterator().next();

				/*
				 * mergeGroupName must be in my new groups and curRCGroup must
				 * exist locally. The latter is needed in order to know the
				 * epoch number of the group being merged. In the running
				 * example above, E does not satisfy both conditions because the
				 * mergeGroupName CDE exists at E but the mergee group BCD
				 * doesn't exist at E, so it is not in a position to conduct the
				 * reconfiguration (as it doesn't know which BCD epoch to stop
				 * and merge into CDE), so just one of C or D will conduct the
				 * merge in this case.
				 */
				if (!newRCGroups.containsKey(mergeGroupName)
						|| this.DB.getEpoch(curRCGroup) == null)
					continue;

				// delete current group and merge into a new "mergeGroup"
				debug += (this + " merging current group {" + curRCGroup + ":" + this.DB
						.getReplicaGroup(curRCGroup))
						+ "} with {"
						+ mergeGroupName
						+ ":"
						+ (ncRecord.getRCEpoch(mergeGroupName))
						+ "="
						+ mergeGroup.get(mergeGroupName) + "}\n";

				/*
				 * Spawn WaitAckStopEpoch directly, no intent commit needed. We
				 * know curRCGroup's epoch number here as per the check above.
				 */
				this.protocolExecutor.spawn(new WaitAckStopEpoch<NodeIDType>(
						new StartEpoch<NodeIDType>(this.getMyID(),
								mergeGroupName, ncRecord
										.getRCEpoch(mergeGroupName), mergeGroup
										.get(mergeGroupName), curRCGroups
										.get(curRCGroup), curRCGroup, true,
								ncRecord.getRCEpoch(curRCGroup)), this.DB));
			} else if (!newRCGroups.containsKey(curRCGroup)
					&& !this.DB.isBeingDeleted(curRCGroup)) {
				// delete current group and merge into a new "mergeGroup"
				debug += (this + " expecting others to delete current group {"
						+ curRCGroup + ":" + ncRecord.getRCEpoch(curRCGroup)
						+ "=" + this.DB.getReplicaGroup(curRCGroup))
						+ "}\n";
			}
		}
		return debug;
	}
}