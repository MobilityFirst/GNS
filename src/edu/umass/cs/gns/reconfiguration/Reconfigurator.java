package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.ReconfiguratorProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.WaitAckDropEpoch;
import edu.umass.cs.gns.reconfiguration.json.WaitAckStartEpoch;
import edu.umass.cs.gns.reconfiguration.json.WaitAckStopEpoch;
import edu.umass.cs.gns.reconfiguration.json.WaitCommitStartEpoch;
import edu.umass.cs.gns.reconfiguration.json.WaitPrimaryExecution;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RCRecordRequest.RequestTypes;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.AggregateDemandProfiler;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.gns.util.MyLogger;
import edu.umass.cs.gns.util.Stringifiable;

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
		this.consistentNodeConfig = new ConsistentReconfigurableNodeConfig<NodeIDType>(nc);
		this.protocolExecutor = new ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String>(
				messenger);
		this.protocolTask = new ReconfiguratorProtocolTask<NodeIDType>(
				getMyID(), this);
		this.protocolExecutor.register(this.protocolTask.getDefaultTypes(),
				this.protocolTask); // non default types will be registered by spawned tasks
		this.DB = new RepliconfigurableReconfiguratorDB<NodeIDType>(
				//new InMemoryReconfiguratorDB<NodeIDType>(this.messenger.getMyID(),
				new DerbyPersistentReconfiguratorDB<NodeIDType>(this.messenger.getMyID(),
						this.consistentNodeConfig), getMyID(),
				this.consistentNodeConfig, this.messenger);
		this.DB.setCallback(this);
		this.finishPendingReconfigurations();
	}
	
	public ActiveReplica<NodeIDType> getReconfigurableReconfiguratorAsActiveReplica() {
		return new ActiveReplica<NodeIDType>(this.DB, this.consistentNodeConfig, this.messenger);
	}

	@Override
	public boolean handleJSONObject(JSONObject jsonObject) {
		try {
			ReconfigurationPacket.PacketType rcType = ReconfigurationPacket
					.getReconfigurationPacketType(jsonObject);
			log.log(Level.INFO, MyLogger.FORMAT[3], new Object[]{this, "received", rcType, jsonObject});
			// try handling as reconfiguration packet through protocol task
			assert (rcType != null); // not "unchecked"
			@SuppressWarnings("unchecked")
			BasicReconfigurationPacket<NodeIDType> rcPacket = (BasicReconfigurationPacket<NodeIDType>) ReconfigurationPacket
					.getReconfigurationPacket(jsonObject, this.getUnstringer());
			if(!this.protocolExecutor.handleEvent(rcPacket)) {
				//this.DB.handleRequest(rcPacket, null); // ugly hack for lazy coordination packets
			}
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
		log.log(Level.FINEST, MyLogger.FORMAT[3], new Object[]{this, "received", report.getType(), report});
		if (this.DB.handleIncoming(report)) // possibly coordinated
			this.updateDemandProfile(report); // only upon no coordination
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
		log.log(Level.FINEST, MyLogger.FORMAT[3], new Object[]{this, "received", event.getType(), create});

		// commit initial "reconfiguration" intent
		this.initiateReconfiguration(create.getServiceName(),
				this.consistentNodeConfig.getReplicatedActives(create
						.getServiceName()), create.getSender(), create.getInitialState());
		return null;
	}
	
	/* Simply hand over DB request to DB. The only type of RC record 
	 * that can come here is one announcing reconfiguration completion.
	 * Reconfiguration initiation messages are derived locally and 
	 * coordinated through paxos, not received from outside.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleRCRecordRequest(
			RCRecordRequest<NodeIDType> rcRecReq,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(Level.FINEST, MyLogger.FORMAT[3], new Object[]{this, "received", rcRecReq.getType(), rcRecReq});
		// Could also be a previously coordinated request
		//this.DB.handleIncoming(rcRecReq); // FIXME: just handle locally, nothing else 
		if(rcRecReq.isReconfigurationComplete() || rcRecReq.isDeleteConfirmation()) {
			/* If reconfiguration is complete, remove any previously spawned 
			 * secondary tasks for the same reconfiguration. We do not remove
			 * WaitAckDropEpoch here because that might still be waiting for
			 * drop ack messages. If they don't arrive in a reasonable 
			 * period of time, WaitAckDropEpoch is designed to self-destruct.
			 * But we do remove all tasks corresponding to the previous 
			 * epoch at this point.
			 */
			this.protocolExecutor.spawn(new WaitCommitStartEpoch<NodeIDType>(
					rcRecReq, this.DB));
			this.garbageCollectPendingTasks(rcRecReq);
		}
		else assert(false);
		return null;
	}
		
	/* We need to ensure that both the stop/drop at actives happens atomically
	 * with the removal of the record at reconfigurators. To accomplish this,
	 * we first mark the record as stopped at reconfigurators, then wait 
	 * for the stop/drop tasks to finish, and finally coordinate the 
	 * completion notification so that reconfigurators can completely remove
	 * the record from their DB.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleDeleteServiceName(
			DeleteServiceName delete,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
				this.getMyID(), this.formStartEpoch(delete.getServiceName(), null, delete.getSender(), null),
				RequestTypes.REGISTER_RECONFIURATION_INTENT);
		// coordinate intent with replicas
		if(this.isReadyForReconfiguration(rcRecReq))
			this.DB.handleIncoming(rcRecReq); 		
		else log.log(Level.INFO, MyLogger.FORMAT[3], new Object[] {"Discarding ", rcRecReq.getSummary()});
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public GenericMessagingTask<NodeIDType, ?>[] handleRequestActiveReplicas(
			RequestActiveReplicas request,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(request.getServiceName());
		if(record==null) return null;
		//else 
		Set<NodeIDType> actives = record.getActiveReplicas();
		Set<InetSocketAddress> activeIPs = new HashSet<InetSocketAddress>();
		for (NodeIDType node : actives) {
			activeIPs.add(new InetSocketAddress(this.consistentNodeConfig
					.getNodeAddress(node), this.consistentNodeConfig
					.getNodePort(node)));
		}
		request.setActives(activeIPs);
		GenericMessagingTask<InetSocketAddress, ?> mtask = new GenericMessagingTask<InetSocketAddress, Object>(
				request.getSender(), request);
		return (GenericMessagingTask<NodeIDType, ?>[]) (mtask.toArray()); // FIXME:
	}
	
	/*
	 * Reconfiguration is initiated using a callback because the intent to
	 * conduct a reconfiguration must be persistently committed before
	 * initiating the reconfiguration. Otherwise, the failure of say the
	 * initiating reconfigurator can leave an active replica group stopped
	 * indefinitely. Exactly one reconfigurator, the one that proposes the
	 * request initiating reconfiguration registers the callback. This
	 * initiaiting reconfigurator will spawn a WaitAckStopEpoch task when the
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
				|| !rcPacket
						.getType()
						.equals(ReconfigurationPacket.PacketType.RC_RECORD_REQUEST))
			return;
		@SuppressWarnings("unchecked") // checked right above
		RCRecordRequest<NodeIDType> rcRecReq = (RCRecordRequest<NodeIDType>) rcPacket;
		
		// handled is true when reconfiguration intent causes state change
		if (handled && rcRecReq.isReconfigurationIntent()) {
			// if I initiated this, spawn reconfiguration task
			if (rcRecReq.startEpoch.getInitiator().equals(getMyID()))
				this.spawnPrimaryReconfiguratorTask(rcRecReq);
			// else I am secondary, so wait for primary's execution
			else
				this.spawnSecondaryReconfiguratorTask(rcRecReq);
		} else if (handled
				&& (rcRecReq.isReconfigurationComplete() || rcRecReq
						.isDeleteConfirmation())) {
			// remove commit start epoch task
			this.protocolExecutor.remove(this.getTaskKey(
					WaitCommitStartEpoch.class, rcPacket));
			if(rcRecReq.isDeleteConfirmation()) sendDeleteConfirmationToClient(rcRecReq);
		}
	}
	
	/****************************** End of protocol task handler methods *********************/

	/*********************** Private methods below **************************/

	private void spawnPrimaryReconfiguratorTask(RCRecordRequest<NodeIDType> rcRecReq) {
		/* This assert follows from the fact that the return value handled
		 * can be true for a reconfiguration intent packet exactly once.
		 */
		assert(!this.isTaskRunning(this.getTaskKey(WaitAckStopEpoch.class, rcRecReq)));
		log.log(Level.INFO,
				MyLogger.FORMAT[4],
				new Object[] { this, " spawning WaitAckStopEpoch for ",
						rcRecReq.getServiceName(), ":",
						rcRecReq.getEpochNumber() - 1 });
		this.protocolExecutor.spawn(new WaitAckStopEpoch<NodeIDType>(
				rcRecReq.startEpoch, this.DB));

	}
	private void spawnSecondaryReconfiguratorTask(RCRecordRequest<NodeIDType> rcRecReq) {
		/* This assert follows from the fact that the return value handled
		 * can be true for a reconfiguration intent packet exactly once.
		 */
		assert (!this.isTaskRunning(this.getTaskKey(
				WaitPrimaryExecution.class, rcRecReq)));

		log.log(Level.INFO,
				MyLogger.FORMAT[3],
				new Object[] { this,
						" spawning WaitPrimaryExecution for ",
						rcRecReq.getServiceName(),
						rcRecReq.getEpochNumber() - 1 });
		this.protocolExecutor
				.schedule(new WaitPrimaryExecution<NodeIDType>(
						getMyID(), rcRecReq.startEpoch, this.DB,
						this.consistentNodeConfig
								.getReplicatedReconfigurators(rcRecReq
										.getServiceName())));
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
	 */
	private Set<NodeIDType> shouldReconfigure(String name) {
		// return null if no current actives
		Set<NodeIDType> oldActives = this.DB.getActiveReplicas(name);
		if (oldActives == null || oldActives.isEmpty()) return null;
		// get new IP addresses (via consistent hashing if no oldActives
		ArrayList<InetAddress> newActives = this.demandProfiler
				.testAndSetReconfigured(name,
						this.consistentNodeConfig.getNodeIPs(oldActives));
		assert (newActives != null);
		// get new actives based on new IP addresses
		return (newActives.equals(oldActives)) ? null
				: (this.consistentNodeConfig.getIPToNodeIDs(newActives,
						oldActives));
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

	/* Stow away to disk if the size of the memory map becomes large.
	 * We will refresh in the updateDemandProfile method if needed.
	 */
	private void trimAggregateDemandProfile() {
		Set<AbstractDemandProfile> profiles = this.demandProfiler.trim();
		for(AbstractDemandProfile profile : profiles) {
			// initiator and epoch are irrelevant in this report
			DemandReport<NodeIDType> report = new DemandReport<NodeIDType>(
					this.getMyID(), profile.getName(), 0, profile);
			// will update stats in DB
			this.DB.handleRequest(report);
		}
	}

	// coordinate reconfiguration intent
	private void initiateReconfiguration(String name, Set<NodeIDType> newActives, 
                InetSocketAddress sender, String initialState) {
		if(newActives==null) return;
		// request to persistently log the intent to reconfigure
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
				this.getMyID(), formStartEpoch(name, newActives, sender, initialState),
				RequestTypes.REGISTER_RECONFIURATION_INTENT);
		// coordinate intent with replicas
		if(this.isReadyForReconfiguration(rcRecReq))
			this.DB.handleIncoming(rcRecReq); 

	}
	/* We check for ongoing reconfigurations to avoid multiple paxos coordinations
	 * by different nodes each trying to initiate a reconfiguration. Although only
	 * one will succeed at the end, it is still useful to limit needless paxos
	 * coordinated requests. Nevertheless, one problem with the check in this
	 * method is that multiple nodes can still try to initiate a reconfiguration
	 * as it only checks based on the DB state. Ideally, some randomization should
	 * make the likelihood of redundant concurrent reconfigurations low.
	 */
	private boolean isReadyForReconfiguration(BasicReconfigurationPacket<NodeIDType> rcPacket) {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(rcPacket.getServiceName());
		return record==null || record.getState().equals(RCStates.READY);
	}
	private void initiateReconfiguration(String name, Set<NodeIDType> newActives) {
		this.initiateReconfiguration(name, newActives, null, null);
	}
	private NodeIDType getMyID() {
		return this.messenger.getMyID();
	}

	private Stringifiable<NodeIDType> getUnstringer() {
		return this.consistentNodeConfig;
	}

	private StartEpoch<NodeIDType> formStartEpoch(String name,
			Set<NodeIDType> newActives, InetSocketAddress sender, String initialState) {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(name);
		StartEpoch<NodeIDType> startEpoch = (record != null) ? new StartEpoch<NodeIDType>(getMyID(), name,
				record.getEpoch() + 1, newActives, record.getActiveReplicas(
						record.getName(), record.getEpoch()), sender, initialState)
				: new StartEpoch<NodeIDType>(getMyID(), name, 0, newActives,
						null, sender, initialState);	
		return startEpoch;
	}
	private String getTaskKey(Class<?> C, BasicReconfigurationPacket<?> rcPacket) {
		return C.getSimpleName() + this.getMyID() + ":" + rcPacket.getServiceName() + ":" + rcPacket.getEpochNumber();
	}
	private String getTaskKeyPrev(Class<?> C, BasicReconfigurationPacket<?> rcPacket) {
		return C.getSimpleName() + this.getMyID() + ":" + rcPacket.getServiceName() + ":" + (rcPacket.getEpochNumber()-1);
	}
	/* FIXME: We need to handle the case of epoch jumps better. A task may
	 * not get garbage collected below if the task doesn't naturally 
	 * terminate and the replica jumps more than one epoch forward. This
	 * would be rare but can happen. In these cases, the task will still
	 * eventually terminate but will take longer at least until the 
	 * restart period. So a large flurry of reconfigurations coupled
	 * with replica failures could end up using a large amount of 
	 * memory for pending (but unnecessary) tasks.
	 */
	private void garbageCollectPendingTasks(RCRecordRequest<NodeIDType> rcRecReq) {
		// remove secondary task, primary will take care of itself
		this.protocolExecutor.remove(getTaskKey(WaitPrimaryExecution.class, rcRecReq));
		// but do remove tasks from previous epoch if any just in case
		this.protocolExecutor.remove(this.getTaskKeyPrev(WaitAckStopEpoch.class, rcRecReq));
		this.protocolExecutor.remove(this.getTaskKeyPrev(WaitAckStartEpoch.class, rcRecReq));
		this.protocolExecutor.remove(this.getTaskKeyPrev(WaitAckDropEpoch.class, rcRecReq));
		this.protocolExecutor.remove(getTaskKeyPrev(WaitPrimaryExecution.class, rcRecReq));
	}

	private void finishPendingReconfigurations() {
		String[] pending = this.DB.getPendingReconfigurations();
		for(String name : pending) {
			ReconfigurationRecord<NodeIDType> record = this.DB.getReconfigurationRecord(name);
			RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
					this.getMyID(), this.formStartEpoch(name,
							record.getNewActives(), null, null),
					RCRecordRequest.RequestTypes.REGISTER_RECONFIURATION_INTENT);
			/* We spawn primary even though that may be unnecessary because we
			 * don't know if or when any other reconfigurator might finish this
			 * pending reconfiguration. Having multiple reconfigurators push
			 * a reconfiguration is okay as stop, start, and drop are all
			 * idempotent operations.
			 */
			this.spawnPrimaryReconfiguratorTask(rcRecReq);
		}
	}

	private void sendDeleteConfirmationToClient(RCRecordRequest<NodeIDType> rcRecReq) {
		try {
			this.messenger.sendToAddress(
					rcRecReq.startEpoch.creator,
					new DeleteServiceName(rcRecReq.startEpoch.creator, rcRecReq
							.getServiceName(), rcRecReq.getEpochNumber() - 1)
							.toJSONObject());
		} catch (IOException | JSONException e) {
			log.severe(this + " received " + e.getClass().getSimpleName() + e.getMessage());
			e.printStackTrace();
		}
	}
	
	/*************** Reconfigurator reconfiguration related methods ***************/

}
