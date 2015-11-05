/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.AddressMessenger;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOInstrumenter;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorCallback;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigureRCNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest.RequestTypes;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.ReconfiguratorProtocolTask;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.WaitAckDropEpoch;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.WaitAckStartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.WaitAckStopEpoch;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.CommitWorker;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.WaitPrimaryExecution;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.AggregateDemandProfiler;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationPacketDemultiplexer;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            This class is the main Reconfigurator module. It issues
 *            reconfiguration commands to ActiveReplicas and also responds to
 *            client requests to create or delete names or request the list of
 *            active replicas for a name.
 * 
 *            It relies on the following helper protocol tasks:
 *            {@code WaitAckStopEpoch,
 *            WaitAckStartEpoch, WaitAckDropEpoch, WaitCoordinatedCommit, 
 *            WaitPrimaryExecution}. The last one is to enable exactly one
 *            primary Reconfigurator in the common case to conduct
 *            reconfigurations but ensure that others safely complete the
 *            reconfiguration if the primary fails to do so.
 *            WaitCoordinatedCommit is a worker that is needed to ensure that a
 *            paxos-coordinated request eventually gets committed; we need this
 *            property to ensure that a reconfiguration operation terminates,
 *            but paxos itself provides us no such liveness guarantee.
 * 
 *            This class now supports add/remove operations for the set of
 *            Reconfigurator nodes. This is somewhat tricky, but works
 *            correctly. A detailed, formal description of the protocol is TBD.
 *            The documentation further below in this class explains the main
 *            ideas.
 * 
 */
public class Reconfigurator<NodeIDType> implements
		PacketDemultiplexer<JSONObject>,
		ReconfiguratorCallback {

	private final SSLMessenger<NodeIDType, JSONObject> messenger;
	private final ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String> protocolExecutor;
	protected final ReconfiguratorProtocolTask<NodeIDType> protocolTask;
	private final RepliconfigurableReconfiguratorDB<NodeIDType> DB;
	private final ConsistentReconfigurableNodeConfig<NodeIDType> consistentNodeConfig;
	private final AggregateDemandProfiler demandProfiler = new AggregateDemandProfiler();
	private final CommitWorker<NodeIDType> commitWorker;
	private boolean recovering = true;

	private static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	/**
	 * @return Logger used by all of the reconfiguration package.
	 */
	public static final Logger getLogger() {
		return log;
	}

	/*
	 * Any id-based communication requires NodeConfig and Messenger. In general,
	 * the former may be a subset of the NodeConfig used by the latter, so they
	 * are separate arguments.
	 */
	protected Reconfigurator(ReconfigurableNodeConfig<NodeIDType> nc,
			SSLMessenger<NodeIDType, JSONObject> m,
			boolean startCleanSlate) {
		this.messenger = m;
		this.consistentNodeConfig = new ConsistentReconfigurableNodeConfig<NodeIDType>(
				nc);
		this.DB = new RepliconfigurableReconfiguratorDB<NodeIDType>(
				new SQLReconfiguratorDB<NodeIDType>(this.messenger.getMyID(),
						this.consistentNodeConfig), getMyID(),
				this.consistentNodeConfig, this.messenger, startCleanSlate);
		// recovery complete at this point
		this.DB.setCallback(this); // no callbacks will happen during recovery

		// protocol executor not needed until recovery complete
		this.protocolExecutor = new ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String>(
				messenger);
		this.protocolTask = new ReconfiguratorProtocolTask<NodeIDType>(
				getMyID(), this);
		// non default types will be registered by spawned tasks
		this.protocolExecutor.register(this.protocolTask.getDefaultTypes(),
				this.protocolTask);
		this.commitWorker = new CommitWorker<NodeIDType>(this.DB);
		this.initFinishPendingReconfigurations();
		this.messenger.setClientMessenger(this.initClientMessenger());
		assert (this.getClientMessenger() != null || this
				.clientFacingPortIsMyPort());
		
		// if here, recovery must be complete
		this.DB.setRecovering(this.recovering = false); 
		log.log(Level.FINE,
				"{0} finished recovery with NodeConfig = {1}",
				new Object[] { this,
						this.consistentNodeConfig.getReconfigurators() });
	}

	/**
	 * This treats the reconfigurator itself as an "active replica" in order to
	 * be able to reconfigure reconfigurator groups.
	 */
	protected ActiveReplica<NodeIDType> getReconfigurableReconfiguratorAsActiveReplica() {
		return new ActiveReplica<NodeIDType>(this.DB,
				this.consistentNodeConfig.getUnderlyingNodeConfig(),
				this.messenger);
	}

	@Override
	public boolean handleMessage(JSONObject jsonObject) {
		try {
			ReconfigurationPacket.PacketType rcType = ReconfigurationPacket
					.getReconfigurationPacketType(jsonObject);
			log.log(Level.FINE, "{0} received {1} {2} {3}", new Object[] {
					this, rcType, jsonObject });
			/*
			 * This assertion is true only if TLS with mutual authentication is
			 * enabled. If so, only authentic servers will be able to send
			 * messages to a reconfigurator and they will never send any message
			 * other than the subset of ReconfigurationPacket types meant to be
			 * processed by reconfigurators.
			 */
			assert (rcType != null || ReconfigurationConfig.isTLSEnabled());

			// try handling as reconfiguration packet through protocol task
			@SuppressWarnings("unchecked")
			// checked by assert above
			BasicReconfigurationPacket<NodeIDType> rcPacket = (BasicReconfigurationPacket<NodeIDType>) ReconfigurationPacket
					.getReconfigurationPacket(jsonObject, this.getUnstringer());
			// all packets are handled through executor, nice and simple
			if (!this.protocolExecutor.handleEvent(rcPacket))
				// do nothing
				log.log(Level.FINE, MyLogger.FORMAT[2], new Object[] { this,
						"unable to handle packet", jsonObject });
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return false; // neither reconfiguration packet nor app request
	}

	/**
	 * @return Packet types handled by Reconfigurator. Refer
	 *         {@link ReconfigurationPacket}.
	 */
	public Set<ReconfigurationPacket.PacketType> getPacketTypes() {
		return this.protocolTask.getEventTypes();
	}

	public String toString() {
		return "RC" + getMyID();
	}

	/**
	 * Close gracefully.
	 */
	public void close() {
		this.protocolExecutor.stop();
		this.messenger.stop();
		this.DB.close();
	}

	/*
	 * *********** Start of protocol task handler methods *****************
	 */
	/**
	 * Incorporates demand reports (possibly but not necessarily with replica
	 * coordination), checks for reconfiguration triggers, and initiates
	 * reconfiguration if needed.
	 * 
	 * @param report
	 * @param ptasks
	 * @return MessagingTask, typically null. No protocol tasks spawned.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleDemandReport(
			DemandReport<NodeIDType> report,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(Level.FINEST, "{0} received {1} {2}", new Object[] { this,
				report.getType(), report });
		if (report.needsCoordination())
			this.DB.handleIncoming(report); // coordinated
		else
			this.updateDemandProfile(report); // no coordination
		ReconfigurationRecord<NodeIDType> record = this.DB.getReconfigurationRecord(report.getServiceName());
		if (record != null)
			// coordinate and commit reconfiguration intent
			this.initiateReconfiguration(report.getServiceName(), record,
					shouldReconfigure(report.getServiceName()), null, null,
					null, null, null, null); // coordinated
		trimAggregateDemandProfile();
		return null; // never any messaging or ptasks
	}

	private boolean isLegitimateCreateRequest(CreateServiceName create) {
		if (!create.isBatched())
			return true;
		return this.consistentNodeConfig.checkSameGroup(create.nameStates
				.keySet());
	}

	/**
	 * Create service name is a special case of reconfiguration where the
	 * previous group is non-existent.
	 * 
	 * @param create
	 * @param ptasks
	 * @return Messaging task, typically null. No protocol tasks spawned.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleCreateServiceName(
			CreateServiceName create,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(Level.FINE, "{0} received {1} from {2}", new Object[] { this,
				create.getSummary(), create.getCreator() });

		// quick reject for bad batched create
		if (!this.isLegitimateCreateRequest(create))
			this.sendClientReconfigurationPacket(create
					.setFailed()
					.setResponseMessage(
							"The names in this create request do not all map to the same reconfigurator group"));

		if (this.processRedirection(create))
			return null;
		// else I am responsible for handling this (possibly forwarded) request

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
		 */
		log.log(Level.FINE, "{0} processing {1} from creator {2} {3}",
				new Object[] {
						this,
						create.getSummary(),
						create.getCreator(),
						create.getForwader() != null ? " forwarded by "
								+ create.getForwader() : "" });

		ReconfigurationRecord<NodeIDType> record = null;
		/*
		 * Check if record doesn't already exist. This check is meaningful only
		 * for unbatched create requests. For batched creates, we optimistically
		 * assume that none of the batched names already exist and let the
		 * create fail later if that is not the case.
		 */
		if ((record = this.DB.getReconfigurationRecord(create.getServiceName())) == null)
			this.initiateReconfiguration(create.getServiceName(), record,
					this.consistentNodeConfig.getReplicatedActives(create
							.getServiceName()), create.getCreator(), create
							.getMyReceiver(), create.getForwader(), create
							.getInitialState(), create.getNameStates(), null);

		// record already exists, so return error message
		else
			this.sendClientReconfigurationPacket(create
					.setFailed()
					.setResponseMessage(
							"Can not (re-)create an already "
									+ (record.isDeletePending() ? "deleted name until "
											+ ReconfigurationConfig
													.getMaxFinalStateAge()
											/ 1000
											+ " seconds have elapsed after the most recent deletion."
											: "existing name.")));
		return null;
	}

	private static final boolean TWO_PAXOS_RC = Config.getGlobalBoolean(ReconfigurationConfig.RC.TWO_PAXOS_RC);
	/**
	 * Simply hand over DB request to DB. The only type of RC record that can
	 * come here is one announcing reconfiguration completion. Reconfiguration
	 * initiation messages are derived locally and coordinated through paxos,
	 * not received from outside.
	 * 
	 * @param rcRecReq
	 * @param ptasks
	 * @return Messaging task, typically null. We may spawn a protocol task but
	 *         don't return it in {@code ptasks[0]} because we want to spawn it
	 *         only if not already running. The spwanIfNotRunning check needs to
	 *         be atomic in {@link ProtocolExecutor} but we currently have no
	 *         meachanism to tell it to spawn {@code ptasks[0]} only if not
	 *         already running. FIXME: This issue is straightforward to fix by
	 *         supporting a spawnIfNotRunning flag in protocol task.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleRCRecordRequest(
			RCRecordRequest<NodeIDType> rcRecReq,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(Level.FINE, "{0} receievd {1}",
				new Object[] { this, rcRecReq.getSummary() });

		GenericMessagingTask<NodeIDType, ?> mtask = null;
		// for NC changes, prev drop complete signifies everyone's on board
		if (rcRecReq.isReconfigurationPrevDropComplete()
				&& rcRecReq.getServiceName().equals(
						AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
								.toString()))
			this.sendRCReconfigurationConfirmationToInitiator(rcRecReq);
		// single paxos reconfiguration allowed only for non-RC-group names
		else if (!TWO_PAXOS_RC
				&& !this.DB.isRCGroupName(rcRecReq.getServiceName())
				&& !rcRecReq.isNodeConfigChange()) {
			if (rcRecReq.isReconfigurationComplete()
					|| rcRecReq.isReconfigurationPrevDropComplete()) {
				if (rcRecReq.getInitiator().equals(getMyID()))
					mtask = new GenericMessagingTask<NodeIDType, RCRecordRequest<NodeIDType>>(
							getOthers(this.consistentNodeConfig.getReplicatedReconfigurators(rcRecReq
									.getServiceName())), rcRecReq);
				// no coordination
				boolean handled = this.DB.execute(rcRecReq);
				if(handled) this.garbageCollectPendingTasks(rcRecReq);
			}
		} else
			// commit until committed by default
			this.repeatUntilObviated(rcRecReq);

		if (rcRecReq.isReconfigurationMerge())
			// stop mergee task obviated when reconfiguration merge proposed
			this.protocolExecutor.remove(getTaskKey(WaitAckStopEpoch.class,
					getMyID().toString(),
					rcRecReq.startEpoch.getPrevGroupName(),
					rcRecReq.startEpoch.getPrevEpochNumber()));

		return mtask!=null ? mtask.toArray() : null;
	}
	
	Object[] getOthers(Set<NodeIDType> nodes) {
		Set<NodeIDType> others = new HashSet<NodeIDType>();
		for(NodeIDType node : nodes)
			if(!node.equals(getMyID())) others.add(node);
		return others.toArray();
	}

	/**
	 * We need to ensure that both the stop/drop at actives happens atomically
	 * with the removal of the record at reconfigurators. To accomplish this, we
	 * first mark the record as stopped at reconfigurators, then wait for the
	 * stop/drop tasks to finish, and finally coordinate the completion
	 * notification so that reconfigurators can completely remove the record
	 * from their DB.
	 * 
	 * @param delete
	 * @param ptasks
	 * @return Messaging task, typically null. No protocol tasks spawned.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleDeleteServiceName(
			DeleteServiceName delete,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(Level.FINE, "{0} received {1} from creator {2}", new Object[] {
				this, delete.getSummary(), delete.getCreator() });

		if (this.processRedirection(delete))
			return null;
		log.log(Level.FINE,
				"{0} processing delete request {1} from creator {2} {3}",
				new Object[] {
						this,
						delete.getSummary(),
						delete.getCreator(),
						delete.getForwader() != null ? " forwarded by "
								+ delete.getForwader() : "" });
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(delete.getServiceName());
		RCRecordRequest<NodeIDType> rcRecReq = null;
		// coordinate delete intent, response will be sent in callback
		if (record != null
				&& this.isReadyForReconfiguration(
						rcRecReq = new RCRecordRequest<NodeIDType>(this
								.getMyID(), this.formStartEpoch(
								delete.getServiceName(), record, null,
								delete.getCreator(), delete.getMyReceiver(),
								delete.getForwader(), null, null, null),
								RequestTypes.RECONFIGURATION_INTENT), record)) {
			this.DB.handleIncoming(rcRecReq);
			return null;
		}
		// WAIT_DELETE state also means success
		else if (this.isWaitingDelete(delete)) {
			//this.sendDeleteConfirmationToClient(rcRecReq);
			this.sendClientReconfigurationPacket(delete
					.setResponseMessage(delete.getServiceName()
							+ " already pending deletion"));
			return null;
		}
		// else failure
		this.sendClientReconfigurationPacket(delete
				.setFailed()
				.setResponseMessage(
						delete.getServiceName()
								+ (record != null ? " is being reconfigured and can not be deleted just yet."
										: " does not exist")));
		log.log(Level.FINE,
				"{0} discarded {1} because RC record is not reconfiguration ready.",
				new Object[] { this, delete.getSummary() });
		return null;
	}

	private boolean isWaitingDelete(DeleteServiceName delete) {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(delete.getServiceName());
		return record != null && record.getState().equals(RCStates.WAIT_DELETE);
	}

	/**
	 * This method simply looks up and returns the current set of active
	 * replicas. Maintaining this state consistently is the primary and only
	 * existential purpose of reconfigurators.
	 * 
	 * @param request
	 * @param ptasks
	 * @return Messaging task returning the set of active replicas to the
	 *         requestor. No protocol tasks spawned.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleRequestActiveReplicas(
			RequestActiveReplicas request,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {

		if (this.processRedirection(request))
			return null;

		NIOInstrumenter.addExcludePort(request.getCreator().getPort());

		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(request.getServiceName());
		if (record == null || record.getActiveReplicas() == null
				|| record.isDeletePending()) {
			log.log(Level.FINE,
					"{0} returning null active replicas for name {1}; record = {2}",
					new Object[] { this, request.getServiceName(), record });
			// I am responsible but can't find actives for the name
			String responseMessage = "No state found for name "
					+ request.getServiceName();
			request.setResponseMessage(responseMessage
					+ " probably because the name has not yet been created or is pending deletion");
			this.sendClientReconfigurationPacket(request.setFailed()
					.makeResponse());
			return null;
		}

		// else
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
		for (NodeIDType node : record.getActiveReplicas())
			activeIPs.add(this.consistentNodeConfig.getNodeSocketAddress(node));
		// to support different client facing ports
		request.setActives(modifyPortsForSSL(activeIPs));
		this.sendClientReconfigurationPacket(request.makeResponse());
		/*
		 * We message using sendActiveReplicasToClient above as opposed to
		 * returning a messaging task below because protocolExecutor's messenger
		 * may not be usable for client facing requests.
		 */
		return null;
	}

	/**
	 * Handles a request to add or delete a reconfigurator from the set of all
	 * reconfigurators in NodeConfig. The reconfiguration record corresponding
	 * to NodeConfig is stored in the RC records table and the
	 * "active replica state" or the NodeConfig info itself is stored in a
	 * separate NodeConfig table in the DB.
	 * 
	 * @param changeRC
	 * @param ptasks
	 * @return Messaging task typically null. No protocol tasks spawned.
	 */
	public GenericMessagingTask<?, ?>[] handleReconfigureRCNodeConfig(
			ReconfigureRCNodeConfig<NodeIDType> changeRC,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		assert (changeRC.getServiceName()
				.equals(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString()));
		log.log(Level.INFO,
				"{0} received node config change request {1} from initiator {2}",
				new Object[] { this, changeRC.getSummary(),
						changeRC.getIssuer() });
		if (!this.isPermitted(changeRC)) {
			String errorMessage = " Impermissible node config change request";
			log.severe(this + errorMessage + ": " + changeRC);
			// this.sendRCReconfigurationErrorToInitiator(changeRC).setFailed().setResponseMessage(errorMessage);
			return (new GenericMessagingTask<InetSocketAddress, ReconfigureRCNodeConfig<NodeIDType>>(
					changeRC.getIssuer(), changeRC.setFailed()
							.setResponseMessage(errorMessage))).toArray();
		}
		// check first if NC is ready for reconfiguration
		ReconfigurationRecord<NodeIDType> ncRecord = this.DB
				.getReconfigurationRecord(changeRC.getServiceName());
		if (ncRecord == null)
			return null; // possible if startCleanSlate

		if (!ncRecord.isReady()) {
			String errorMessage = " Trying to conduct concurrent node config changes";
			log.warning(this + errorMessage + ": " + changeRC);
			return (new GenericMessagingTask<InetSocketAddress, ReconfigureRCNodeConfig<NodeIDType>>(
					changeRC.getIssuer(), changeRC.setFailed()
							.setResponseMessage(errorMessage))).toArray();
		}
		// else try to reconfigure even though it may still fail
		Set<NodeIDType> curRCs = ncRecord.getActiveReplicas();
		Set<NodeIDType> newRCs = new HashSet<NodeIDType>(curRCs);
		newRCs.addAll(changeRC.getAddedRCNodeIDs());
		newRCs.removeAll(changeRC.getDeletedRCNodeIDs());
		// will use the nodeConfig before the change below.
		if (changeRC.newlyAddedNodes != null || changeRC.deletedNodes != null)
			this.initiateReconfiguration(
					AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString(),
					ncRecord,
					newRCs, // this.consistentNodeConfig.getNodeSocketAddress
					(changeRC.getIssuer()), null, null, null, null,
					changeRC.newlyAddedNodes);
		return null;
	}
	
	/**
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
	public void executed(Request request, boolean handled) {
		if (this.isRecovering())
			return; // no messaging during recovery
		BasicReconfigurationPacket<?> rcPacket = null;
		try {
			rcPacket = ReconfigurationPacket.getReconfigurationPacket(request,
					getUnstringer());
		} catch (JSONException e) {
			if (!request.toString().equals(Request.NO_OP))
				e.printStackTrace();
		}
		if (rcPacket == null
				|| !rcPacket.getType().equals(
						ReconfigurationPacket.PacketType.RC_RECORD_REQUEST))
			return;
		@SuppressWarnings("unchecked")
		// checked right above
		RCRecordRequest<NodeIDType> rcRecReq = (RCRecordRequest<NodeIDType>) rcPacket;

		if (this.isCommitWorkerCoordinated(rcRecReq))
			this.commitWorker.executedCallback(rcRecReq, handled);

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

		} else if (handled
				&& (rcRecReq.isReconfigurationComplete() || rcRecReq
						.isDeleteIntentOrPrevDropComplete())) {
			// send delete confirmation to deleting client
			if (rcRecReq.isDeleteIntent()
					&& rcRecReq.startEpoch.isDeleteRequest())
				sendDeleteConfirmationToClient(rcRecReq);
			// send response back to creating client
			else if (rcRecReq.isReconfigurationComplete()
					&& rcRecReq.startEpoch.isCreateRequest())
				this.sendCreateConfirmationToClient(rcRecReq);
			// send response back to RCReconfigure initiator
			else if (rcRecReq.isReconfigurationComplete()
					&& rcRecReq.isNodeConfigChange()) 
				// checkpoint and garbage collect
				this.postCompleteNodeConfigChange(rcRecReq);

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
		} else if (handled && rcRecReq.isNodeConfigChange()) {
			if (rcRecReq.isReconfigurationIntent()) {
				ncAssert(rcRecReq, handled);
				// initiate and complete reconfiguring RC groups here
				executeNodeConfigChange(rcRecReq);
			}
		} else if (rcRecReq.isReconfigurationMerge()) {
			if (!handled)
				/*
				 * Merge was unsuccessful probably because the node that
				 * responded with the checkpoint handle did not deliver on the
				 * actual checkpoint, so we need to start from WaitAckStopEpoch
				 * all over again. Note that it is inconvenient to do something
				 * similar to WaitEpochFinalState and merge upon successfully
				 * getting the state because the merge needs a coordinated
				 * commit task that is asynchronous. The only way to know if the
				 * merge succeeded or failed is in this Reconfigurator
				 * executed() callback function but WaitEpochFinalState by
				 * design belongs to ActiveReplica.
				 */
				this.protocolExecutor
						.spawnIfNotRunning(new WaitAckStopEpoch<NodeIDType>(
								rcRecReq.startEpoch, this.DB));

			else if (handled && rcRecReq.getInitiator().equals(getMyID()))
				/*
				 * We shoudln't explicitly drop the mergee's final epoch state
				 * as other nodes may not have completed the merge and the node
				 * that first supplied the final checkpoint handle may have
				 * crashed. If so, we need to resume WaitAckStopEpoch and for it
				 * to succeed, we need the final checkpoints to not be dropped.
				 * The mergee's final state can be left around hanging and will
				 * eventually become unusable after MAX_FINAL_STATE_AGE. The
				 * actual checkpoints via the file system will be deleted by the
				 * garbage collector eventually, but the final checkpoint
				 * handles in the DB will remain forever or at least until a
				 * node with this name is re-added to the system.
				 */
				;

		}
	}
	
	private boolean isCommitWorkerCoordinated(
			RCRecordRequest<NodeIDType> rcRecReq) {
		return (TWO_PAXOS_RC && (rcRecReq.isReconfigurationComplete() || rcRecReq
				.isReconfigurationPrevDropComplete()))
				|| rcRecReq.isReconfigurationMerge();
	}
	
	private void ncAssert(RCRecordRequest<NodeIDType> rcRecReq, boolean handled) {
		ReconfigurationRecord<NodeIDType> ncRecord = this.DB
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString());
		assert (!ncRecord.getActiveReplicas().equals(ncRecord.getNewActives())) : this
				+ " : handled="
				+ handled
				+ "; "
				+ ncRecord
				+ "\n  upon \n"
				+ rcRecReq;
	}

	@Override
	public void preExecuted(Request request) {
		if (this.isRecovering())
			return;
		// checked right above
		RCRecordRequest<NodeIDType> rcRecReq = this
				.requestToRCRecordRequest(request);

		// this method is currently used for NC record completions
		if (rcRecReq == null || !this.DB.isNCRecord(rcRecReq.getServiceName())
				|| !rcRecReq.isReconfigurationComplete())
			return;

		/*
		 * Only newly added nodes need to do this as they received a
		 * reconfiguration complete out of the blue and may not even know the
		 * socket addresses of other newly added nodes.
		 */
		if (rcRecReq.startEpoch.getNewlyAddedNodes().contains(this.getMyID())) 
			this.executeNodeConfigChange(rcRecReq);
	}

	/****************************** End of protocol task handler methods *********************/

	/*********************** Private methods below **************************/

	@SuppressWarnings("unchecked")
	private RCRecordRequest<NodeIDType> requestToRCRecordRequest(
			Request request) {
		if (request instanceof RCRecordRequest<?>)
			return (RCRecordRequest<NodeIDType>) request;
		BasicReconfigurationPacket<?> rcPacket = null;
		try {
			rcPacket = ReconfigurationPacket.getReconfigurationPacket(request,
					getUnstringer());
		} catch (JSONException e) {
			if (!request.toString().equals(Request.NO_OP))
				e.printStackTrace();
		}
		if (rcPacket == null
				|| !rcPacket.getType().equals(
						ReconfigurationPacket.PacketType.RC_RECORD_REQUEST))
			return null;
		// checked right above
		RCRecordRequest<NodeIDType> rcRecReq = (RCRecordRequest<NodeIDType>) rcPacket;
		return rcRecReq;
	}

	private void spawnPrimaryReconfiguratorTask(
			RCRecordRequest<NodeIDType> rcRecReq) {
		/*
		 * This assert follows from the fact that the return value handled can
		 * be true for a reconfiguration intent packet exactly once.
		 */
                //MOB-504: Fix 2: This is more of a hack for now. 
//                if(this.isTaskRunning(this.getTaskKey(WaitAckStopEpoch.class,
//				rcRecReq)) && !rcRecReq.isSplitIntent()) return;
                
		assert (!this.isTaskRunning(this.getTaskKey(WaitAckStopEpoch.class,
				rcRecReq)));
                
		log.log(Level.FINE,
				MyLogger.FORMAT[8],
				new Object[] { this, "spawning WaitAckStopEpoch for",
						rcRecReq.startEpoch.getPrevGroupName(), ":",
						rcRecReq.getEpochNumber() - 1, "for starting",
						rcRecReq.getServiceName(), ":",
						rcRecReq.getEpochNumber() });
		// the main stop/start/drop sequence begins here
		if (!rcRecReq.isSplitIntent())
			this.protocolExecutor
					.spawnIfNotRunning(new WaitAckStopEpoch<NodeIDType>(
							rcRecReq.startEpoch, this.DB));
		else
			// split reconfigurations should skip the stop phase
			this.protocolExecutor
					.spawnIfNotRunning(new WaitAckStartEpoch<NodeIDType>(
							rcRecReq.startEpoch, this.DB));

	}

	private void spawnSecondaryReconfiguratorTask(
			RCRecordRequest<NodeIDType> rcRecReq) {
		/*
		 * This assert follows from the fact that the return value handled can
		 * be true for a reconfiguration intent packet exactly once.
		 */
          if (this.isTaskRunning(this.getTaskKey(WaitPrimaryExecution.class,
				rcRecReq))) {
            log.log(Level.INFO, MyLogger.FORMAT[3], 
                    new Object[] { this, 
              " TASK IS ALREADY RUNNING: ", rcRecReq.getSummary()});
          }
                // disable
		assert (!this.isTaskRunning(this.getTaskKey(WaitPrimaryExecution.class,
				rcRecReq)));
                // 

		log.log(Level.FINE, MyLogger.FORMAT[3],
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

	private ReconfigurationPacket.PacketType[] clientRequestTypes = {
			ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME,
			ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME,
			ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS,
			// this packet is in clientRequestTypes only for ease of testing
			ReconfigurationPacket.PacketType.RECONFIGURE_RC_NODE_CONFIG };

	// put anything needing periodic instrumentation here
	private void instrument(Level level) {
		log.log(level,
				"{0} activeThreadCount = {1}; taskCount = {2}; completedTaskCount = {3}",
				new Object[] { this, this.protocolExecutor.getActiveCount(),
						this.protocolExecutor.getTaskCount(),
						this.protocolExecutor.getCompletedTaskCount() });
	}

	class Instrumenter implements Runnable {
		public void run() {
			instrument(Level.FINE);
		}
	}

	private AddressMessenger<JSONObject> getClientMessenger() {
		return this.messenger.getClientMessenger();
	}

	private boolean processRedirection(
			ClientReconfigurationPacket clientRCPacket) {
		/*
		 * Received response from responsible reconfigurator to which I
		 * previously forwarded this client request. Need to check whether I
		 * received a redirected response before checking whether I am
		 * responsible, otherwise there will be an infinite forwarding loop.
		 */
		if (clientRCPacket.isRedirectedResponse()) {
			log.log(Level.FINE,
					"{0} relaying response for forwarded request {1}",
					new Object[] { this, clientRCPacket.getSummary() });
			// just relay response to the client
			return this.sendClientReconfigurationPacket(clientRCPacket);
		}
		// forward if I am not responsible
		else
			return (this.redirectableRequest(clientRCPacket));
	}

	private static Set<InetSocketAddress> modifyPortsForSSL(
			Set<InetSocketAddress> replicas, boolean actives) {
		if (ReconfigurationConfig.getClientPortOffset() == 0)
			return replicas;
		Set<InetSocketAddress> modified = new HashSet<InetSocketAddress>();
		for (InetSocketAddress sockAddr : replicas)
			modified.add(new InetSocketAddress(sockAddr.getAddress(),
					getClientFacingPort(sockAddr.getPort())));
		return modified;
	}

	private static Set<InetSocketAddress> modifyPortsForSSL(
			Set<InetSocketAddress> replicas) {
		return modifyPortsForSSL(replicas, false);
	}

	/**
	 * Refer {@link ActiveReplica#getClientFacingPort(int)}.
	 * 
	 * @param port
	 * @return The client facing port.
	 */
	public static int getClientFacingPort(int port) {
		return ActiveReplica.getClientFacingPort(port);
	}

	private boolean clientFacingPortIsMyPort() {
		return getClientFacingPort(this.consistentNodeConfig
				.getNodePort(getMyID())) == this.consistentNodeConfig
				.getNodePort(getMyID());
	}

	private boolean redirectableRequest(ClientReconfigurationPacket request) {
		// I am responsible
		if (this.consistentNodeConfig.getReplicatedReconfigurators(
				request.getServiceName()).contains(getMyID()))
			return false;

		// else if forwardable
		if (request.isForwardable())
			// forward to a random responsible reconfigurator
			this.forwardClientReconfigurationPacket(request);
		else
			// error with redirection hints
			this.sendClientReconfigurationPacket(request
					.setFailed()
					.setHashRCs(
							modifyPortsForSSL(this
									.getSocketAddresses(this.consistentNodeConfig
											.getReplicatedReconfigurators(request
													.getServiceName()))))
					.setResponseMessage(
							" <Wrong number! I am not the reconfigurator responsible>"));
		return true;
	}

	private Set<InetSocketAddress> getSocketAddresses(Set<NodeIDType> nodes) {
		Set<InetSocketAddress> sockAddrs = new HashSet<InetSocketAddress>();
		for (NodeIDType node : nodes)
			sockAddrs.add(this.consistentNodeConfig.getNodeSocketAddress(node));
		return sockAddrs;
	}

	/*
	 * We may need to use a separate messenger for end clients if we use two-way
	 * authentication between servers.
	 */
	private AddressMessenger<JSONObject> initClientMessenger() {
		AbstractPacketDemultiplexer<JSONObject> pd = null;
		Messenger<InetSocketAddress, JSONObject> cMsgr = null;
		try {
			int myPort = (this.consistentNodeConfig.getNodePort(getMyID()));
			if (getClientFacingPort(myPort) != myPort) {
				cMsgr = new JSONMessenger<InetSocketAddress>(
						new MessageNIOTransport<InetSocketAddress, JSONObject>(
								this.consistentNodeConfig
										.getBindAddress(getMyID()),
								getClientFacingPort(myPort),
								(pd = new ReconfigurationPacketDemultiplexer()),
								ReconfigurationConfig.getClientSSLMode()));
				pd.register(clientRequestTypes, this);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return cMsgr != null ? cMsgr
				: (AddressMessenger<JSONObject>) this.messenger;
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
		if (newActiveIPs == null)
			return null;
		// get new actives based on new IP addresses
		Set<NodeIDType> newActives = this.consistentNodeConfig
				.getIPToActiveReplicaIDs(newActiveIPs, oldActives);
		return (!newActives.equals(oldActives) || ReconfigurationConfig
				.shouldReconfigureInPlace()) ? newActives : null;
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
			this.DB.execute(report);
		}
	}

	// coordinate reconfiguration intent
	private boolean initiateReconfiguration(String name,
			ReconfigurationRecord<NodeIDType> record,
			Set<NodeIDType> newActives, InetSocketAddress sender,
			InetSocketAddress receiver, InetSocketAddress forwarder,
			String initialState, Map<String, String> nameStates,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes) {
		if (newActives == null)
			return false;
		// request to persistently log the intent to reconfigure
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
				this.getMyID(), formStartEpoch(name, record, newActives,
						sender, receiver, forwarder, initialState, nameStates,
						newlyAddedNodes), RequestTypes.RECONFIGURATION_INTENT);
		// coordinate intent with replicas
		if (this.isReadyForReconfiguration(rcRecReq, record))
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
			BasicReconfigurationPacket<NodeIDType> rcPacket,
			ReconfigurationRecord<NodeIDType> recordServiceName) {
		ReconfigurationRecord<NodeIDType> recordGroupName = this.DB
				.getReconfigurationRecord(this.DB.getRCGroupName(rcPacket
						.getServiceName()));
		/*
		 * We need to check both if the RC group record is ready and the service
		 * name record is either also ready or null (possible during name
		 * creation).
		 */
		boolean ready = recordGroupName != null && recordGroupName.isReady()
				&& (recordServiceName == null || recordServiceName.isReady());
		if (!ready)
			log.log(Level.FINE,
					"{0} not ready to reconfigure {1}; record={2} and rcGroupRecord={3}",
					new Object[] {
							this,
							rcPacket.getServiceName(),
							recordServiceName != null ? recordServiceName
									.getSummary() : "[null]",
							recordGroupName != null ? recordGroupName
									.getSummary() : "[null]" });
		return ready;
	}

	private NodeIDType getMyID() {
		return this.messenger.getMyID();
	}

	private Stringifiable<NodeIDType> getUnstringer() {
		return this.consistentNodeConfig;
	}

	private StartEpoch<NodeIDType> formStartEpoch(String name,
			ReconfigurationRecord<NodeIDType> record,
			Set<NodeIDType> newActives, InetSocketAddress sender,
			InetSocketAddress receiver, InetSocketAddress forwarder,
			String initialState, Map<String, String> nameStates,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes) {
		StartEpoch<NodeIDType> startEpoch = (record != null) ?
		// typical reconfiguration
		new StartEpoch<NodeIDType>(getMyID(), name, record.getEpoch() + 1,
				newActives, record.getActiveReplicas(record.getName(),
						record.getEpoch()), sender, receiver, forwarder,
				initialState, nameStates, newlyAddedNodes)
		// creation reconfiguration
				: new StartEpoch<NodeIDType>(getMyID(), name, 0, newActives,
						null, sender, receiver, forwarder, initialState,
						nameStates, newlyAddedNodes);
		return startEpoch;
	}

	/************ Start of key construction utility methods *************/

	private String getTaskKey(Class<?> C, BasicReconfigurationPacket<?> rcPacket) {
		return getTaskKey(C, rcPacket, getMyID().toString());
	}

	/**
	 * @param C
	 * @param rcPacket
	 * @param myID
	 * @return The task key.
	 */
	public static String getTaskKey(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket, String myID) {
		return getTaskKey(C, myID, rcPacket.getServiceName(),
				rcPacket.getEpochNumber());
	}

	private static String getTaskKey(Class<?> C, String myID, String name,
			int epoch) {
		return C.getSimpleName() + myID + ":" + name + ":" + epoch;
	}

	private String getTaskKeyPrev(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket) {
		return getTaskKeyPrev(C, rcPacket, getMyID().toString());
	}

	private String getTaskKeyPrev(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket, int prev) {
		return getTaskKeyPrev(C, rcPacket, getMyID().toString(), prev);
	}

	protected static String getTaskKeyPrev(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket, String myID) {
		return getTaskKeyPrev(C, rcPacket, myID, 1);
	}

	private static String getTaskKeyPrev(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket, String myID, int prev) {
		return getTaskKey(C, myID, rcPacket.getServiceName(),
				rcPacket.getEpochNumber() - prev);
	}

	/************ End of key construction utility methods *************/

	private void garbageCollectPendingTasks(RCRecordRequest<NodeIDType> rcRecReq) {
		this.garbageCollectStopAndStartTasks(rcRecReq);
		/*
		 * Remove secondary task, primary will take care of itself.
		 * 
		 * Invariant: The secondary task always terminates when a
		 * reconfiguration completes.
		 */
		this.protocolExecutor.remove(getTaskKey(WaitPrimaryExecution.class,
				rcRecReq));

		/*
		 * We don't need to garbage collect the just completed reconfiguration's
		 * WaitAckDropEpoch as it should clean up after itself when if and when
		 * it finishes, but we should garbage collect any WaitAckDropEpoch from
		 * the immediately preceding reconfiguration completion. So we remove
		 * WaitAckDropEpoch[myID]:name:n-2 here, where 'n' is the epoch number
		 * to which we just completed reconfiguring.
		 * 
		 * Invariant: There is at most one WaitAckDropEpoch task running for a
		 * given name at any reconfigurator, the one for the most recently
		 * completed reconfiguration.
		 */
		this.protocolExecutor.remove(getTaskKeyPrev(WaitAckDropEpoch.class,
				rcRecReq, 2));
	}

	// just before coordinating reconfiguration complete/merge
	private void garbageCollectStopAndStartTasks(
			RCRecordRequest<NodeIDType> rcRecReq) {
		// stop task obviated just before reconfiguration complete proposed
		this.protocolExecutor.remove(this.getTaskKeyPrev(
				WaitAckStopEpoch.class, rcRecReq));
		// start task obviated just before reconfiguration complete proposed
		this.protocolExecutor.remove(this.getTaskKey(WaitAckStartEpoch.class,
				rcRecReq));

		// remove previous epoch's start task in case it exists here
		this.protocolExecutor.remove(this.getTaskKeyPrev(
				WaitAckStartEpoch.class, rcRecReq));
	}

	private void initFinishPendingReconfigurations() {
		/*
		 * Invoked just once upon recovery, but we could also invoke this
		 * periodically.
		 */
		this.finishPendingReconfigurations();

		/*
		 * Periodic task to remove old file system based checkpoints after a
		 * safe timeout of MAX_FINAL_STATE_AGE. The choice of the period below
		 * of a tenth of that is somewhat arbitrary.
		 */
		this.protocolExecutor.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				DB.garbageCollectOldFileSystemBasedCheckpoints();
			}
		}, 0, ReconfigurationConfig.getMaxFinalStateAge() / 10,
				TimeUnit.MILLISECONDS);

		/*
		 * Periodic task to finish pending deletions after a safe timeout of
		 * MAX_FINAL_STATE_AGE. The choice of the period below of a tenth of
		 * that is somewhat arbitrary.
		 */
		this.protocolExecutor.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				DB.delayedDeleteComplete();
			}
		}, 0, ReconfigurationConfig.getMaxFinalStateAge() / 10,
				TimeUnit.MILLISECONDS);

		// for instrumentation, unrelated to pending reconfigurations
		this.protocolExecutor.scheduleWithFixedDelay(new Instrumenter(), 0, 60,
				TimeUnit.SECONDS);
	}

	/*
	 * Called initially upon recovery to finish pending reconfigurations.
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
			log.log(Level.FINE,
					"{0} initiating pending reconfiguration for {1}",
					new Object[] { this, name });
			RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
					this.getMyID(), this.formStartEpoch(name, record,
							record.getNewActives(), null, null, null, null, null, null),
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

	private boolean forwardClientReconfigurationPacket(
			ClientReconfigurationPacket request) {
		try {
			Set<NodeIDType> responsibleRCs = this.consistentNodeConfig
					.getReplicatedReconfigurators(request.getServiceName());
			@SuppressWarnings("unchecked")
			NodeIDType randomResponsibleRC = (NodeIDType) (responsibleRCs
					.toArray()[(int) (Math.random() * responsibleRCs.size())]);
			request = request.setForwader(this.consistentNodeConfig
					.getBindSocketAddress(getMyID()));
			log.log(Level.FINE,
					"{0} forwarding client request {1} to reconfigurator {2}",
					new Object[] { this, request.getSummary(),
							randomResponsibleRC });
			this.messenger.sendToAddress(
					this.consistentNodeConfig
							.getNodeSocketAddress(randomResponsibleRC),
					request.setForwader(
							this.consistentNodeConfig
									.getNodeSocketAddress(getMyID()))
							.toJSONObject());
		} catch (IOException | JSONException e) {
			log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
		return true;
	}

	private boolean sendClientReconfigurationPacket(
			ClientReconfigurationPacket response) {
		try {

			InetSocketAddress querier = this.getQuerier(response);
			if (querier.equals(response.getCreator())) {
				// only response can go back to client
				log.log(Level.FINE,
						"{0} sending client response {1}:{2} to client {3}",
						new Object[] { this, response,
								response.getResponseMessage(), querier });
				(this.getMessenger(response.getMyReceiver())).sendToAddress(
						querier, response.toJSONObject());
			} else {
				// may be a request or response
				log.log(Level.FINE,
						"{0} sending client {1} {2} to reconfigurator {3}",
						new Object[] {
								this,
								response.isRequest() ? "request"
										: "request's response",
								response.getSummary(), querier });
				this.messenger.sendToAddress(querier, response.toJSONObject());
			}
		} catch (IOException | JSONException e) {
			log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
		return true;
	}

	/*
	 * There are only two messengers in all, so if it is not my node config
	 * socket address, it must be client messenger.
	 */
	private AddressMessenger<JSONObject> getMessenger(
			InetSocketAddress receiver) {
		if (receiver.equals(this.consistentNodeConfig.getBindSocketAddress(this
				.getMyID()))) {
			log.log(Level.FINE,
					"{0} using messenger for {1}; bindAddress is {2}",
					new Object[] {
							this,
							receiver,
							this.consistentNodeConfig.getBindSocketAddress(this
									.getMyID()) });
			return this.messenger;
		} else {
			log.log(Level.FINE,
					"{0} using clientMessenger for {1}; bindAddress is {2}",
					new Object[] {
							this,
							receiver,
							this.consistentNodeConfig.getBindSocketAddress(this
									.getMyID()) });
			return this.getClientMessenger();
		}
	}

	/*
	 * Confirmation means necessarily a positive response. This method is
	 * invoked from the creation execution callback. If the record already
	 * exists or is in the process of being created, we return an error as
	 * opposed to sending a confirmation via this method.
	 * 
	 * Note: this behavior is different from deletions where we return success
	 * if the record is pending deletion (but do return failure if it has been
	 * completely deleted).
	 */
	private void sendCreateConfirmationToClient(
			RCRecordRequest<NodeIDType> rcRecReq) {
		if (rcRecReq.startEpoch.creator == null
				|| !rcRecReq.getInitiator().equals(getMyID()))
			return;

		DelayProfiler.updateDelay("createServiceName",
				rcRecReq.startEpoch.getInitTime());
		try {
			InetSocketAddress querier = this.getQuerier(rcRecReq);
			CreateServiceName response = (CreateServiceName) (new CreateServiceName(
					rcRecReq.startEpoch.creator, rcRecReq.getServiceName(),
					rcRecReq.getEpochNumber(), null)).setForwader(
					rcRecReq.startEpoch.getForwarder()).makeResponse();
			// need to use different messengers for client and forwarder
			if (querier.equals(rcRecReq.startEpoch.creator)) {
				log.log(Level.FINE,
						"{0} sending creation confirmation {1} to client {2}",
						new Object[] { this, response.getSummary(), querier });
				// this.getClientMessenger()
				(this.getMessenger(rcRecReq.startEpoch.getMyReceiver()))
						.sendToAddress(querier, response.toJSONObject());
			} else {
				log.log(Level.FINE,
						"{0} sending creation confirmation {1} to forwarding reconfigurator {2}",
						new Object[] { this, response.getSummary(), querier });
				this.messenger.sendToAddress(querier, response.toJSONObject());
			}
		} catch (IOException | JSONException e) {
			log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	/*
	 * Confirmation means necessarily a positive response. This method is
	 * invoked either via the delete execution callback or immediately if the
	 * record is already pending deletion. If the record is completely deleted,
	 * we return an error as opposed to sending a confirmation via this method.
	 * 
	 * Note: Returning success for pending deletions is different from the
	 * behavior for creations where we return success only after the record's
	 * creation is complete, i.e., pending creations return a creation error but
	 * pending deletions return a deletion success. This difference is because
	 * once a record is marked as pending deletion (WAIT_DELETE), it is as good
	 * as deleted and is only waiting final garbage collection.
	 */
	private void sendDeleteConfirmationToClient(
			RCRecordRequest<NodeIDType> rcRecReq) {
		if (rcRecReq.startEpoch.creator == null
				|| !rcRecReq.getInitiator().equals(getMyID()))
			return;
		try {
			InetSocketAddress querier = this.getQuerier(rcRecReq);
			// copy forwarder from startEpoch and mark as response
			DeleteServiceName response = (DeleteServiceName) new DeleteServiceName(
					rcRecReq.startEpoch.creator, rcRecReq.getServiceName(),
					rcRecReq.getEpochNumber() - 1).setForwader(
					rcRecReq.startEpoch.getForwarder()).makeResponse();

			if (querier.equals(rcRecReq.startEpoch.creator)) {
				log.log(Level.FINE,
						"{0} sending deletion confirmation {1} to client {2}",
						new Object[] { this, response.getSummary(), querier });
				// this.getClientMessenger()
				(this.getMessenger(rcRecReq.startEpoch.getMyReceiver()))
						.sendToAddress(this.getQuerier(rcRecReq),
								response.toJSONObject());
			} else {
				log.log(Level.FINE,
						"{0} sending deletion confirmation {1} to forwarding reconfigurator {2}",
						new Object[] { this, response.getSummary(), querier });
				this.messenger.sendToAddress(querier, response.toJSONObject());
			}
		} catch (IOException | JSONException e) {
			log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	private InetSocketAddress getQuerier(RCRecordRequest<NodeIDType> rcRecReq) {
		InetSocketAddress forwarder = rcRecReq.startEpoch.getForwarder();
		InetSocketAddress me = this.consistentNodeConfig
				.getBindSocketAddress(getMyID());
		// if there is a forwarder that is not me, relay back
		if (forwarder != null && !forwarder.equals(me))
			return forwarder;
		else
			// return directly to creator
			return rcRecReq.startEpoch.creator;
	}

	private InetSocketAddress getQuerier(ClientReconfigurationPacket response) {
		InetSocketAddress forwarder = response.getForwader();
		InetSocketAddress me = this.consistentNodeConfig
				.getBindSocketAddress(getMyID());
		// if there is a forwarder that is not me, relay back
		if (forwarder != null && !forwarder.equals(me)) {
			return forwarder;
		} else {
			// return directly to creator
			return response.getCreator();
		}
	}

	// true only for easy testing
	private boolean enableRCReconfigurationFromClient = true;

	private void sendRCReconfigurationConfirmationToInitiator(
			RCRecordRequest<NodeIDType> rcRecReq) {
		try {
			// FIXME: can't put self or socket address as initiator here
			ReconfigureRCNodeConfig<NodeIDType> response = new ReconfigureRCNodeConfig<NodeIDType>(
					this.DB.getMyID(), rcRecReq.startEpoch.newlyAddedNodes,
					this.diff(rcRecReq.startEpoch.prevEpochGroup,
							rcRecReq.startEpoch.curEpochGroup));
			log.log(Level.FINE,
					"{0} sending ReconfigureRCNodeConfig confirmation to {1}: {2}",
					new Object[] { this, rcRecReq.startEpoch.creator,
							response.getSummary() });
			// FIXME: use right socket address for self
			(this.consistentNodeConfig.getNodeSocketAddress(getMyID()).equals(
					rcRecReq.startEpoch.getMyReceiver()) ? this.messenger
					: this.getClientMessenger()).sendToAddress(
					rcRecReq.startEpoch.creator, response.toJSONObject());
		} catch (IOException | JSONException e) {
			log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	// FIXME: use or remove
	protected void sendRCReconfigurationErrorToInitiator(
			ReconfigureRCNodeConfig<NodeIDType> changeRCReq) {
		try {
			log.log(Level.FINE, MyLogger.FORMAT[2],
					new Object[] { this,
							"sending ReconfigureRCNodeConfig error to",
							changeRCReq.getIssuer() });
			(enableRCReconfigurationFromClient ? this.getClientMessenger()
					: this.messenger).sendToAddress(changeRCReq.getIssuer(),
					changeRCReq.setFailed().toJSONObject());
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

	// all nodes are primaries for NC change.
	private boolean reconfigureNodeConfigRecord(
			RCRecordRequest<NodeIDType> rcRecReq) {
		if (rcRecReq.getInitiator().equals(getMyID()))
			this.spawnPrimaryReconfiguratorTask(rcRecReq);
		else
			this.spawnSecondaryReconfiguratorTask(rcRecReq);
		return true;
	}

	/*
	 * This method conducts the actual reconfiguration assuming that the
	 * "intent" has already been committed in the NC record. It (1) spawns each
	 * constituent reconfiguration for its new reconfigurator groups and (2)
	 * reconfigures the NC record itself. Spawning each constituent
	 * reconfiguration means executing the corresponding reconfiguration intent,
	 * then spawning WaitAckStop, etc. It is not important to worry about
	 * "completing" the NC change intent under failures as paxos will ensure
	 * safety. We do need a trigger to indicate the completion of all
	 * constituent reconfigurations so that the NC record change can be
	 * considered and marked as complete. For this, upon every NC
	 * reconfiguration complete commit, we could simply check if any of the new
	 * RC groups are still pending and if not, consider the NC change as
	 * incomplete until all constituent RC groups are ready. That is what we do
	 * in AbstractReconfiguratorDB.
	 */
	private boolean executeNodeConfigChange(RCRecordRequest<NodeIDType> rcRecReq) {
		boolean allDone = true;

		// change soft copy of node config
		boolean ncChanged = changeSoftNodeConfig(rcRecReq.startEpoch);
		// change persistent copy of node config
		ncChanged = ncChanged
				&& this.DB.changeDBNodeConfig(rcRecReq.startEpoch
						.getEpochNumber());
		if (!ncChanged)
			throw new RuntimeException("Unable to change node config");
		assert(!rcRecReq.startEpoch.getNewlyAddedNodes().isEmpty() || 
				!diff(rcRecReq.startEpoch.prevEpochGroup,
						rcRecReq.startEpoch.curEpochGroup).isEmpty());
	
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

		this.reconfigureNodeConfigRecord(rcRecReq);

		// finally all done
		return allDone;
	}

	/*
	 * We need to checkpoint the NC record after every NC change. Unlike other
	 * records for RC groups where we can roll forward quickly by simply
	 * applying state changes specified in the logged decisions (instead of
	 * actually re-conducting the corresponding reconfigurations), NC group
	 * changes are more complex and have to be re-conducted at each node
	 * redundantly, however that may not even be possible as deleted nodes or
	 * even existing nodes may no longer have the final state corresponding to
	 * older epochs. Checkpointing after every NC change ensures that, upon
	 * recovery, each node has to try to re-conduct at most only the most recent
	 * NC change.
	 * 
	 * What if this forceCheckpoint operation fails? If the next NC change
	 * successfully completes at this node before the next crash, there is no
	 * problem. Else, upon recovery, this node will try to re-conduct the NC
	 * change corresponding to the failed forceCheckpoint and might be unable to
	 * do so. This is equivalent to this node having missed long past NC
	 * changes. At this point, this node must be deleted and re-added to NC.
	 */
	private void postCompleteNodeConfigChange(
			RCRecordRequest<NodeIDType> rcRecReq) {
		log.log(Level.FINE,
				"{0} completed node config change for epoch {1}; forcing checkpoint..",
				new Object[] { this, rcRecReq.getEpochNumber() });
		this.DB.forceCheckpoint(rcRecReq.getServiceName());

		// stop needless failure monitoring
		for (NodeIDType node : diff(rcRecReq.startEpoch.prevEpochGroup,
				rcRecReq.startEpoch.curEpochGroup))
			this.DB.garbageCollectDeletedNode(node);

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
				log.log(Level.FINE,
						"{0} added new reconfigurator {1}={2} to node config",
						new Object[] {
								this,
								entry.getKey(),
								this.consistentNodeConfig
										.getNodeSocketAddress(entry.getKey()) });
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

	// FIXME: derive and include provably correct constraints here
	private boolean isPermitted(ReconfigureRCNodeConfig<NodeIDType> changeRC) {

		// if node is pending deletion from previous incarnation
		if (changeRC.getAddedRCNodeIDs() != null)
			for (NodeIDType addNode : changeRC.getAddedRCNodeIDs()) {
				ReconfigurationRecord<NodeIDType> rcRecord = this.DB
						.getReconfigurationRecord(this.DB
								.getRCGroupName(addNode));
				{
					if (rcRecord != null && rcRecord.isDeletePending()) {
						changeRC.setResponseMessage("Can not add reconfigurator named "
								+ addNode
								+ " as it is pending deletion from a previous add.");
						return false;
					}
					// check if name conflicts with active replica name
					else if (this.consistentNodeConfig.nodeExists(addNode)) {
						changeRC.setResponseMessage("Can not add reconfigurator named "
								+ addNode
								+ " as another node with the same name already exists.");
						return false;
					}
				}
			}
		// if node is not in the current set of RC nodes
		if (changeRC.deletedNodes != null)
			for (NodeIDType deleteNode : changeRC.deletedNodes) {
				if (!this.consistentNodeConfig.getReconfigurators().contains(
						deleteNode)) {
					changeRC.setResponseMessage("Can not delete reconfigurator "
							+ deleteNode
							+ " as it is not part of the current set of reconfigurators");
					return false;
				}
			}

		int permittedSize = this.consistentNodeConfig
				.getReplicatedReconfigurators("0").size();
		// allow at most one less than the reconfigurator group size
		return changeRC.getDeletedRCNodeIDs().size() > permittedSize ? (changeRC
				.setResponseMessage("Deleting more than " + (permittedSize - 1)
						+ " reconfigurators simultaneously is not permitted") != null)
				: true;
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
		
		assert(!ncRecord.getActiveReplicas().equals(ncRecord.getNewActives())) : ncRecord;

		if (ncRecord == null)
			return false;

		// adjustCurWithNewRCGroups(curRCGroups, newRCGroups, ncRecord);
		String changed = this.changeExistingGroups(curRCGroups, newRCGroups,
				ncRecord, affectedNodes);
		String split = this.splitExistingGroups(curRCGroups, newRCGroups,
				ncRecord);
		String merged = this.mergeExistingGroups(curRCGroups, newRCGroups,
				ncRecord);

		log.log(Level.INFO, "{0} changed/split/merged = \n{1}{2}{3}",
				new Object[] { this, changed, split, merged });
		return !(changed + split + merged).isEmpty();
	}

	private boolean isRecovering() {
		return this.recovering;
	}

	private boolean isPresent(String rcGroupName, Set<NodeIDType> affectedNodes) {
		for (NodeIDType node : affectedNodes) {
			if (this.DB.getRCGroupName(node).equals(rcGroupName))
				return true;
		}
		return false;
	}

	// NC request restarts should be slow and mostly unnecessary
	private static final long NODE_CONFIG_RESTART_PERIOD = 8 * WaitAckStopEpoch.RESTART_PERIOD;

	private void repeatUntilObviated(RCRecordRequest<NodeIDType> rcRecReq) {
		if (this.DB.isNCRecord(rcRecReq.getServiceName()))
			this.commitWorker.enqueueForExecution(rcRecReq,
					NODE_CONFIG_RESTART_PERIOD);
		else {
			this.commitWorker.enqueueForExecution(rcRecReq);
		}
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
		Map<String, Set<String>> mergeLists = this.DB.app.getMergeLists();
		// for each new group, initiate group change if and as needed
		for (String newRCGroup : newRCGroups.keySet()) {
			if (curRCGroups.keySet().contains(newRCGroup)) {
				if (!isPresent(newRCGroup, affectedNodes))
					continue; // don't trivial-reconfigure

				int ncEpoch = ncRecord.getRCEpoch(newRCGroup);
				// change current group
				debug += (this + " changing current group {" + newRCGroup + ":"
						+ (ncEpoch - 1) + "=" + curRCGroups.get(newRCGroup)
						+ "} to {" + newRCGroup + ":" + (ncEpoch) + "=" + newRCGroups
							.get(newRCGroup)) + "}\n";
				this.repeatUntilObviated(new RCRecordRequest<NodeIDType>(this
						.getMyID(),
						new StartEpoch<NodeIDType>(this.getMyID(), newRCGroup,
								ncEpoch, newRCGroups.get(newRCGroup),
								curRCGroups.get(newRCGroup), mergeLists
										.get(newRCGroup)), // mergees
						RequestTypes.RECONFIGURATION_INTENT));
			}
		}
		return debug;
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
						+ (ncRecord.getRCEpoch(oldGroupName) - 1) + "="
						+ oldGroup.get(oldGroupName) + "}\n";
				if (newRCGroup.equals(oldGroupName))
					continue; // no trivial splits

				this.DB.execute(new RCRecordRequest<NodeIDType>(this
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
		 * necessity/sufficiency of this constraint). For now, simple and safe
		 * is good enough.
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
				 * Register the mergee groups right here so that they can be
				 * available upon reconfiguration complete and can be executed
				 * sequentially in the new epoch. It is also easy to look at the
				 * RC record and determine if all the merges are done.
				 */
				this.protocolExecutor
						.spawnIfNotRunning(new WaitAckStopEpoch<NodeIDType>(
								new StartEpoch<NodeIDType>(this.getMyID(),
										mergeGroupName, ncRecord
												.getRCEpoch(mergeGroupName),
										mergeGroup.get(mergeGroupName),
										curRCGroups.get(curRCGroup),
										curRCGroup, true, ncRecord
												.getRCEpoch(curRCGroup)),
								this.DB));
			} else if (!newRCGroups.containsKey(curRCGroup)
					&& !this.DB.isBeingDeleted(curRCGroup)) {
				// delete current group and merge into a new "mergeGroup"
				debug += (this + " expecting others to delete current group {"
						+ curRCGroup + ":"
						+ (ncRecord.getRCEpoch(curRCGroup) - 1) + "=" + this.DB
							.getReplicaGroup(curRCGroup)) + "}\n";
			}
		}
		return debug;
	}
}