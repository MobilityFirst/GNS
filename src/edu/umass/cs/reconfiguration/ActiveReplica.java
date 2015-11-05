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
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.OverloadException;
import edu.umass.cs.gigapaxos.paxosutil.PaxosPacketDemultiplexer;
import edu.umass.cs.gigapaxos.paxosutil.StringContainer;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.AddressMessenger;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorCallback;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckDropEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckStartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckStopEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DefaultAppRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DropEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.EpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StopEpoch;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.ActiveReplicaProtocolTask;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.WaitEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.AggregateDemandProfiler;
import edu.umass.cs.reconfiguration.reconfigurationutils.CallbackMap;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationPacketDemultiplexer;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            This class is the main wrapper around active replicas of
 *            reconfigurable app instances. It processes requests both from the
 *            Reconfigurator as well as the underlying app's clients. This class
 *            is also use to wrap the Reconfigurator itself in order to
 *            reconfigure Reconfigurators (to correctly perform reconfigurator
 *            add/remove operations).
 * 
 *            <p>
 * 
 *            This class handles the following ReconfigurationPackets:
 *            {@code STOP_EPOCH,
 *            START_EPOCH, REQUEST_EPOCH_FINAL_STATE, DROP_EPOCH} as also listed
 *            in {@link ActiveReplicaProtocolTask ActiveReplicaProtocolTask}. It
 *            relies upon the app's implementation of AbstractDemandProfile in
 *            order to determine how frequenltly to report demand statistics to
 *            Reconfigurators and whether and how to reconfigure the current
 *            active replica placement.
 */
public class ActiveReplica<NodeIDType> implements
		ReconfiguratorCallback,
		PacketDemultiplexer<JSONObject> {
	/**
	 * Offset for client facing port that may in general be different from
	 * server-to-server communication as we may need different transport-layer
	 * security schemes for server-server compared to client-server
	 * communication.
	 * 
	 * The commented DEFAULT_CLIENT_PORT_OFFSET field below has been moved to
	 * ReconfigurationConfig.
	 */
	// public static int DEFAULT_CLIENT_PORT_OFFSET = 00; // default 100

	private final AbstractReplicaCoordinator<NodeIDType> appCoordinator;
	private final ConsistentReconfigurableNodeConfig<NodeIDType> nodeConfig;
	private final ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String> protocolExecutor;
	private final ActiveReplicaProtocolTask<NodeIDType> protocolTask;
	private final SSLMessenger<NodeIDType, ?> messenger;

	private final AggregateDemandProfiler demandProfiler;
	private final boolean noReporting;
	private boolean recovering = true;

	private static final Logger log = (Reconfigurator.getLogger());

	/*
	 * Stores only those requests for which a callback is desired after
	 * (coordinated) execution. StopEpoch is the only example of such a request
	 * in ActiveReplica.
	 */
	private final CallbackMap<NodeIDType> callbackMap = new CallbackMap<NodeIDType>();

	private ActiveReplica(AbstractReplicaCoordinator<NodeIDType> appC,
			ReconfigurableNodeConfig<NodeIDType> nodeConfig,
			SSLMessenger<NodeIDType, ?> messenger, boolean noReporting) {
		this.appCoordinator = appC
				.setStopCallback((ReconfiguratorCallback) this);
		this.nodeConfig = new ConsistentReconfigurableNodeConfig<NodeIDType>(
				nodeConfig);
		this.demandProfiler = new AggregateDemandProfiler(this.nodeConfig);
		this.messenger = messenger;
		this.protocolExecutor = new ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String>(
				messenger);
		this.protocolTask = new ActiveReplicaProtocolTask<NodeIDType>(
				getMyID(), this.nodeConfig, this);
		this.protocolExecutor.register(this.protocolTask.getDefaultTypes(),
				this.protocolTask);
		// FIXME: this is not doing much
		this.appCoordinator.setMessenger(this.messenger);
		this.noReporting = noReporting;
		if (this.messenger.getClientMessenger() == null) // exactly once
			this.messenger.setClientMessenger(initClientMessenger());
		assert(this.messenger.getClientMessenger()!=null);
		assert(this.appCoordinator.getMessenger()==this.messenger);
		this.recovering = false;
	}

	protected ActiveReplica(AbstractReplicaCoordinator<NodeIDType> appC,
			ReconfigurableNodeConfig<NodeIDType> nodeConfig,
			SSLMessenger<NodeIDType, JSONObject> messenger) {
		this(appC, nodeConfig, messenger, false);
	}

	@Override
	public boolean handleMessage(JSONObject jsonObject) {
		BasicReconfigurationPacket<NodeIDType> rcPacket = null;
		try {
			// try handling as reconfiguration packet through protocol task
			if (ReconfigurationPacket.isReconfigurationPacket(jsonObject)
					&& (rcPacket = this.protocolTask
							.getReconfigurationPacket(jsonObject)) != null) {
				if (!this.protocolExecutor.handleEvent(rcPacket)) {
					// do nothing
					log.log(Level.FINE, "{0} unable to handle packet {1}",
							new Object[] { this, jsonObject });
				}
			}
			// else check if app request
			else if (isAppRequest(jsonObject)) {
				Request request = this.getRequest(jsonObject);
				// send to app via its coordinator
				boolean handled = this.handRequestToApp(request);
				// if handled, update demand stats (for reconfigurator) 
				if (handled)
					updateDemandStats(request,
							MessageNIOTransport
									.getSenderInetAddress(jsonObject));
				// else send error message to sender
				else {
					if (request instanceof ClientRequest) {
						this.send(
								MessageNIOTransport
										.getSenderAddress(jsonObject),
								new ActiveReplicaError(this.nodeConfig
										.getNodeSocketAddress(getMyID()),
										request.getServiceName(),
										((ClientRequest) request)
												.getRequestID()).toJSONObject());
					}
				}
			}
		} catch (RequestParseException rpe) {
			rpe.printStackTrace();
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return false; // neither reconfiguration packet nor app request
	}

	// special case optimization for RequestPacket
	private Request getRequest(JSONObject jsonObject)
			throws RequestParseException, JSONException {
		if (JSONPacket.getPacketType(jsonObject) == PaxosPacket.PaxosPacketType.REQUEST
				.getInt())
			return new RequestPacket(jsonObject);
		return this.appCoordinator.getRequest(jsonObject.toString());
	}

	@Override
	public void executed(Request request, boolean handled) {
		if (this.isRecovering())
			return;
		assert (request instanceof ReconfigurableRequest);
		assert (handled);
		/*
		 * We need to handle the callback in a separate thread, otherwise we
		 * risk sending the ackStop before the epoch final state has been
		 * checkpointed that in turn has to happen strictly before the app
		 * itself has dropped the state. If we send the ackStop early for a
		 * delete request and the reconfigurator sends a delete confirmation to
		 * the client, a client read request may find the record undeleted (at
		 * each and every active replica) despite receiving the delete
		 * confirmation. This scenario is unlikely but can happen, especially if
		 * creating the final epoch state checkpoint takes long. The thread
		 * below waits for the app state to be deleted before sending out the
		 * ackStop. We need a separate thread because "this" thread is the one
		 * executing the stop request and is the one responsible for creating
		 * the epoch final state checkpoint, so it can not itself wait for that
		 * to complete without getting stuck.
		 * 
		 * Note that a client read may still find a record undeleted despite
		 * receiving a delete confirmation if it sends the read to an active
		 * replica other than the one that sent the ackStop to the
		 * reconfigurator as that other active replica may still be catching up.
		 * But the point above is that this scenario can happen even with a
		 * single replica, which is "wrong", unless we spawn a separate thread.
		 */
		if (handled)
			// protocol executor also allows us to just submit a Runnable
			this.protocolExecutor.submit(new AckStopNotifier(request));
	}

	/*
	 * This class is a task to notify reconfigurators of a successfully stopped
	 * replica group. It deletes the replica group and then sends the ackStop.
	 */
	class AckStopNotifier implements Runnable {
		Request request;

		AckStopNotifier(Request request) {
			this.request = request;
		}

		public void run() {
			StopEpoch<NodeIDType> stopEpoch = null;
			int epoch = ((ReconfigurableRequest) request)
					.getEpochNumber();
			while ((stopEpoch = callbackMap.notifyStop(
					request.getServiceName(), epoch)) != null) {
				appCoordinator.deleteReplicaGroup(request.getServiceName(),
						epoch);
				sendAckStopEpoch(stopEpoch);
			}
		}
	}

	/**
	 * @return Set of packet types processed by ActiveReplica.
	 */
	public Set<IntegerPacketType> getPacketTypes() {
		Set<IntegerPacketType> types = this.getAppPacketTypes();
		
		if (types == null)
			types = new HashSet<IntegerPacketType>();
		
		types.remove(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
		
		for (IntegerPacketType type : this.getActiveReplicaPacketTypes()) {
			types.add(type);
		}
		return types;
	}

	/**
	 * For graceful closure.
	 */
	public void close() {
		this.protocolExecutor.stop();
		this.messenger.stop();
	}

	// /////////////// Start of protocol task handler
	// methods//////////////////////

	/**
	 * Will spawn FetchEpochFinalState to fetch the final state of the previous
	 * epoch if one existed, else will locally create the current epoch with an
	 * empty initial state.
	 * 
	 * @param event
	 * @param ptasks
	 * @return Messaging task, typically null as we spawn a protocol task to
	 *         fetch the previous epoch's final state.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleStartEpoch(
			StartEpoch<NodeIDType> event,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		StartEpoch<NodeIDType> startEpoch = ((StartEpoch<NodeIDType>) event);
		this.logEvent(event, Level.FINE);
		AckStartEpoch<NodeIDType> ackStart = new AckStartEpoch<NodeIDType>(
				startEpoch.getSender(), startEpoch.getServiceName(),
				startEpoch.getEpochNumber(), getMyID());
		GenericMessagingTask<NodeIDType, ?>[] mtasks = (new GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>>(
				startEpoch.getSender(), ackStart)).toArray();
		// send positive ack even if app has moved on
		if (this.alreadyMovedOn(startEpoch)) {
			log.log(Level.FINE,
					"{0} sending to {1}: {2} as paxos group has already moved on to version {3}",
					new Object[] {
							this,
							startEpoch.getSender(),
							ackStart.getSummary(),
							this.appCoordinator.getEpoch(startEpoch
									.getServiceName()) });
			return mtasks;
		}
		// else
		// if no previous group, create replica group with empty state
		if (startEpoch.noPrevEpochGroup()) {
			// createReplicaGroup is a local operation (but may fail)
			boolean created = startEpoch.isBatchedCreate() ? this
					.batchedCreate(startEpoch) : this.appCoordinator
					.createReplicaGroup(startEpoch.getServiceName(),
							startEpoch.getEpochNumber(),
							startEpoch.getInitialState(),
							startEpoch.getCurEpochGroup());
			// the creation above will throw an exception if it fails
			assert (created && this.appCoordinator.getReplicaGroup(
					startEpoch.getServiceName()).equals(
					startEpoch.getCurEpochGroup()));
			log.log(Level.FINE, "{0} sending to {1}: {2}", new Object[] { this,
					startEpoch.getSender(), ackStart.getSummary() });

			return mtasks; // and also send positive ack
		}
		/*
		 * Else request previous epoch state using a threshold protocoltask. We
		 * spawn WaitEpochFinalState as opposed to simply returning it in
		 * ptasks[0] as otherwise we could end up creating tasks with duplicate
		 * keys.
		 */
		this.spawnWaitEpochFinalState(startEpoch);
		return null; // no messaging if asynchronously fetching state
	}

	private static final boolean BATCH_CREATION = true;
	/* This could be optimized further by supporting batch creation in 
	 * PaxosManager.
	 */
	private boolean batchedCreate(StartEpoch<NodeIDType> startEpoch) {
		boolean created = true;
		if (BATCH_CREATION)
			return this.appCoordinator.createReplicaGroup(startEpoch.getNameStates(),
					startEpoch.getCurEpochGroup());
		else
			for (String name : startEpoch.getNameStates().keySet()) {
				assert (startEpoch.getEpochNumber() == 0);
				created = created
						&& this.appCoordinator.createReplicaGroup(
								name,
								startEpoch.getEpochNumber(), // 0
								startEpoch.getNameStates().get(name),
								startEpoch.getCurEpochGroup());
			}
		return created;
	}

	// synchronized to ensure atomic testAndStart property
	private synchronized void spawnWaitEpochFinalState(
			StartEpoch<NodeIDType> startEpoch) {
		WaitEpochFinalState<NodeIDType> waitFinal = new WaitEpochFinalState<NodeIDType>(
				getMyID(), startEpoch, this.appCoordinator);
		if (!this.protocolExecutor.isRunning(waitFinal.getKey()))
			this.protocolExecutor.spawn(waitFinal);
		else {
			WaitEpochFinalState<NodeIDType> running = (WaitEpochFinalState<NodeIDType>) this.protocolExecutor
					.getTask(waitFinal.getKey());
			if (running != null)
				running.addNotifiee(startEpoch.getInitiator(),
						startEpoch.getKey());
		}
	}

	/**
	 * @param stopEpoch
	 * @param ptasks
	 * @return Messaging task, typically null as we coordinate the stop request
	 *         and use a callback to notify the reconfigurator that issued the
	 *         {@link StopEpoch}.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleStopEpoch(
			StopEpoch<NodeIDType> stopEpoch,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		this.logEvent(stopEpoch);
		if (this.stoppedOrMovedOn(stopEpoch))
			return this.sendAckStopEpoch(stopEpoch).toArray(); // still send ack
		// else coordinate stop with callback
		this.callbackMap.addStopNotifiee(stopEpoch);
		ReconfigurableRequest appStop = this.getAppStopRequest(
				stopEpoch.getServiceName(), stopEpoch.getEpochNumber());
		log.log(Level.FINE,
				"{0} coordinating {1} as {2}:{3}",
				new Object[] { this, stopEpoch.getSummary(),
						appStop.getServiceName(), appStop.getEpochNumber() });
		this.appCoordinator.handleIncoming(appStop);
		return null; // need to wait until callback
	}

	/**
	 * @param event
	 * @param ptasks
	 * @return Messaging task to send AckDropEpochFinalState to reconfigurator
	 *         that issued the corresponding DropEpochFinalState.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleDropEpochFinalState(
			DropEpochFinalState<NodeIDType> event,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		this.logEvent(event);
		DropEpochFinalState<NodeIDType> dropEpoch = (DropEpochFinalState<NodeIDType>) event;
		boolean deleted = this.appCoordinator.deleteFinalState(
				dropEpoch.getServiceName(), dropEpoch.getEpochNumber());
		AckDropEpochFinalState<NodeIDType> ackDrop = new AckDropEpochFinalState<NodeIDType>(
				getMyID(), dropEpoch);
		GenericMessagingTask<NodeIDType, AckDropEpochFinalState<NodeIDType>> mtask = new GenericMessagingTask<NodeIDType, AckDropEpochFinalState<NodeIDType>>(
				dropEpoch.getInitiator(), ackDrop);
		assert (ackDrop.getInitiator().equals(dropEpoch.getInitiator()));
		log.log(deleted ? Level.FINE : Level.FINE,
				"{0} {1} sending {2} to {3}",
				new Object[] { this, deleted ? "" : "*NOT*",
						ackDrop.getSummary(), ackDrop.getInitiator() });
		this.garbageCollectPendingTasks(dropEpoch);
		return deleted ? mtask.toArray() : null;
	}

	// drop any pending task (only WaitEpochFinalState possible) upon dropEpoch
	private void garbageCollectPendingTasks(
			DropEpochFinalState<NodeIDType> dropEpoch) {
		/*
		 * Can drop waiting on epoch final state of the epoch just before the
		 * epoch being dropped as we don't have to bother starting the dropped
		 * epoch after all.
		 */
		boolean removed = (this.protocolExecutor.remove(Reconfigurator
				.getTaskKeyPrev(WaitEpochFinalState.class, dropEpoch, this
						.getMyID().toString())) != null);
		if (removed)
			log.log(Level.FINEST,
					"{0} removed WaitEpochFinalState {1}:{2}",
					new Object[] { this, dropEpoch.getServiceName(),
							(dropEpoch.getEpochNumber() - 1) });
	}

	/**
	 * @param event
	 * @param ptasks
	 * @return Messaging task returning the requested epoch final state to the
	 *         requesting ActiveReplica.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleRequestEpochFinalState(
			RequestEpochFinalState<NodeIDType> event,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		RequestEpochFinalState<NodeIDType> request = (RequestEpochFinalState<NodeIDType>) event;
		this.logEvent(event);
		StringContainer stateContainer = this.getFinalStateContainer(
				request.getServiceName(), request.getEpochNumber());
		if (stateContainer == null) {
			log.log(Level.INFO,
					"{0} ####did not find any epoch final state for#### {1}",
					new Object[] { this, request.getSummary() });
			return null;
		}

		EpochFinalState<NodeIDType> epochState = new EpochFinalState<NodeIDType>(
				request.getInitiator(), request.getServiceName(),
				request.getEpochNumber(), stateContainer.state);
		GenericMessagingTask<NodeIDType, EpochFinalState<NodeIDType>> mtask = null;

		log.log(Level.INFO, "{0} returning epoch final state to {1} {2}",
				new Object[] { this, event.getKey(), epochState.getSummary() });
		mtask = new GenericMessagingTask<NodeIDType, EpochFinalState<NodeIDType>>(
				request.getInitiator(), epochState);

		return (mtask != null ? mtask.toArray() : null);
	}

	private StringContainer getFinalStateContainer(String name, int epoch) {
		if (this.appCoordinator instanceof PaxosReplicaCoordinator)
			return ((PaxosReplicaCoordinator<NodeIDType>) (this.appCoordinator))
					.getFinalStateContainer(name, epoch);
		String finalState = this.appCoordinator.getFinalState(name, epoch);
		return finalState == null ? null : new StringContainer(finalState);
	}

	public String toString() {
		return "AR" + this.messenger.getMyID();
	}

	/*
	 * ************ End of protocol task handler methods *************
	 */

	/*
	 * ****************** Private methods below *******************
	 */

	private boolean handRequestToApp(Request request) {
		long t1 = System.currentTimeMillis();
		boolean handled = false;
		try {
			handled = this.appCoordinator.handleIncoming(request);
		} catch (OverloadException re) {
			PaxosPacketDemultiplexer.throttleExcessiveLoad();
		}
		if (Util.oneIn(Integer.MAX_VALUE))
			DelayProfiler.updateDelay("appHandleIncoming@AR", t1);
		return handled;
	}

	private boolean stoppedOrMovedOn(BasicReconfigurationPacket<?> packet) {
		boolean retval = false;
		Integer epoch = this.appCoordinator.getEpoch(packet.getServiceName());
		if (epoch != null // has state
				// and moved on
				&& ((epoch - packet.getEpochNumber() > 0)
				// or same epoch but no replica group (because already stopped)
				|| (epoch == packet.getEpochNumber() && this.appCoordinator
						.getReplicaGroup(packet.getServiceName()) == null)))
			retval = true;
		if (retval)
			log.log(Level.INFO,
					"{0} has no state or has already moved on to epoch {1} when received {2}",
					new Object[] { this, epoch, packet.getSummary() });
		else
			log.log(Level.INFO,
					"{0} has {1}:{2} with replica group {3} when asked to stop {4}:{5}",
					new Object[] {
							this,
							packet.getServiceName(),
							epoch,
							this.appCoordinator.getReplicaGroup(packet
									.getServiceName()),
							packet.getServiceName(), packet.getEpochNumber() });
		return retval;
	}

	// true means move on, so the replica group won't get created
	private boolean alreadyMovedOn(BasicReconfigurationPacket<?> packet) {
		Integer epoch = this.appCoordinator.getEpoch(packet.getServiceName());
		if (epoch != null)
			if (epoch - packet.getEpochNumber() > 0)
				return true;
			/*
			 * If state for this epoch exists and the replica group is not null,
			 * this is an active replica group, so we return true so that the
			 * replica group is not created; else the replica group is stopped
			 * locally and the only reason we got a re-creation request from
			 * reconfigurators is because it is legitimate to do so (i..e, the
			 * delete pending period has passed), so we return false so that the
			 * replica group is created after forcibly wiping out previous epoch
			 * final state that must be necessarily outdated at this point.
			 */
			else if (epoch == packet.getEpochNumber())
				// active epoch
				return (this.appCoordinator.getReplicaGroup(packet
						.getServiceName()) != null);
		return false;
	}

	private NodeIDType getMyID() {
		return this.messenger.getMyID();
	}

	private Set<ReconfigurationPacket.PacketType> getActiveReplicaPacketTypes() {
		return this.protocolTask.getEventTypes();
	}

	private Set<IntegerPacketType> getAppPacketTypes() {
		return this.appCoordinator.getRequestTypes();
	}

	private boolean isAppRequest(JSONObject jsonObject) throws JSONException {
		int type = JSONPacket.getPacketType(jsonObject);
		Set<IntegerPacketType> appTypes = this.appCoordinator.getRequestTypes();
		boolean contains = false;
		for (IntegerPacketType reqType : appTypes) {
			if (reqType.getInt() == type) {
				contains = true;
			}
		}
		return contains;
	}

	/*
	 * Demand stats are updated upon every request. Demand reports are
	 * dispatched to reconfigurators only if warranted by the shouldReport
	 * method. This allows for reporting policies that locally aggregate some
	 * stats based on a threshold number of requests before reporting to
	 * reconfigurators.
	 */
	private void updateDemandStats(Request request, InetAddress sender) {
		if (this.noReporting)
			return;

		String name = request.getServiceName();
		if (request instanceof ReconfigurableRequest
				&& ((ReconfigurableRequest) request).isStop())
			return; // no reporting on stop
		if (this.demandProfiler.register(request, sender).shouldReport())
			report(this.demandProfiler.pluckDemandProfile(name));
		else
			report(this.demandProfiler.trim());
	}

	/*
	 * Report demand stats to reconfigurators. This method will necessarily
	 * result in a stats message being sent out to reconfigurators.
	 */
	private void report(AbstractDemandProfile demand) {
		try {
			NodeIDType reportee = selectReconfigurator(demand.getName());
			assert (reportee != null);
			/*
			 * We don't strictly need the epoch number in demand reports, but it
			 * is useful for debugging purposes.
			 */
			Integer epoch = this.appCoordinator.getEpoch(demand.getName());
			GenericMessagingTask<NodeIDType, ?> mtask = new GenericMessagingTask<NodeIDType, Object>(
					reportee, (new DemandReport<NodeIDType>(getMyID(),
							demand.getName(), (epoch == null ? 0 : epoch),
							demand)).toJSONObject());
			this.send(mtask);
		} catch (JSONException je) {
			je.printStackTrace();
		}
	}

	/*
	 * Returns a random reconfigurator. Util.selectRandom is designed to return
	 * a value of the same type as the objects in the input set, so it is okay
	 * to suppress the warning.
	 */
	@SuppressWarnings("unchecked")
	private NodeIDType selectReconfigurator(String name) {
		Set<NodeIDType> reconfigurators = this.getReconfigurators(name);
		return (NodeIDType) Util.selectRandom(reconfigurators);
	}

	private Set<NodeIDType> getReconfigurators(String name) {
		return this.nodeConfig.getReplicatedReconfigurators(name);
	}

	private void send(GenericMessagingTask<NodeIDType, ?> mtask) {
		try {
			this.messenger.send(mtask);
		} catch (JSONException je) {
			je.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	private void send(InetSocketAddress isa, JSONObject msg) {
		try {
			((AddressMessenger<JSONObject>) this.messenger)
					.sendToAddress(isa, msg);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void report(Set<AbstractDemandProfile> demands) {
		if (demands != null && !demands.isEmpty())
			for (AbstractDemandProfile demand : demands)
				this.report(demand);
	}

	private GenericMessagingTask<NodeIDType, ?> sendAckStopEpoch(
			StopEpoch<NodeIDType> stopEpoch) {
		// inform reconfigurator
		AckStopEpoch<NodeIDType> ackStopEpoch = new AckStopEpoch<NodeIDType>(
				this.getMyID(), stopEpoch,
				(stopEpoch.shouldGetFinalState() ? this.appCoordinator
						.getFinalState(stopEpoch.getServiceName(),
								stopEpoch.getEpochNumber()) : null));
		GenericMessagingTask<NodeIDType, ?> mtask = new GenericMessagingTask<NodeIDType, Object>(
				(stopEpoch.getInitiator()), ackStopEpoch);
		log.log(Level.INFO, "{0} sending {1} to {2}", new Object[] { this,
				ackStopEpoch.getSummary(), stopEpoch.getInitiator() });
		this.send(mtask);
		return mtask;
	}

	private ReconfigurableRequest getAppStopRequest(String name,
			int epoch) {
		ReconfigurableRequest appStop = this.appCoordinator
				.getStopRequest(name, epoch);
		return appStop == null ? new DefaultAppRequest(name, epoch, true)
				: appStop;
	}

	/*
	 * We may need to use a separate messenger for end clients if we use two-way
	 * authentication between servers.
	 * 
	 * FIXME: The class casts below are bad and we probably need a cleaner way
	 * to really support generic message types.
	 */
	@SuppressWarnings("unchecked")
	private AddressMessenger<JSONObject> initClientMessenger() {
		AbstractPacketDemultiplexer<JSONObject> pd = null;
		Messenger<InetSocketAddress, JSONObject> cMsgr = null;
		if (this.appCoordinator.getAppRequestTypes().isEmpty())
			return null;
		try {
			int myPort = (this.nodeConfig.getNodePort(getMyID()));
			if (getClientFacingPort(myPort) != myPort) {
				log.log(Level.INFO,
						"Creating client messenger at {0}:{1}",
						new Object[] {
								this.nodeConfig.getBindAddress(getMyID()),
								getClientFacingPort(myPort) });

				if (this.messenger.getClientMessenger() == null
						|| this.messenger.getClientMessenger() != this.messenger)
					cMsgr = new JSONMessenger<InetSocketAddress>(
							new MessageNIOTransport<InetSocketAddress, JSONObject>(
									this.nodeConfig.getBindAddress(getMyID()),
									getClientFacingPort(myPort),
									/*
									 * Client facing demultiplexer is single
									 * threaded to keep clients from
									 * overwhelming the system with request
									 * load.
									 */
									(pd = new ReconfigurationPacketDemultiplexer(
											0)), ReconfigurationConfig
											.getClientSSLMode()));
				else if (this.messenger.getClientMessenger() instanceof Messenger)
					((Messenger<NodeIDType, ?>) this.messenger
							.getClientMessenger())
							.addPacketDemultiplexer(pd = new ReconfigurationPacketDemultiplexer(
									0));
				pd.register(this.appCoordinator.getAppRequestTypes(), this);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return cMsgr != null ? cMsgr
				: (AddressMessenger<JSONObject>) this.messenger;
	}

	private boolean isRecovering() {
		return this.recovering;
	}

	/**
	 * @param port
	 * @return The client facing port number corresponding to port.
	 */
	public static int getClientFacingPort(int port) {
		return port + ReconfigurationConfig.getClientPortOffset();
	}

	private void logEvent(BasicReconfigurationPacket<NodeIDType> event) {
		log.log(Level.FINE, "{0} received {1} from {2}", new Object[] { this,
				event.getSummary(), event.getSender() });
	}

	private void logEvent(BasicReconfigurationPacket<NodeIDType> event,
			Level level) {
		log.log(level, "{0} received {1} from {2}",
				new Object[] { this, event.getSummary(), event.getSender() });
	}

	@Override
	public void preExecuted(Request request) {
		throw new RuntimeException("This method should not have gotten called");
	}
}
