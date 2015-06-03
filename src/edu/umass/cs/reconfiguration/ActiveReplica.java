package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.reconfiguration.json.ActiveReplicaProtocolTask;
import edu.umass.cs.reconfiguration.json.WaitEpochFinalState;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.AckDropEpochFinalState;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.AckStartEpoch;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.AckStopEpoch;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.DefaultAppRequest;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.DropEpochFinalState;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.EpochFinalState;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.RequestEpochFinalState;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.StopEpoch;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.AggregateDemandProfiler;
import edu.umass.cs.reconfiguration.reconfigurationutils.CallbackMap;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 */
public class ActiveReplica<NodeIDType> implements
		InterfaceReconfiguratorCallback, InterfacePacketDemultiplexer {
	private final AbstractReplicaCoordinator<NodeIDType> appCoordinator;
	private final ConsistentReconfigurableNodeConfig<NodeIDType> nodeConfig;
	private final ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String> protocolExecutor;
	private final ActiveReplicaProtocolTask<NodeIDType> protocolTask;
	private final JSONMessenger<NodeIDType> messenger;

	private final AggregateDemandProfiler demandProfiler;
	private final boolean noReporting;

	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	/*
	 * Stores only those requests for which a callback is desired after
	 * (coordinated) execution. StopEpoch is the only example of such a request
	 * in ActiveReplica.
	 */
	private final CallbackMap<NodeIDType> callbackMap = new CallbackMap<NodeIDType>();

	public ActiveReplica(AbstractReplicaCoordinator<NodeIDType> appC,
			InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig,
			JSONMessenger<NodeIDType> messenger, boolean noReporting) {
		this.appCoordinator = appC
				.setActiveCallback((InterfaceReconfiguratorCallback) this);
		this.nodeConfig = new ConsistentReconfigurableNodeConfig<NodeIDType>(
				nodeConfig);
                this.demandProfiler = new AggregateDemandProfiler(this.nodeConfig);
		this.messenger = messenger;
		this.protocolExecutor = new ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String>(
				messenger);
		this.protocolTask = new ActiveReplicaProtocolTask<NodeIDType>(
				getMyID(), this);
		this.protocolExecutor.register(this.protocolTask.getDefaultTypes(),
				this.protocolTask);
		this.appCoordinator.setMessenger(this.messenger);
		this.noReporting = noReporting;
	}

	public ActiveReplica(AbstractReplicaCoordinator<NodeIDType> appC,
			InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig,
			JSONMessenger<NodeIDType> messenger) {
		this(appC, nodeConfig, messenger, false);
	}

	public Stringifiable<NodeIDType> getUnstringer() {
		return this.nodeConfig;
	}

	@Override
	public boolean handleJSONObject(JSONObject jsonObject) {
		BasicReconfigurationPacket<NodeIDType> rcPacket = null;
		try {
			// try handling as reconfiguration packet through protocol task
			if (ReconfigurationPacket.isReconfigurationPacket(jsonObject)
					&& (rcPacket = this.protocolTask
							.getReconfigurationPacket(jsonObject)) != null) {
				if (!this.protocolExecutor.handleEvent(rcPacket)) {
					// do nothing
					log.log(Level.INFO, MyLogger.FORMAT[2], new Object[] {
							this, "unable to handle packet", jsonObject });
				}
			}
			// else check if app request
			else if (isAppRequest(jsonObject)) {
				InterfaceRequest request = this.appCoordinator
						.getRequest(jsonObject.toString());
				// send to app via its coordinator
				this.appCoordinator.handleIncoming(request);
				// update demand stats (for reconfigurator) if handled by app
				updateDemandStats(request,
						JSONPacket.getSenderAddress(jsonObject));
			}
		} catch (RequestParseException rpe) {
			rpe.printStackTrace();
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return false; // neither reconfiguration packet nor app request
	}

	@Override
	public void executed(InterfaceRequest request, boolean handled) {
		assert (request instanceof InterfaceReconfigurableRequest);
		int epoch = ((InterfaceReconfigurableRequest) request).getEpochNumber();
		StopEpoch<NodeIDType> stopEpoch = null;
		if (handled)
			while ((stopEpoch = this.callbackMap.notifyStop(
					request.getServiceName(), epoch)) != null)
				this.sendAckStopEpoch(stopEpoch);
	}

	public Set<IntegerPacketType> getPacketTypes() {
		Set<IntegerPacketType> types = this.getAppPacketTypes();
		if (types == null)
			types = new HashSet<IntegerPacketType>();
		for (IntegerPacketType type : this.getActiveReplicaPacketTypes()) {
			types.add(type);
		}
		return types;
	}

	public void close() {
		this.protocolExecutor.stop();
		this.messenger.stop();
	}

	/********************* Start of protocol task handler methods ************************/

	/*
	 * Will spawn FetchEpochFinalState to fetch the final state of the previous
	 * epoch if one existed, else will locally create the current epoch with an
	 * empty initial state.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleStartEpoch(
			StartEpoch<NodeIDType> event,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		StartEpoch<NodeIDType> startEpoch = ((StartEpoch<NodeIDType>) event);
		this.logEvent(event);
		AckStartEpoch<NodeIDType> ackStart = new AckStartEpoch<NodeIDType>(
				startEpoch.getSender(), startEpoch.getServiceName(),
				startEpoch.getEpochNumber(), getMyID());
		GenericMessagingTask<NodeIDType, ?>[] mtasks = (new GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>>(
				startEpoch.getSender(), ackStart)).toArray();
		// send positive ack even if app has moved on
		if (this.alreadyMovedOn(startEpoch)) {
			log.info(this + " sending to " + startEpoch.getSender() + ": " + ackStart.getSummary());
			return mtasks;
		}
		// else
		// if no previous group, create replica group with empty state
		if (startEpoch.getPrevEpochGroup() == null
				|| startEpoch.getPrevEpochGroup().isEmpty()) {
			// createReplicaGroup is a local operation
			this.appCoordinator.createReplicaGroup(startEpoch.getServiceName(),
					startEpoch.getEpochNumber(), startEpoch.getInitialState(),
					startEpoch.getCurEpochGroup());
			log.info(this + " sending to " + startEpoch.getSender() + ": " + ackStart.getSummary());
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

	public GenericMessagingTask<NodeIDType, ?>[] handleStopEpoch(
			StopEpoch<NodeIDType> stopEpoch,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		this.logEvent(stopEpoch);
		if (this.noStateOrAlreadyMovedOn(stopEpoch))
			return this.sendAckStopEpoch(stopEpoch).toArray(); // still send ack
		// else coordinate stop with callback
		this.callbackMap.addStopNotifiee(stopEpoch);
		log.info(this + " coordinating " + stopEpoch.getSummary());
		this.appCoordinator.handleIncoming(this.getAppStopRequest(stopEpoch.getServiceName(), stopEpoch.getEpochNumber()));
		return null; // need to wait until callback
	}

	public GenericMessagingTask<NodeIDType, ?>[] handleDropEpochFinalState(
			DropEpochFinalState<NodeIDType> event,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		this.logEvent(event);
		DropEpochFinalState<NodeIDType> dropEpoch = (DropEpochFinalState<NodeIDType>) event;
		this.appCoordinator.deleteFinalState(dropEpoch.getServiceName(),
				dropEpoch.getEpochNumber());
		AckDropEpochFinalState<NodeIDType> ackDrop = new AckDropEpochFinalState<NodeIDType>(
				getMyID(), dropEpoch);
		GenericMessagingTask<NodeIDType, AckDropEpochFinalState<NodeIDType>> mtask = new GenericMessagingTask<NodeIDType, AckDropEpochFinalState<NodeIDType>>(
				dropEpoch.getInitiator(), ackDrop);
		assert(ackDrop.getInitiator().equals(dropEpoch.getInitiator()));
		log.info(this + " sending " + ackDrop.getSummary() + " to "
				+ ackDrop.getInitiator() + ": " + ackDrop);
		this.garbageCollectPendingTasks(dropEpoch);
		return mtask.toArray();
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
			log.log(Level.INFO, MyLogger.FORMAT[4], new Object[] {this , " removed WaitEpochFinalState"
					, dropEpoch.getServiceName() , ":"
					, (dropEpoch.getEpochNumber() - 1)});
	}

	public GenericMessagingTask<NodeIDType, ?>[] handleRequestEpochFinalState(
			RequestEpochFinalState<NodeIDType> event,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		RequestEpochFinalState<NodeIDType> request = (RequestEpochFinalState<NodeIDType>) event;
		this.logEvent(event);
		EpochFinalState<NodeIDType> epochState = new EpochFinalState<NodeIDType>(
				request.getInitiator(), request.getServiceName(),
				request.getEpochNumber(), this.appCoordinator.getFinalState(
						request.getServiceName(), request.getEpochNumber()));
		GenericMessagingTask<NodeIDType, EpochFinalState<NodeIDType>> mtask = null;

		if (epochState.getState() != null) {
			log.log(Level.INFO, MyLogger.FORMAT[4], new Object[] { this,
					"returning to ", request.getInitiator(), event.getKey(),
					epochState });
			mtask = new GenericMessagingTask<NodeIDType, EpochFinalState<NodeIDType>>(
					request.getInitiator(), epochState);
		} else
			log.log(Level.INFO, MyLogger.FORMAT[2], new Object[] { this,
					"****not returning*****", epochState });

		return (mtask != null ? mtask.toArray() : null);
	}

	public String toString() {
		return "AR" + this.messenger.getMyID();
	}

	/********************* End of protocol task handler methods ************************/

	/*********************** Private methods below ************************************/

	private boolean noStateOrAlreadyMovedOn(BasicReconfigurationPacket<?> packet) {
		boolean retval = false;
		Integer epoch = this.appCoordinator.getEpoch(packet.getServiceName());
		// no state or higher epoch
		if (epoch == null || (epoch - packet.getEpochNumber() > 0))
			retval = true;
		// FIXME: same epoch but no replica group (or stopped)
		else if (epoch == packet.getEpochNumber()
				&& this.appCoordinator.getReplicaGroup(packet.getServiceName()) == null)
			retval = true;
		if (retval)
			log.info(this + " has no state or already moved on "
					+ packet.getSummary());
		return retval;
	}

	private boolean alreadyMovedOn(BasicReconfigurationPacket<?> packet) {
		Integer epoch = this.appCoordinator.getEpoch(packet.getServiceName());
		if (epoch != null && epoch - packet.getEpochNumber() >= 0)
			return true;
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
	private void updateDemandStats(InterfaceRequest request, InetAddress sender) {
		if (this.noReporting)
			return;

		String name = request.getServiceName();
		if (request instanceof InterfaceReconfigurableRequest
				&& ((InterfaceReconfigurableRequest) request).isStop())
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
		log.log(Level.INFO, MyLogger.FORMAT[5], new Object[] { this, "sending",
				ackStopEpoch.getType(), ackStopEpoch.getServiceName(),
				ackStopEpoch.getEpochNumber(), mtask });
		this.send(mtask);
		return mtask;
	}
	
	private InterfaceReconfigurableRequest getAppStopRequest(String name,
			int epoch) {
		InterfaceReconfigurableRequest appStop = this.appCoordinator
				.getStopRequest(name, epoch);
		return appStop == null ? new DefaultAppRequest(name, epoch, true)
				: appStop;
	}

	private void logEvent(BasicReconfigurationPacket<NodeIDType> event) {
		log.log(Level.INFO,
				MyLogger.FORMAT[6],
				new Object[] { this, "received", event.getType(),
						event.getServiceName(), event.getEpochNumber(),
						"from " + event.getSender(), event });
	}
}
