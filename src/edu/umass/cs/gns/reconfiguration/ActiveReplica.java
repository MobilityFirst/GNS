package edu.umass.cs.gns.reconfiguration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONPacket;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.ActiveReplicaProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.WaitEpochFinalState;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.AckDropEpochFinalState;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.AckStartEpoch;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.AckStopEpoch;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DropEpochFinalState;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.EpochFinalState;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RequestEpochFinalState;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StopEpoch;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.AggregateDemandProfiler;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.CallbackMap;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.util.MyLogger;
import edu.umass.cs.gns.util.Stringifiable;
import edu.umass.cs.gns.util.Util;

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
				this.protocolExecutor.handleEvent(rcPacket);
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
		GenericMessagingTask<NodeIDType, ?>[] mtasks = (new GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>>(
				startEpoch.getSender(), new AckStartEpoch<NodeIDType>(
						startEpoch.getSender(), startEpoch.getServiceName(),
						startEpoch.getEpochNumber(), getMyID()))).toArray();
		// send positive ack even if app has moved on
		if (this.alreadyMovedOn(startEpoch))
			return mtasks;
		// else
		// if no previous group, create replica group with empty state
		if (startEpoch.getPrevEpochGroup() == null
				|| startEpoch.getPrevEpochGroup().isEmpty()) {
			// createReplicaGroup is a local operation
			this.appCoordinator.createReplicaGroup(startEpoch.getServiceName(),
					startEpoch.getEpochNumber(), startEpoch.getInitialState(),
					startEpoch.getCurEpochGroup());
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

	private String getTaskKeyPrev(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket) {
		return getTaskKey(C, rcPacket.getServiceName(),
				rcPacket.getEpochNumber() - 1);
	}

	private String getTaskKey(Class<?> C, String name, int epoch) {
		return C.getSimpleName() + this.getMyID() + ":" + name + ":" + epoch;
	}

	public GenericMessagingTask<NodeIDType, ?>[] handleStopEpoch(
			StopEpoch<NodeIDType> stopEpoch,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		this.logEvent(stopEpoch);
		if (this.noStateOrAlreadyMovedOn(stopEpoch)) {
			log.info(this + " has no state or already moved on "
					+ stopEpoch.getSummary());
			return this.sendAckStopEpoch(stopEpoch).toArray(); // still send ack
		}
		// else coordinate stop with callback
		this.callbackMap.addStopNotifiee(stopEpoch);
		this.appCoordinator.handleIncoming(this.appCoordinator.getStopRequest(
				stopEpoch.getServiceName(), stopEpoch.getEpochNumber()));
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
		boolean removed = (this.protocolExecutor.remove(getTaskKeyPrev(
				WaitEpochFinalState.class, dropEpoch)) != null);
		if (removed)
			System.out.println(this + " removed WaitEpochFinalState"
					+ dropEpoch.getServiceName() + ":"
					+ (dropEpoch.getEpochNumber() - 1));
	}

	public GenericMessagingTask<NodeIDType, ?>[] handleRequestEpochFinalState(
			RequestEpochFinalState<NodeIDType> event,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		RequestEpochFinalState<NodeIDType> request = (RequestEpochFinalState<NodeIDType>) event;
		this.logEvent(event);
		EpochFinalState<NodeIDType> epochState = new EpochFinalState<NodeIDType>(
				request.getInitiator(), request.getServiceName(),
				request.getEpochNumber(), this.getFinalState(
						request.getServiceName(), request.getEpochNumber()));
		GenericMessagingTask<NodeIDType, EpochFinalState<NodeIDType>> mtask = null;

		if (epochState.getState() != null) {
			log.log(Level.INFO, MyLogger.FORMAT[3], new Object[] { this,
					"returning to ", request.getInitiator(), epochState });
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

	private String getFinalState(String name, int epoch) {
		String state = this.appCoordinator.getFinalState(name, epoch);
		if (state == null || !this.appCoordinator.hasLargeCheckpoints())
			return state;

		String actualState = null;
		BufferedReader br = null;
		try {
			// read state from file
			if (new File(state).exists()) {
				br = new BufferedReader(new InputStreamReader(
						new FileInputStream(state)));
				String line = null;
				while ((line = br.readLine()) != null) {
					actualState = (actualState == null ? line
							: (actualState + line));
				}
			}
		} catch (IOException e) {
			log.severe(this + " unable to read actual state from file");
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException e) {
				log.severe(this + " unable to close checkpoint file " + state);
				e.printStackTrace();
			}
		}
		return actualState;
	}

	private boolean noStateOrAlreadyMovedOn(BasicReconfigurationPacket<?> packet) {
		Integer epoch = this.appCoordinator.getEpoch(packet.getServiceName());
		// no state or higher epoch
		if (epoch == null || (epoch - packet.getEpochNumber() > 0))
			return true;
		// FIXME: same epoch but no replica group (or stopped)
		else if (epoch == packet.getEpochNumber()
				&& this.appCoordinator.getReplicaGroup(packet.getServiceName()) == null) {
			return true;
		}
		return false;
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
				stopEpoch.getInitiator(), stopEpoch);
		GenericMessagingTask<NodeIDType, ?> mtask = new GenericMessagingTask<NodeIDType, Object>(
				(stopEpoch.getInitiator()), ackStopEpoch);
		log.log(Level.INFO, MyLogger.FORMAT[5], new Object[] { this, "sending",
				ackStopEpoch.getType(), ackStopEpoch.getServiceName(),
				ackStopEpoch.getEpochNumber(), mtask });
		this.send(mtask);
		return mtask;
	}

	private void logEvent(BasicReconfigurationPacket<NodeIDType> event) {
		log.log(Level.INFO,
				MyLogger.FORMAT[6],
				new Object[] { this, "received", event.getType(),
						event.getServiceName(), event.getEpochNumber(),
						"from " + event.getSender(), event });
	}
}
