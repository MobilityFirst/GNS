package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;
import java.net.InetAddress;
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
@author V. Arun
 */
public class ActiveReplica<NodeIDType> implements  InterfaceReconfiguratorCallback, 
InterfacePacketDemultiplexer {
	private final AbstractReplicaCoordinator<NodeIDType> appCoordinator;
	private final ConsistentReconfigurableNodeConfig<NodeIDType> nodeConfig;
	private final ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String> protocolExecutor;
	private final ActiveReplicaProtocolTask<NodeIDType> protocolTask;
	private final JSONMessenger<NodeIDType> messenger;

	private final AggregateDemandProfiler demandProfiler = new AggregateDemandProfiler();
	
	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());
	
	/* Stores only those requests for which a callback is desired after (coordinated) execution.
	 * StopEpoch is the only example of such a request in ActiveReplica.
	 */
	private final CallbackMap<NodeIDType> callbackMap = new CallbackMap<NodeIDType>();
	
	public ActiveReplica(AbstractReplicaCoordinator<NodeIDType> appC, 
			InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig, JSONMessenger<NodeIDType> messenger) {
		this.appCoordinator = appC.setCallback((InterfaceReconfiguratorCallback)this);
		this.nodeConfig = new ConsistentReconfigurableNodeConfig<NodeIDType>(nodeConfig);
		this.messenger = messenger;
		this.protocolExecutor = new ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String>(messenger);
		this.protocolTask = new ActiveReplicaProtocolTask<NodeIDType>(getMyID(), this);
		this.protocolExecutor.register(this.protocolTask.getDefaultTypes(), this.protocolTask);
		this.appCoordinator.setMessenger(this.messenger);
	}
	
	public Stringifiable<NodeIDType> getUnstringer() {
		return this.nodeConfig;
	}

	@Override
	public boolean handleJSONObject(JSONObject jsonObject) {
		BasicReconfigurationPacket<NodeIDType> rcPacket = null;
		try {
			// try handling as reconfiguration packet through protocol task 
			if(ReconfigurationPacket.isReconfigurationPacket(jsonObject) && 
					(rcPacket = this.protocolTask.getReconfigurationPacket(jsonObject))!=null) {
				this.protocolExecutor.handleEvent(rcPacket);
			}
			// else check if app request
			else if(isAppRequest(jsonObject)) { 
				InterfaceRequest request = this.appCoordinator.getRequest(jsonObject.toString());
				// send to app via its coordinator
				this.appCoordinator.handleIncoming(request); 
				// update demand stats (for reconfigurator) if handled by app
				updateDemandStats(request, JSONPacket.getSenderAddress(jsonObject)); 
			}
		} catch(RequestParseException rpe) {
			rpe.printStackTrace();
		} catch(JSONException je) {
			je.printStackTrace();
		}

		return false; // neither reconfiguration packet nor app request
	}
	
	@Override
	public void executed(InterfaceRequest request, boolean handled) {
		StopEpoch<NodeIDType> stopEpoch = (StopEpoch<NodeIDType>)(
				this.callbackMap.get(request.getServiceName()));
		if(stopEpoch==null) return;
		 /* 
		 * Currently, the map is being maintained to translate from
		 * the app stop request to StopRequest from a reconfigurator
		 * to an active replica. But if we know the request is
		 * of type InterfaceReconfigurableRequest, we can get
		 * the name and epoch, and simply send the ack stop
		 * message to all RCs.
		 */
		this.callbackMap.remove(stopEpoch.getServiceName());
		if(handled) this.sendAckStopEpoch(stopEpoch);
	}
	
	public Set<IntegerPacketType> getPacketTypes() {
		Set<IntegerPacketType> types = this.getAppPacketTypes();
		for(IntegerPacketType type : this.getActiveReplicaPacketTypes()) {
			types.add(type);
		}
		return types;
	}
	
	public void close() {
		this.protocolExecutor.stop();
		this.messenger.stop();
	}


	/********************* Start of protocol task handler methods ************************/

	/* Will spawn FetchEpochFinalState to fetch the final state of the
	 * previous epoch if one existed, else will locally create the 
	 * current epoch with an empty initial state.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleStartEpoch(
			StartEpoch<NodeIDType> event,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		StartEpoch<NodeIDType> startEpoch = ((StartEpoch<NodeIDType>) event);
		this.logEvent(event);
		GenericMessagingTask<NodeIDType, ?>[] mtasks = (new GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>>(
				startEpoch.getInitiator(), new AckStartEpoch<NodeIDType>(
						startEpoch.getInitiator(), startEpoch.getServiceName(),
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
					startEpoch.getEpochNumber(), null, // empty state
					startEpoch.getCurEpochGroup());
			return mtasks; // and also send positive ack
		}

		// else request previous epoch state using a threshold protocoltask
		ptasks[0] = new WaitEpochFinalState<NodeIDType>(getMyID(),
				((StartEpoch<NodeIDType>) event), this.appCoordinator);
		return null; // no messaging if asynchronously fetching state
	}

	public GenericMessagingTask<NodeIDType, ?>[] handleStopEpoch(
			StopEpoch<NodeIDType> stopEpoch,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		this.logEvent(stopEpoch);
		if (this.noStateOrAlreadyMovedOn(stopEpoch))
			return this.sendAckStopEpoch(stopEpoch).toArray(); // still send ack
		// else coordinate stop with callback
		this.callbackMap.put(stopEpoch);
		this.appCoordinator.handleIncoming(
				this.appCoordinator.getStopRequest(
						stopEpoch.getServiceName(),
						stopEpoch.getEpochNumber())); 
		return null; // need to wait until callback
	}
	
	private boolean noStateOrAlreadyMovedOn(BasicReconfigurationPacket<?> packet) {
		if (this.appCoordinator.getEpoch(packet.getServiceName()) == null
				|| (this.appCoordinator.getEpoch(packet.getServiceName())
						- packet.getEpochNumber() > 0))
			return true; 
		return false;
	}
	private boolean alreadyMovedOn(BasicReconfigurationPacket<?> packet) {
		if (this.appCoordinator.getEpoch(packet.getServiceName()) != null
				&& this.appCoordinator.getEpoch(packet.getServiceName())
				- packet.getEpochNumber() >= 0) 
			return true; 
		return false;
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
		return mtask.toArray();
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
			mtask = new GenericMessagingTask<NodeIDType, EpochFinalState<NodeIDType>>(
					request.getInitiator(), epochState);
		}
		return (mtask != null ? mtask.toArray() : null);
	}
	public String toString() {
		return "AR" + this.messenger.getMyID();
	}

	/********************* End of protocol task handler methods ************************/

	
	/*********************** Private methods below ************************************/

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
		for(IntegerPacketType reqType : appTypes) {
			if(reqType.getInt()==type) {
				contains = true;
			}
		}
		return contains;
	}

	private void updateDemandStats(InterfaceRequest request, InetAddress sender) {
		String name = request.getServiceName();

		if (request instanceof InterfaceReconfigurableRequest
				&& ((InterfaceReconfigurableRequest) request).isStop())
			return; // no reporting on stop
		if (this.demandProfiler.register(request, sender).shouldReport())
			report(this.demandProfiler.pluckDemandProfile(name));
		else
			report(this.demandProfiler.trim());
	}

	private void report(AbstractDemandProfile demand) {
		try {
			NodeIDType reportee = selectReconfigurator(demand.getName());
			assert (reportee != null);
			/* We don't strictly need the epoch number in demand reports, 
			 * but it is useful for debugging purposes.
			 */
			Integer epoch = this.appCoordinator.getEpoch(demand.getName());
			epoch = (epoch==null ? 0 : epoch);
			GenericMessagingTask<NodeIDType, ?> mtask = new GenericMessagingTask<NodeIDType, Object>(
					reportee, (new DemandReport<NodeIDType>(getMyID(),
							demand.getName(), epoch, demand)).toJSONObject());
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

	private void send(GenericMessagingTask<NodeIDType,?> mtask)  {
		try {
			this.messenger.send(mtask);
		} catch(JSONException je) {
			je.printStackTrace();
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
	private void report(Set<AbstractDemandProfile> demands) {
		if(demands!=null && !demands.isEmpty())
			for(AbstractDemandProfile demand : demands) this.report(demand);
	}
	private GenericMessagingTask<NodeIDType,?> sendAckStopEpoch(StopEpoch<NodeIDType> stopEpoch) {
		// inform reconfigurator
		AckStopEpoch<NodeIDType> ackStopEpoch = new AckStopEpoch<NodeIDType>(
				getMyID(),stopEpoch); 
		GenericMessagingTask<NodeIDType,?> mtask = new 
				GenericMessagingTask<NodeIDType,Object>((stopEpoch.getInitiator()), 
						ackStopEpoch);
		log.log(Level.INFO, MyLogger.FORMAT[5], new Object[]{this, "sending", ackStopEpoch.getType(),
				ackStopEpoch.getServiceName(), ackStopEpoch.getEpochNumber(),
				mtask});
		this.send(mtask);
		return mtask;
	}
	private void logEvent(BasicReconfigurationPacket<NodeIDType> event) {
		log.log(Level.INFO, MyLogger.FORMAT[6], new Object[]{this, "received", event.getType(), event.getServiceName(),
				event.getEpochNumber(), "from", event.getSender(), event});
	}
}
