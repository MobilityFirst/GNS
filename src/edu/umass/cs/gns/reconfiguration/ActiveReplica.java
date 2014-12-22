package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONPacket;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.ActiveReplicaProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.FetchEpochFinalState;
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
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.AggregateDemandProfiler;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.CallbackMap;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentHashing;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.DemandProfile;

/**
@author V. Arun
 */
public class ActiveReplica<NodeIDType> implements  InterfaceReconfiguratorCallback, 
InterfacePacketDemultiplexer {
	private final AbstractReplicaCoordinator<NodeIDType> appCoordinator;
	private final InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig;
	private final ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String> protocolExecutor;
	private final ActiveReplicaProtocolTask<NodeIDType> protocolTask;
	private final JSONMessenger<NodeIDType> messenger;

	private final AggregateDemandProfiler demandProfiler = new AggregateDemandProfiler();
	private final ConsistentHashing<NodeIDType> CH;
	private final CallbackMap<NodeIDType> callbackMap = new CallbackMap<NodeIDType>();
	
	public ActiveReplica(AbstractReplicaCoordinator<NodeIDType> appC, 
			InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig, JSONMessenger<NodeIDType> messenger) {
		this.appCoordinator = appC.setCallback((InterfaceReconfiguratorCallback)this);
		this.nodeConfig = nodeConfig;
		this.messenger = messenger;
		this.protocolExecutor = new ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String>(messenger);
		this.protocolTask = new ActiveReplicaProtocolTask<NodeIDType>(getMyID(), this);
		this.protocolExecutor.register(this.protocolTask.getDefaultTypes(), this.protocolTask);
		this.CH = new ConsistentHashing<NodeIDType>(this.nodeConfig.getReconfigurators().toArray());
		this.appCoordinator.setMessenger(this.messenger);
	}

	@Override
	public boolean handleJSONObject(JSONObject jsonObject) {
		BasicReconfigurationPacket<NodeIDType> rcPacket = null;
		try {
			// try handling as reconfiguration packet through protocol task 
			if((rcPacket = this.protocolTask.getReconfigurationPacket(jsonObject))!=null) {
				this.protocolExecutor.handleEvent(rcPacket);
			}
			// else check if app request
			else if(isAppRequest(jsonObject)) { 
				InterfaceRequest request = this.appCoordinator.getRequest(jsonObject.toString());
				this.appCoordinator.handleIncoming(request); // for app
				updateStats(request, JSONPacket.getSenderAddress(jsonObject)); // for reconfiguration
			}
		} catch(RequestParseException rpe) {
			rpe.printStackTrace();
		} catch(JSONException je) {
			je.printStackTrace();
		}

		return false; // neither reconfiguration packet nor app request
	}
	
	@Override
	public void executed(InterfaceReconfigurableRequest request, boolean handled) {
		StopEpoch<NodeIDType> stopEpoch = (StopEpoch<NodeIDType>)(
				this.callbackMap.get(request.getServiceName()));
		if(stopEpoch==null) return;
		/* Note: stopEpoch is removed irrespective of handled below.
		 * If needed, the reconfigurator will resend the stopEpoch.
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

	/********************* Start of protocol task handler methods ************************/

	@SuppressWarnings("unchecked")
	public GenericMessagingTask<NodeIDType,?>[] handleStartEpoch(ProtocolEvent<ReconfigurationPacket.PacketType, String> event,
		ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		StartEpoch<NodeIDType> startEpoch = ((StartEpoch<NodeIDType>)event);
		System.out.println("AR"+getMyID() + " received " + event.getType() +" "+ event.getMessage());
		if(this.appCoordinator.getEpoch(startEpoch.getServiceName())!=null && 
				this.appCoordinator.getEpoch(startEpoch.getServiceName())  - startEpoch.getEpochNumber() >=0) {
			return null; // app has no state for name or has already moved on to a higher epoch
		}
		if(startEpoch.getPrevEpochGroup()==null || startEpoch.getPrevEpochGroup().isEmpty()) {
			// create replica group with empty state
			this.appCoordinator.createReplicaGroup(startEpoch.getServiceName(), startEpoch.getEpochNumber(), null, 
				startEpoch.getCurEpochGroup());
			AckStartEpoch<NodeIDType> ackStartEpoch = new AckStartEpoch<NodeIDType>(startEpoch.getInitiator(), 
					startEpoch.getServiceName(), startEpoch.getEpochNumber(), getMyID());
			return (new GenericMessagingTask<NodeIDType,AckStartEpoch<NodeIDType>>(
					startEpoch.getInitiator(),ackStartEpoch)).toArray();
		} 
		
		// request previous epoch state using a threshold protocoltask
		ptasks[0] = new FetchEpochFinalState<NodeIDType>(getMyID(), ((StartEpoch<NodeIDType>)event), this.appCoordinator);
		return null;
	}

	@SuppressWarnings("unchecked")
	public GenericMessagingTask<NodeIDType,?>[] handleStopEpoch(ProtocolEvent<ReconfigurationPacket.PacketType, String> event,
		ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		StopEpoch<NodeIDType> stopEpoch = (StopEpoch<NodeIDType>)event;
		System.out.println("AR"+getMyID() + " received " + event.getType() + " " + event);
		if(this.appCoordinator.getEpoch(stopEpoch.getServiceName())==null || 
				(this.appCoordinator.getEpoch(stopEpoch.getServiceName()) - stopEpoch.getEpochNumber() > 0)) {
			return null; // app has already moved on
		}
		try {
			this.callbackMap.put(stopEpoch);
			this.appCoordinator.handleIncoming(this.appCoordinator.getStopRequest(
				stopEpoch.getServiceName(), stopEpoch.getEpochNumber()));
		} catch(RuntimeException re) {
			re.printStackTrace();
		}
		return null; // need to wait until callback 
	}

	@SuppressWarnings("unchecked")
	public GenericMessagingTask<NodeIDType,?>[] handleDropEpochFinalState(
		ProtocolEvent<ReconfigurationPacket.PacketType, String> event,
		ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		System.out.println("AR"+getMyID() + " received " + event.getType() + " " + event);
		DropEpochFinalState<NodeIDType> dropEpoch = (DropEpochFinalState<NodeIDType>)event;
		this.appCoordinator.deleteReplicaGroup((dropEpoch).getServiceName());
		AckDropEpochFinalState<NodeIDType> ackDrop = new AckDropEpochFinalState<NodeIDType>(getMyID(), 
				dropEpoch);
		GenericMessagingTask<NodeIDType,AckDropEpochFinalState<NodeIDType>> mtask = 
				new GenericMessagingTask<NodeIDType,AckDropEpochFinalState<NodeIDType>>(dropEpoch.getInitiator(), 
						ackDrop);
		return mtask.toArray();
	}
	@SuppressWarnings("unchecked")
	public GenericMessagingTask<NodeIDType,?>[] handleRequestEpochFinalState(
		ProtocolEvent<ReconfigurationPacket.PacketType, String> event,
		ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		//this.appCoordinator.deleteReplicaGroup(((DropEpochFinalState<NodeIDType>)event).getServiceName());
		RequestEpochFinalState<NodeIDType> request = (RequestEpochFinalState<NodeIDType>)event;
		System.out.println("AR"+getMyID()+" received " + event.getType() + " " + event);
		EpochFinalState<NodeIDType> epochState = new EpochFinalState<NodeIDType>(request.getInitiator(), 
				request.getServiceName(), request.getEpochNumber(), this.appCoordinator.getFinalState(
					request.getServiceName(), request.getEpochNumber()));
		GenericMessagingTask<NodeIDType,EpochFinalState<NodeIDType>> mtask = null;

		if(epochState.getState()!=null) {
			mtask = new GenericMessagingTask<NodeIDType,EpochFinalState<NodeIDType>>(request.getInitiator(), epochState);
		}
		return (mtask!=null ? mtask.toArray() : null);
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

	private void updateStats(InterfaceRequest request, InetAddress sender) {
		String name = request.getServiceName();

		if(request instanceof InterfaceReconfigurableRequest && ((InterfaceReconfigurableRequest)request).isStop()) return; // no reporting on stop
		if(this.demandProfiler.register(request, sender).shouldReport()) {
			report(this.demandProfiler.pluckDemandProfile(name));
		}		
		else report(this.demandProfiler.trim());
	}
	private void report(DemandProfile demand) {
		try {
			NodeIDType reportee = selectReconfigurator(CH.getReplicatedServers(demand.getName()));
			if(reportee==null) return;
			// else
			GenericMessagingTask<NodeIDType,?> mtask = 
					new GenericMessagingTask<NodeIDType,Object>(reportee,
							(new DemandReport<NodeIDType>(getMyID(), 
									demand.getName(), 0, demand)).toJSONObject());
			this.send(mtask);
		} catch(JSONException je) {
			je.printStackTrace();
		}
	}
	private NodeIDType selectReconfigurator(Set<NodeIDType> reconfigurators) {
		Iterator<NodeIDType> iterator = reconfigurators.iterator();
		return iterator.next();
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
	private void report(Set<DemandProfile> demands) {
		if(demands!=null && !demands.isEmpty())
			for(DemandProfile demand : demands) this.report(demand);
	}
	private void sendAckStopEpoch(StopEpoch<NodeIDType> stopEpoch) {
		// inform reconfigurator
		AckStopEpoch<NodeIDType> ackStopEpoch = new AckStopEpoch<NodeIDType>(
				getMyID(),stopEpoch); 
		GenericMessagingTask<NodeIDType,?> mtask = new 
				GenericMessagingTask<NodeIDType,Object>((stopEpoch.getInitiator()), 
						ackStopEpoch);
		System.out.println("AR"+getMyID()+" sending " + ackStopEpoch.getType() +" " + mtask);
		this.send(mtask);
	}
}
