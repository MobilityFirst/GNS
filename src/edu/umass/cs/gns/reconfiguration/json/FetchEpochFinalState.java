package edu.umass.cs.gns.reconfiguration.json;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.gns.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.AckStartEpoch;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.EpochFinalState;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RequestEpochFinalState;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
public class FetchEpochFinalState<NodeIDType> extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private final StartEpoch<NodeIDType> startEpoch; // message that started the epoch change
	private final AbstractReplicaCoordinator<NodeIDType> appCoordinator;
	private final RequestEpochFinalState<NodeIDType> reqState;
	private Iterator<NodeIDType> prevGroupIterator;
	private boolean first = true;

	private String key = null;
	
	public FetchEpochFinalState(NodeIDType myID, StartEpoch<NodeIDType> startEpoch, AbstractReplicaCoordinator<NodeIDType> appCoordinator) {
		super(startEpoch.getPrevEpochGroup(), 1);
		this.startEpoch = startEpoch;
		this.appCoordinator = appCoordinator;
		this.prevGroupIterator = this.startEpoch.getPrevEpochGroup().iterator();
		this.reqState = new RequestEpochFinalState<NodeIDType>(myID, 
				startEpoch.getServiceName(), (startEpoch.getEpochNumber()-1));;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if(!this.prevGroupIterator.hasNext()) this.prevGroupIterator = this.startEpoch.getPrevEpochGroup().iterator();
		return start();
	}

	@Override
	public GenericMessagingTask<NodeIDType,?>[] start() {
		if(!this.prevGroupIterator.hasNext()) return null;
		// Try myself first if I am in both old and new groups
		NodeIDType target = this.positionIterator(); 
		GenericMessagingTask<NodeIDType,?> mtask = new GenericMessagingTask<NodeIDType,Object>(target, 
				this.reqState);
		return mtask.toArray();
	}
	
	private NodeIDType positionIterator() {
		NodeIDType myID = this.reqState.getInitiator();
		if(!this.first || !this.startEpoch.getPrevEpochGroup().contains(myID)) 
			return this.prevGroupIterator.next();
		first = false;
		while(this.prevGroupIterator.hasNext() && this.prevGroupIterator.next()!=myID);
		return myID;
	}

	@Override
	public String refreshKey() {
		return (this.key = Util.refreshKey(this.reqState.getInitiator().toString()));
	}

	protected static final ReconfigurationPacket.PacketType[] types = {
		ReconfigurationPacket.PacketType.EPOCH_FINAL_STATE
	};
	
	@Override
	public Set<PacketType> getEventTypes() {
		return new HashSet<ReconfigurationPacket.PacketType>(Arrays.asList(types));
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public boolean handleEvent(ProtocolEvent<PacketType, String> event) {
		ReconfigurationPacket.PacketType type = event.getType();
		if(type==null) return false;
		boolean handled = false;
		switch(type) {
		case EPOCH_FINAL_STATE: 
			EpochFinalState<NodeIDType> state = (EpochFinalState<NodeIDType>)event;
			handled = checkEpochFinalState(event);
			this.appCoordinator.createReplicaGroup(state.getServiceName(), state.getEpochNumber()+1,  
				state.getState(), this.startEpoch.getCurEpochGroup());		
			System.out.println("App-" + this.appCoordinator.getMyID() + 
				" received " + event.getType() + " " + event);
		}
		return handled; 
	}
	private boolean checkEpochFinalState(ProtocolEvent<PacketType, String> event) {
		// FIXME: What is there to check here other than the type?
		return true;
	}
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		System.out.println("App-"+this.appCoordinator.getMyID() + " sending ack start epoch "
			+ " to RC" + this.startEpoch.getInitiator());
		AckStartEpoch<NodeIDType> ackStartEpoch = new AckStartEpoch<NodeIDType>(this.startEpoch.getInitiator(), 
			startEpoch.getServiceName(), startEpoch.getEpochNumber(), this.appCoordinator.getMyID());
		GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>> mtask = new GenericMessagingTask<NodeIDType, 
				AckStartEpoch<NodeIDType>>(this.startEpoch.getInitiator(), ackStartEpoch);
		ackStartEpoch.setKey(this.startEpoch.getKey());
		System.out.println("App-"+ackStartEpoch.getSender() + " sending " + ackStartEpoch.getType() + " to RC" + this.startEpoch.getInitiator());
		return mtask.toArray();
	}
	
	public static void main(String[] args) {
		
	}
}
