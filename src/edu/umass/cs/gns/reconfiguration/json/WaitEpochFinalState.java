package edu.umass.cs.gns.reconfiguration.json;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.gns.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.gns.reconfiguration.Reconfigurator;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.AckStartEpoch;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.EpochFinalState;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RequestEpochFinalState;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.gns.util.MyLogger;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
public class WaitEpochFinalState<NodeIDType> extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private final StartEpoch<NodeIDType> startEpoch; // message that started the epoch change
	private final AbstractReplicaCoordinator<NodeIDType> appCoordinator;
	private final RequestEpochFinalState<NodeIDType> reqState;
	private Iterator<NodeIDType> prevGroupIterator;
	private boolean first = true;

	private String key = null;
	
	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());
	
	public WaitEpochFinalState(NodeIDType myID, StartEpoch<NodeIDType> startEpoch, AbstractReplicaCoordinator<NodeIDType> appCoordinator) {
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
			@SuppressWarnings("unchecked")
			EpochFinalState<NodeIDType> state = (EpochFinalState<NodeIDType>)event;
			handled = checkEpochFinalState(event);
			this.appCoordinator.createReplicaGroup(state.getServiceName(), state.getEpochNumber()+1,  
				state.getState(), this.startEpoch.getCurEpochGroup());		
			log.log(Level.INFO, MyLogger.FORMAT[3], new Object[]{
					this.getClass().getSimpleName(), this.appCoordinator.getMyID(), "received",
					state.getSummary()});
			default:
				break;
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
		AckStartEpoch<NodeIDType> ackStartEpoch = new AckStartEpoch<NodeIDType>(this.startEpoch.getInitiator(), 
			startEpoch.getServiceName(), startEpoch.getEpochNumber(), this.appCoordinator.getMyID());
		GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>> mtask = new GenericMessagingTask<NodeIDType, 
				AckStartEpoch<NodeIDType>>(this.startEpoch.getInitiator(), ackStartEpoch);
		ackStartEpoch.setKey(this.startEpoch.getKey());
		log.log(Level.INFO, MyLogger.FORMAT[5], new Object[]{this.getClass().getSimpleName() , ackStartEpoch.getSender() , "sending"
				, ackStartEpoch.getSummary(), "to RC"
				, this.startEpoch.getInitiator()});
		return mtask.toArray();
	}
	
	public static void main(String[] args) {
		
	}
}
