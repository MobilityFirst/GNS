package edu.umass.cs.gns.reconfiguration.json;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.gns.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StopEpoch;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
/*
 * This protocol task is initiated at a reconfigurator to await
 * a majority of acknowledgments from active replicas for 
 * StopEpoch messages.
 */
public class WaitAckStopEpoch<NodeIDType> extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private String key = null;
	private final StopEpoch<NodeIDType> stopEpoch;
	private final StartEpoch<NodeIDType> startEpoch; // just convenient to remember this
	private Iterator<NodeIDType> nodeIterator = null;
	private final AbstractReconfiguratorDB<NodeIDType> DB;
	
	public WaitAckStopEpoch(StartEpoch<NodeIDType> startEpoch, AbstractReconfiguratorDB<NodeIDType> DB) {
		super(startEpoch.getPrevEpochGroup(), 1); // default is all?
		this.stopEpoch = new StopEpoch<NodeIDType>(startEpoch.getSender(), startEpoch.getServiceName(), startEpoch.getEpochNumber()-1);
		this.startEpoch = startEpoch;
		this.nodeIterator = startEpoch.getPrevEpochGroup().iterator();
		this.DB = DB;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		return start();
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		//this.DB.setState(this.startEpoch.getServiceName(), this.startEpoch.getEpochNumber(), ReconfigurationRecord.RCStates.WAIT_ACK_STOP);
		if(!this.nodeIterator.hasNext()) this.nodeIterator = startEpoch.getPrevEpochGroup().iterator();
		HashSet<NodeIDType> nodeSet = new HashSet<NodeIDType>();
		nodeSet.add(this.nodeIterator.next());
		// send stopEpoch sequentially to old actives and await a response from any 
		GenericMessagingTask<NodeIDType, ?> mtask = new GenericMessagingTask<NodeIDType, 
				StopEpoch<NodeIDType>>(nodeSet.toArray(), this.stopEpoch);
		return mtask.toArray();
	}

	@Override
	public String refreshKey() {
		return (this.key = Util.refreshKey(this.stopEpoch.getSender().toString()));
	}

	public static final ReconfigurationPacket.PacketType[] types = {
		ReconfigurationPacket.PacketType.ACK_STOP_EPOCH
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
		// FIXME: Is there anything to check here?
		return true;
	}
	
	// Send startEpoch when stopEpoch is committed
	@Override
	public GenericMessagingTask<NodeIDType,?>[] handleThresholdEvent(ProtocolTask<NodeIDType,PacketType,String>[] ptasks) {
		System.out.println("RC starting epoch "+ this.startEpoch.getServiceName()+":"+this.startEpoch.getEpochNumber());
		this.DB.setState(this.startEpoch.getServiceName(), this.startEpoch.getEpochNumber(), ReconfigurationRecord.RCStates.WAIT_ACK_START);
		ptasks[0] = new WaitAckStartEpoch<NodeIDType>(this.startEpoch, this.DB);
		return null; // ptasks[0].start() will actually send the startEpoch message
	}
	
	public static void main(String[] args) {
	}
}
