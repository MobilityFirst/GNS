package edu.umass.cs.gns.reconfiguration.json;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.gns.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
/*
 * This protocol task is initiated at a reconfigurator to await
 * a majority of acknowledgments for StartEpoch messages from 
 * active replicas.
 */
public class WaitAckStartEpoch<NodeIDType> extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private String key = null;
	private final StartEpoch<NodeIDType> startEpoch;
	private final AbstractReconfiguratorDB<NodeIDType> DB;
	private final CreateServiceName create;

	public WaitAckStartEpoch(StartEpoch<NodeIDType> startEpoch, AbstractReconfiguratorDB<NodeIDType> DB) {
		super(startEpoch.getCurEpochGroup(), startEpoch.getCurEpochGroup().size()/2+1);
		this.startEpoch = startEpoch;
		this.DB = DB;
		this.create=null;
	}
	public WaitAckStartEpoch(StartEpoch<NodeIDType> startEpoch, AbstractReconfiguratorDB<NodeIDType> DB, CreateServiceName create) {
		super(startEpoch.getCurEpochGroup(), startEpoch.getCurEpochGroup().size()/2+1);
		this.startEpoch = startEpoch;
		this.DB = DB;
		this.create=create;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		return start();
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		// send StartEpoch to all new actives and await a majority 
		GenericMessagingTask<NodeIDType, StartEpoch<NodeIDType>> mtask = new GenericMessagingTask<NodeIDType, 
				StartEpoch<NodeIDType>>(this.startEpoch.getCurEpochGroup().toArray(), this.startEpoch);
		return mtask.toArray();
	}

	@Override
	public String refreshKey() {
		return (this.key = Util.refreshKey(this.startEpoch.getSender().toString()));
	}

	public static final ReconfigurationPacket.PacketType[] types = {
		ReconfigurationPacket.PacketType.ACK_START_EPOCH,
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
	
	// Send dropEpoch when startEpoch is acked by majority
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public GenericMessagingTask<NodeIDType,?>[] handleThresholdEvent(ProtocolTask<NodeIDType, 
		ReconfigurationPacket.PacketType, String>[] ptasks) {

		System.out.println("RC"+startEpoch.getInitiator()+" received MAJORITY ackStartEpoch for "+
				startEpoch.getServiceName()+":"+startEpoch.getEpochNumber()+ (this.create!=null ? "; sending ack to client " + 
				this.create.getSender() : ""));
		this.DB.setState(this.startEpoch.getServiceName(), this.startEpoch.getEpochNumber(), ReconfigurationRecord.RCStates.READY);
		GenericMessagingTask<NodeIDType,?>[] mtasks=null;
		if(this.startEpoch.getPrevEpochGroup()!=null && !this.startEpoch.getPrevEpochGroup().isEmpty()) {
			ptasks[0] = new WaitAckDropEpoch<NodeIDType>(this.startEpoch);
		} 
		else {
			// inform client of name creation
			mtasks = (new GenericMessagingTask(this.create.getSender(), this.create)).toArray();
		}
		return mtasks;
	}
}
