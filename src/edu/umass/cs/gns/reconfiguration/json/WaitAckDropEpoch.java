package edu.umass.cs.gns.reconfiguration.json;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.gns.reconfiguration.Reconfigurator;
import edu.umass.cs.gns.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DropEpochFinalState;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.gns.util.MyLogger;

/**
 * @author V. Arun
 */
/*
 * This protocol task is initiated at a reconfigurator to await a majority of acknowledgments from
 * active replicas for StopEpoch messages.
 */
public class WaitAckDropEpoch<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private final DropEpochFinalState<NodeIDType> dropEpoch;
	private final Set<NodeIDType> group; // just convenient to remember this

	private String key = null;

	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	public WaitAckDropEpoch(StartEpoch<NodeIDType> startEpoch, RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		super(startEpoch.getPrevEpochGroup()); // default is all?
		this.dropEpoch = new DropEpochFinalState<NodeIDType>(
				startEpoch.getSender(), startEpoch.getServiceName(),
				startEpoch.getEpochNumber()-1, false);
		this.group = startEpoch.getPrevEpochGroup();
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		return start();
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		// send StartEpoch to all new actives and await a majority
		GenericMessagingTask<NodeIDType, ?> mtask = new GenericMessagingTask<NodeIDType, DropEpochFinalState<NodeIDType>>(
				this.group.toArray(), this.dropEpoch);
		return mtask.toArray();
	}

	@Override
	public String refreshKey() {
		return (this.key = this.getClass().getSimpleName()
				+ this.dropEpoch.getInitiator() + ":"
				+ this.dropEpoch.getServiceName() + ":"
				+ this.dropEpoch.getEpochNumber());
		//return (this.key = Util.refreshKey(this.dropEpoch.getSender().toString()));
	}

	public static final ReconfigurationPacket.PacketType[] types = { ReconfigurationPacket.PacketType.ACK_DROP_EPOCH_FINAL_STATE };

	@Override
	public Set<PacketType> getEventTypes() {
		return new HashSet<ReconfigurationPacket.PacketType>(
				Arrays.asList(types));
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public boolean handleEvent(ProtocolEvent<PacketType, String> event) {
		assert(event.getType().equals(ReconfigurationPacket.PacketType.ACK_DROP_EPOCH_FINAL_STATE));
		return true;
	}

	// Note: Nothing to do upon threshold event other than default cancel
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(Level.INFO, MyLogger.FORMAT[3], new Object[]{getClass().getSimpleName(), dropEpoch.getInitiator(), "received ACK",
				dropEpoch.getSummary()});
		return null;
	}
	
	public static void main(String[] args) {
	}
}
