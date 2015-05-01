package edu.umass.cs.gns.reconfiguration.deprecated;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DropEpochFinalState;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.gns.util.Util;

/**
 * @author V. Arun
 */
/*
 * FIXME: This task needs to stop epoch followed by drop epoch (without an
 * intervening start epoch as in a group change). Currently, it is just a copy
 * of WaitAckDropEpoch.
 * 
 * This task also needs to remove the name from the reconfigurator group.
 */
public class WaitAckDeleteName<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private String key = null;
	private final DropEpochFinalState<NodeIDType> deleteName;
	private final Set<NodeIDType> group; // just convenient to remember this

	public WaitAckDeleteName(NodeIDType sender, String name,
			Set<NodeIDType> actives) {
		super(actives); // default is all?
		this.deleteName = new DropEpochFinalState<NodeIDType>(sender, name, 0,
				true);
		this.group = actives;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		return start();
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		// send StartEpoch to all new actives and await a majority
		GenericMessagingTask<NodeIDType, ?> mtask = new GenericMessagingTask<NodeIDType, DropEpochFinalState<NodeIDType>>(
				this.group.toArray(), this.deleteName);
		return mtask.toArray();
	}

	@Override
	public String refreshKey() {
		return (this.key = Util.refreshKey(this.deleteName.getSender()
				.toString()));
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
		// FIXME: Is there anything to check here?
		return true;
	}

	public static void main(String[] args) {
	}
}
