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
 * This protocol task is initiated at a reconfigurator to await a majority of
 * acknowledgments from active replicas for StopEpoch messages.
 */
public class WaitAckDropEpoch<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private static final long RESTART_PERIOD = 8000;

	private final NodeIDType myID;
	private final DropEpochFinalState<NodeIDType> dropEpoch;
	private final StartEpoch<NodeIDType> startEpoch;

	private final String key;

	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	public WaitAckDropEpoch(StartEpoch<NodeIDType> startEpoch,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		super(startEpoch.getPrevEpochGroup()); // default is all?
		this.dropEpoch = new DropEpochFinalState<NodeIDType>(DB.getMyID(),
				startEpoch.getServiceName(), startEpoch.getEpochNumber() - 1,
				false);
		this.startEpoch = startEpoch;
		this.myID = DB.getMyID();
		this.key = this.refreshKey();
		this.setPeriod(RESTART_PERIOD);
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		// send StartEpoch to all new actives and await a majority
		GenericMessagingTask<NodeIDType, ?> mtask1 = new GenericMessagingTask<NodeIDType, DropEpochFinalState<NodeIDType>>(
				this.startEpoch.getPrevEpochGroup().toArray(), this.dropEpoch);
		return mtask1.toArray();
	}

	// We skip the first period before sending out drop epochs.
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		return null;
	}

	@Override
	public String refreshKey() {
		return (this.getClass().getSimpleName() + myID + ":"
				+ this.dropEpoch.getServiceName() + ":" + this.dropEpoch
					.getEpochNumber());
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
		assert (event.getType()
				.equals(ReconfigurationPacket.PacketType.ACK_DROP_EPOCH_FINAL_STATE));
		return true;
	}

	// Note: Nothing to do upon threshold event other than default cancel
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(Level.INFO, MyLogger.FORMAT[3], new Object[] { this,
				"received ACK", dropEpoch.getSummary() });
		return null;
	}

	public static void main(String[] args) {
	}
}
