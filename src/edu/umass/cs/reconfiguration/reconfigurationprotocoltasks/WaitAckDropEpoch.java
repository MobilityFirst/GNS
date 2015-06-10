package edu.umass.cs.reconfiguration.reconfigurationprotocoltasks;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DropEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 * @param <NodeIDType> 
 */
/*
 * This protocol task is initiated at a reconfigurator to await a majority of
 * acknowledgments from active replicas for StopEpoch messages.
 */
public class WaitAckDropEpoch<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	/*
	 * We use different restart times for service names and RC group names as RC
	 * group names may have much more variance in checkpoint transfer times.
	 * Setting these values too low or too high will not impact safety. However,
	 * too low values may result in inefficient, remote checkpoint transfers
	 * while too high values may result in a large number of pending tasks
	 * consuming memory.
	 */
	private static final long RESTART_PERIOD_SERVICE_NAMES = 10000;
	private static final long RESTART_PERIOD_RC_GROUP_NAMES = 60000;
	private static final int MAX_RESTARTS = 5;

	private final NodeIDType myID;
	private final DropEpochFinalState<NodeIDType> dropEpoch;
	private final StartEpoch<NodeIDType> startEpoch;
	private int numRestarts = 0;

	private final String key;

	private static final Logger log = Reconfigurator.getLogger();

	/**
	 * @param startEpoch
	 * @param DB
	 */
	public WaitAckDropEpoch(StartEpoch<NodeIDType> startEpoch,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		super(startEpoch.getPrevEpochGroup()); // default is all?
		this.dropEpoch = new DropEpochFinalState<NodeIDType>(DB.getMyID(),
				startEpoch.getServiceName(), startEpoch.getEpochNumber() - 1,
				false);
		this.startEpoch = startEpoch;
		this.myID = DB.getMyID();
		this.key = this.refreshKey();
		this.setPeriod(DB.isRCGroupName(startEpoch.getServiceName()) ? RESTART_PERIOD_RC_GROUP_NAMES
				: RESTART_PERIOD_SERVICE_NAMES);
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		// send DropEpoch to all new actives and await acks from all?
		log.log(Level.INFO,
				MyLogger.FORMAT[2],
				new Object[] {
						this.refreshKey(),
						" (re-)sending[" + this.numRestarts + " of "
								+ MAX_RESTARTS + "] ",
						this.dropEpoch.getSummary() });
		if (this.numRestarts++ < MAX_RESTARTS)
			return (new GenericMessagingTask<NodeIDType, DropEpochFinalState<NodeIDType>>(
					this.startEpoch.getPrevEpochGroup().toArray(),
					this.dropEpoch)).toArray();
		// else
		ProtocolExecutor.cancel(this);
		return null;
	}

	// We skip the first period before sending out drop epochs.
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		return null;
	}

	@Override
	public String refreshKey() {
		return Reconfigurator.getTaskKey(getClass(), dropEpoch,
				this.myID.toString());
	}

	/**
	 * Packet types handled.
	 */
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
		log.log(Level.INFO,
				MyLogger.FORMAT[3],
				new Object[] { this.refreshKey(), "received",
						dropEpoch.getSummary() });
		return true;
	}

	// Note: Nothing to do upon threshold event other than default cancel
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(Level.INFO, MyLogger.FORMAT[3],
				new Object[] { this.refreshKey(), "received all ACKs",
						dropEpoch.getSummary() });
		return null;
	}

}
