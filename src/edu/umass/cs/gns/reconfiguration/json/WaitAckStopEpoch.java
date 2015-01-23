package edu.umass.cs.gns.reconfiguration.json;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.gns.reconfiguration.Reconfigurator;
import edu.umass.cs.gns.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.AckStopEpoch;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StopEpoch;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.gns.util.MyLogger;

/**
 * @author V. Arun
 */
/*
 * This protocol task is initiated at a reconfigurator to await a majority of
 * acknowledgments from active replicas for StopEpoch messages.
 */
public class WaitAckStopEpoch<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private String key = null;
	private final StopEpoch<NodeIDType> stopEpoch;
	protected final StartEpoch<NodeIDType> startEpoch; // just convenient to
														// remember this
	protected final RepliconfigurableReconfiguratorDB<NodeIDType> DB;
	private Iterator<NodeIDType> nodeIterator = null;

	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	public WaitAckStopEpoch(StartEpoch<NodeIDType> startEpoch,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		super(startEpoch.getPrevEpochGroup(), 1); // default is all?
		this.stopEpoch = new StopEpoch<NodeIDType>(startEpoch.getSender(),
				startEpoch.getServiceName(), startEpoch.getEpochNumber() - 1);
		this.startEpoch = startEpoch;
		this.nodeIterator = startEpoch.getPrevEpochGroup().iterator();
		this.DB = DB;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if (this.amObviated())
			ProtocolExecutor.cancel(this);
		;
		return start();
	}

	/*
	 * If DB has already moved on, then we might as well commit suicide.
	 */
	protected boolean amObviated() {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(this.stopEpoch.getServiceName());
		if (record.getEpoch() - this.startEpoch.getEpochNumber() > 0)
			return true;
		return false;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		if (this.startEpoch.isInitEpoch()) {
			// spoof AckStopEpoch to self
			return new GenericMessagingTask<NodeIDType, AckStopEpoch<NodeIDType>>(
					this.DB.getMyID(), new AckStopEpoch<NodeIDType>(
							this.startEpoch.getInitiator(),
							new StopEpoch<NodeIDType>(
									this.startEpoch.getInitiator(),
									this.startEpoch.getServiceName(),
									this.startEpoch.getEpochNumber() - 1)))
					.toArray();
		}
		// else send stopEpoch sequentially to old actives and await a response
		return (this.startEpoch.getPrevEpochGroup() != null && !this.startEpoch
				.getPrevEpochGroup().isEmpty()) ? new GenericMessagingTask<NodeIDType, StopEpoch<NodeIDType>>(
				getNextNode(), this.stopEpoch).toArray() : null;
	}

	private NodeIDType getNextNode() {
		if (!this.nodeIterator.hasNext())
			this.nodeIterator = startEpoch.getPrevEpochGroup().iterator();
		return (this.nodeIterator.next());
	}

	/*
	 * Note: Trying to start this task when one is already running will cause
	 * the executor to get stuck.
	 */
	@Override
	public String refreshKey() {
		return (this.key = this.getClass().getSimpleName() + this.DB.getMyID()
				+ ":" + this.startEpoch.getServiceName() + ":"
				+ (this.startEpoch.getEpochNumber() - 1));
		// return (this.key =
		// Util.refreshKey(this.stopEpoch.getSender().toString()));
	}

	public static final ReconfigurationPacket.PacketType[] types = { ReconfigurationPacket.PacketType.ACK_STOP_EPOCH };

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
		assert (event.getType().equals(types[0]));
		return true;
	}

	// Send startEpoch when stopEpoch is committed
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		log.log(Level.INFO, MyLogger.FORMAT[3], new Object[] {
				getClass().getSimpleName(), this.stopEpoch.getInitiator(),
				" starting epoch ", this.startEpoch.getSummary() });
		// no next epoch group means delete record
		if (this.startEpoch.getCurEpochGroup() == null || this.startEpoch.getCurEpochGroup().isEmpty()) {
			ptasks[0] = new WaitAckDropEpoch<NodeIDType>(this.startEpoch,
					this.DB);
			return this.getDeleteConfirmation();
		}
		// else start next epoch group
		ptasks[0] = new WaitAckStartEpoch<NodeIDType>(this.startEpoch, this.DB);
		return null; // ptasks[0].start() will actually send the startEpoch
	}

	private GenericMessagingTask<NodeIDType, ?>[] getDeleteConfirmation() {
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
				this.DB.getMyID(), this.startEpoch,
				RCRecordRequest.RequestTypes.DELETE_RECORD_COMPLETE);
		return (new GenericMessagingTask<NodeIDType, Object>(this.DB.getMyID(),
				rcRecReq)).toArray();
	}

	public static void main(String[] args) {
	}
}
