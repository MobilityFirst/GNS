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
import edu.umass.cs.utils.MyLogger;

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

	private static final long RESTART_PERIOD = 1000;

	private final String key;
	private final StopEpoch<NodeIDType> stopEpoch;
	protected final StartEpoch<NodeIDType> startEpoch; // just convenient to
														// remember this
	protected final RepliconfigurableReconfiguratorDB<NodeIDType> DB;
	private Iterator<NodeIDType> nodeIterator = null;

	private String finalState = null;

	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	public WaitAckStopEpoch(StartEpoch<NodeIDType> startEpoch,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		super(startEpoch.getPrevEpochGroup(), 1); // default is all?
		this.stopEpoch = new StopEpoch<NodeIDType>(DB.getMyID(),
				startEpoch.getPrevGroupName(), startEpoch.getPrevEpochNumber(),
				startEpoch.isMerge());
		this.startEpoch = startEpoch;
		this.nodeIterator = startEpoch.getPrevEpochGroup().iterator();
		this.DB = DB;
		this.key = this.refreshKey();
		this.setPeriod(RESTART_PERIOD);
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if (this.amObviated()) {
			log.log(Level.INFO, MyLogger.FORMAT[1], new Object[] { this,
					"canceling itself as obviated" });
			ProtocolExecutor.cancel(this);
			return null;
		}
		// else
		log.log(Level.INFO, MyLogger.FORMAT[2],
				new Object[] { this.refreshKey() , " resending "
						, this.stopEpoch.getSummary() });
		return start();
	}

	/*
	 * If DB has already moved on, then we might as well commit suicide.
	 */
	protected boolean amObviated() {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(this.stopEpoch.getServiceName());
		if (record == null
				|| (record.getEpoch() - this.startEpoch.getEpochNumber() > 0))
			return true;
		return false;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		if (this.startEpoch.noPrevEpochGroup()) { // creation epoch
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
		return this.startEpoch.hasPrevEpochGroup() ? new GenericMessagingTask<NodeIDType, StopEpoch<NodeIDType>>(
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
		return Reconfigurator.getTaskKey(getClass(), stopEpoch, this.DB
				.getMyID().toString())
				+ ":" +
				// need different key for split/merge operations
				(this.startEpoch.isSplitOrMerge() ? this.startEpoch
						.getServiceName()
						+ ":"
						+ this.startEpoch.getEpochNumber() : "");
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

	@SuppressWarnings("unchecked")
	@Override
	public boolean handleEvent(ProtocolEvent<PacketType, String> event) {
		assert (event.getType().equals(types[0]));
		// asserted above
		AckStopEpoch<NodeIDType> ackStopEpoch = (AckStopEpoch<NodeIDType>) event;
		log.info(this
				+ " received "
				+ ackStopEpoch.getSummary()
				+ (this.stopEpoch.shouldGetFinalState() ? ":"
						+ ackStopEpoch.getFinalState() : ""));
		// finalState can not be null
		if (this.stopEpoch.shouldGetFinalState())
			return (this.finalState = ackStopEpoch.getFinalState()) != null;
		return true;
	}

	// Send startEpoch when stopEpoch is committed
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		log.log(Level.INFO, MyLogger.FORMAT[2], new Object[] { this,
				"starting epoch", this.startEpoch.getSummary() });
		// no next epoch group means delete record
		if (this.startEpoch.noCurEpochGroup() && !this.startEpoch.isMerge()) {
			ptasks[0] = new WaitAckDropEpoch<NodeIDType>(this.startEpoch,
					this.DB);
			return this.getDeleteConfirmation();
		} else if (this.startEpoch.isMerge()) {
			ptasks[0] = new WaitCoordinatedCommit<NodeIDType>(
					new RCRecordRequest<NodeIDType>(this.DB.getMyID(),
					// just to update initialState with the received state
							new StartEpoch<NodeIDType>(this.startEpoch,
									this.finalState),
							RCRecordRequest.RequestTypes.RECONFIGURATION_MERGE),
					this.DB);
		} else
			// else start next epoch group
			ptasks[0] = new WaitAckStartEpoch<NodeIDType>(this.startEpoch,
					this.DB);

		return null; // ptasks[0].start() will actually send the startEpoch
	}

	private GenericMessagingTask<NodeIDType, ?>[] getDeleteConfirmation() {
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
				this.DB.getMyID(), this.startEpoch,
				RCRecordRequest.RequestTypes.DELETE_COMPLETE);
		return (new GenericMessagingTask<NodeIDType, Object>(this.DB.getMyID(),
				rcRecReq)).toArray();
	}

	public String toString() {
		return this.getKey();
	}

	public static void main(String[] args) {
	}
}
