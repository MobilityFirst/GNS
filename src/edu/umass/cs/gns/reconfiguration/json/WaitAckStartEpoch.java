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
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.gns.util.MyLogger;

/**
 * @author V. Arun
 */
/*
 * This protocol task is initiated at a reconfigurator to await a majority of acknowledgments for
 * StartEpoch messages from active replicas.
 */
public class WaitAckStartEpoch<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private final StartEpoch<NodeIDType> startEpoch;
	private final RepliconfigurableReconfiguratorDB<NodeIDType> DB;

	private boolean done = false;
	private String key = null;

	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());
	
	public WaitAckStartEpoch(StartEpoch<NodeIDType> startEpoch,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		super(startEpoch.getCurEpochGroup(), startEpoch.getCurEpochGroup()
				.size() / 2 + 1);
		this.startEpoch = startEpoch;
		this.DB = DB;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		return start();
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		// send StartEpoch to all new actives and await a majority
		GenericMessagingTask<NodeIDType, StartEpoch<NodeIDType>> mtask = new GenericMessagingTask<NodeIDType, StartEpoch<NodeIDType>>(
				this.startEpoch.getCurEpochGroup().toArray(), this.startEpoch);
		return mtask.toArray();
	}

	@Override
	public String refreshKey() {
		return (this.key = this.getClass().getSimpleName() + this.DB.getMyID()
				+ ":" + this.startEpoch.getServiceName() + ":"
				+ this.startEpoch.getEpochNumber());
		//return (this.key = Util.refreshKey(this.startEpoch.getSender().toString()));
	}

	public static final ReconfigurationPacket.PacketType[] types = { ReconfigurationPacket.PacketType.ACK_START_EPOCH, };

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
		assert(event.getType().equals(types[0]));
		if (isDone()) {
			log.log(Level.INFO, MyLogger.FORMAT[3], new Object[] { this,
					"successfully processed", event.getType(),
					"after being done" });
			;
		}
		return !isDone();
	}

	// Send dropEpoch when startEpoch is acked by majority
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {

		log.log(Level.INFO, MyLogger.FORMAT[3], new Object[]{
				getClass().getSimpleName(),
				this.startEpoch.getInitiator(),
				" received MAJORITY ackStartEpoch for ",
				startEpoch.getSummary(),
				(this.startEpoch.creator != null ? "; sending ack to client "
						+ this.startEpoch.creator : "")});
		
		this.setDone(true);
		// multicast start epoch confirmation message
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest(
				this.DB.getMyID(),
				this.startEpoch,
				RCRecordRequest.RequestTypes.REGISTER_RECONFIGURATION_COMPLETE);
		GenericMessagingTask<NodeIDType, ?> epochStartCommit = new GenericMessagingTask(
				this.DB.getMyID(), rcRecReq);
		//this.DB.handleIncoming(rcRecReq);
		GenericMessagingTask<NodeIDType, ?>[] mtasks = null;
		if (!this.startEpoch.isInitEpoch()) { // not creation epoch
			ptasks[0] = new WaitAckDropEpoch<NodeIDType>(this.startEpoch, this.DB);
			// propagate start epoch confirmation to all
			mtasks = epochStartCommit.toArray();
		} else if (this.startEpoch.creator != null) { // creation epoch
			mtasks = new GenericMessagingTask[2];
			// propagate start epoch confirmation to all
			mtasks[0] = epochStartCommit;
			// also inform client of name creation
			mtasks[1] = (new GenericMessagingTask(this.startEpoch.creator,
					new CreateServiceName(null,
							this.startEpoch.getServiceName(), 0, null)));
		} else
			assert (false);
		//ProtocolExecutor.timedCancel(this); // default action will do timed cancel
		return mtasks;
	}
	
	private synchronized boolean isDone() { return done;}
	private void setDone(boolean b) {this.done = b;}
	public String toString() {
		return this.getClass().getSimpleName() + this.DB.getMyID();
	}
}
