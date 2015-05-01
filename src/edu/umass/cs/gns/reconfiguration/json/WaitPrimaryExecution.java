package edu.umass.cs.gns.reconfiguration.json;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RCRecordRequest.RequestTypes;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.gns.util.MyLogger;

/*
 * This task waits for a single replica in a replicated application to finish
 * executing the request. This is useful when you want only one replica to
 * execute a request in the common case but ensure that the committed request is
 * eventually executed despite failures.
 * 
 * This is essentially a primary backup protocol. But instead of explicitly
 * designating a primary to execute a request and propagate it via consensus to
 * everyone (which could require roll back in the case of primary crashes), we
 * use paxos as usual but only have one node, say the coordinator (or any
 * designated replica, e.g., the replica that first received the client
 * request), actually executes the request and propagates the result to others.
 * The other replicas normally simply wait for the result, i.e., the incremental
 * state change, to arrive. If the change does not arrive within a timeout, they
 * go ahead and execute it anyway.
 * 
 * When used with paxos, each decision now consists of two parts: decision and
 * result. The replicas store the decision until either they timeout and decide
 * to execute the decision themselves or the result arrives.
 */

public class WaitPrimaryExecution<NodeIDType> extends
		WaitAckStopEpoch<NodeIDType> {

	// no types, just waiting to restart reconfiguration or die if obviated
	private static ReconfigurationPacket.PacketType[] types = {};

	private boolean started = false;
	private final String key;

	public WaitPrimaryExecution(NodeIDType myID,
			StartEpoch<NodeIDType> startEpoch,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB,
			Set<NodeIDType> replicas) {
		super(startEpoch, DB);
		this.key = this.refreshKey();
	}

	/*
	 * Nothing to do here.
	 */
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if(this.amObviated()) ProtocolExecutor.cancel(this);
		return super.restart();
	}

	// FIXME: bail out if no longer necessary
	protected boolean amObviated() {
		return super.amObviated();
	}


	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public String refreshKey() {
		return (this.getClass().getSimpleName() + this.DB.getMyID()
				+ ":" + this.startEpoch.getServiceName() + ":"
				+ this.startEpoch.getEpochNumber());
		//return (this.key = Util.refreshKey(this.DB.getMyID().toString()));
	}

	// combine self types with those of WaitAckStopEpoch
	@Override
	public Set<PacketType> getEventTypes() {
		return new HashSet<PacketType>(Arrays.asList(ReconfigurationPacket
				.concatenate(WaitAckStopEpoch.types, types)));
	}

	/*
	 * receipt of reconfiguration completion confirmation should change DB state
	 * to READY and cancel self.
	 */
	@Override
	public boolean handleEvent(ProtocolEvent<PacketType, String> event) {
		if (started)
			return super.handleEvent(event); // same as WaitAckStopEpoch
		// else wait to see if we can cancel self before (delayed) start()
		assert (getEventTypes().contains(event.getType()));
		BasicReconfigurationPacket<?> rcPacket = (BasicReconfigurationPacket<?>) event;
		// if ackStartEpoch or ackDropEpoch and matching name and epoch
		if ((rcPacket.getType().equals(
				ReconfigurationPacket.PacketType.ACK_START_EPOCH)
				&& rcPacket.getServiceName().equals(
						this.startEpoch.getServiceName()) && rcPacket
				.getEpochNumber() == this.startEpoch.getEpochNumber())
				|| rcPacket
						.getType()
						.equals(ReconfigurationPacket.PacketType.ACK_DROP_EPOCH_FINAL_STATE)
				&& rcPacket.getServiceName().equals(
						this.startEpoch.getServiceName())
				&& rcPacket.getEpochNumber() + 1 == this.startEpoch
						.getEpochNumber()) {
			// change DB state to READY marking reconfiguration as complete
			this.DB.handleIncoming(new RCRecordRequest<NodeIDType>(this.DB
					.getMyID(), this.startEpoch,
					RequestTypes.REGISTER_RECONFIGURATION_COMPLETE));
			// cancel task
			log.log(Level.INFO, MyLogger.FORMAT[2], new Object[]{this, "canceling itself"});
			ProtocolExecutor.cancel(this);
		}
		return false;
	}

}