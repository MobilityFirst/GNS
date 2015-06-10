package edu.umass.cs.reconfiguration.reconfigurationprotocoltasks;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;


/**
 * @author arun
 *
 * @param <NodeIDType>
 * 
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

	private static final long RESTART_PERIOD = 30000;
	
	// no types, just waiting to restart reconfiguration or die if obviated
	private static ReconfigurationPacket.PacketType[] types = {};

	private final String key;

	/**
	 * @param myID
	 * @param startEpoch
	 * @param DB
	 * @param replicas
	 */
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
		return Reconfigurator.getTaskKey(this.getClass(), this.startEpoch, this.DB.getMyID().toString());
	}

	// combine self types with those of WaitAckStopEpoch
	@Override
	public Set<PacketType> getEventTypes() {
		return new HashSet<PacketType>(Arrays.asList(ReconfigurationPacket
				.concatenate(WaitAckStopEpoch.types, types)));
	}
	
	@Override
	public long getPeriod() {
		return RESTART_PERIOD;
	}
}