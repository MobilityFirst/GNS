/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.reconfigurationprotocoltasks;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

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
 *            This task waits for a single replica in a replicated application
 *            to finish executing the request. This is useful when you want only
 *            one replica to execute a request in the common case but ensure
 *            that the committed request is eventually executed despite
 *            failures.
 * 
 *            This is essentially a primary backup protocol. But instead of
 *            explicitly designating a primary to execute a request and
 *            propagate it via consensus to everyone (which could require roll
 *            back in the case of primary crashes), we use paxos as usual but
 *            only have one node, say the coordinator (or any designated
 *            replica, e.g., the replica that first received the client
 *            request), actually execute the request and propagate the result to
 *            others. The other replicas normally simply wait for the result,
 *            i.e., the incremental state change, to arrive. If the change does
 *            not arrive within a timeout, they go ahead and execute it anyway.
 * 
 *            When used with paxos, each decision now consists of two parts:
 *            decision and result. The replicas store the decision until either
 *            they timeout and decide to execute the decision themselves or the
 *            result arrives.
 */
public class WaitPrimaryExecution<NodeIDType> extends
		WaitAckStopEpoch<NodeIDType> {

	private static final long RESTART_PERIOD = 32*WaitAckStopEpoch.RESTART_PERIOD;

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
		log.log(Level.INFO,
				"{0} starting to wait for primary to complete reconfiguration",
				new Object[] { this.refreshKey() });
		return this.restartCount > 0 ? super.start() : null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if (this.amObviated())
			ProtocolExecutor.cancel(this);
		log.log(Level.WARNING,
				"{0} starting up SECONDARY task for {1}; this is needed only under failures, "
						+ "high congestion, or node config changes",
				new Object[] { this, this.startEpoch.getSummary() });
		return super.restart();
	}

	protected boolean amObviated() {
		return super.amObviated();
	}

	@Override
	public String getKey() {
		return this.key;
	}

	public String refreshKey() {
		return Reconfigurator.getTaskKey(this.getClass(), this.startEpoch,
				this.DB.getMyID().toString());
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