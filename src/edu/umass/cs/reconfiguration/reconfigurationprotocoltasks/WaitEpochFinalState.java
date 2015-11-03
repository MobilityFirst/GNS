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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckStartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.EpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 * @param <NodeIDType>
 */
public class WaitEpochFinalState<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	/*
	 * This restart period can be small because if the state is large, we should
	 * be sending summary handles instead anyway.
	 */
	private static final long RESTART_PERIOD = WaitAckStopEpoch.RESTART_PERIOD;

	private final StartEpoch<NodeIDType> startEpoch;
	private final AbstractReplicaCoordinator<NodeIDType> appCoordinator;
	private final RequestEpochFinalState<NodeIDType> reqState;
	private final Map<NodeIDType, String> notifiees = new ConcurrentHashMap<NodeIDType, String>();

	private Iterator<NodeIDType> prevGroupIterator;
	private boolean first = true;

	private final String key;

	private static final Logger log = (Reconfigurator.getLogger());

	/**
	 * @param myID
	 * @param startEpoch
	 * @param appCoordinator
	 */
	public WaitEpochFinalState(NodeIDType myID,
			StartEpoch<NodeIDType> startEpoch,
			AbstractReplicaCoordinator<NodeIDType> appCoordinator) {
		super(startEpoch.getPrevEpochGroup(), 1);
		this.startEpoch = startEpoch;
		this.appCoordinator = appCoordinator;
		this.prevGroupIterator = this.startEpoch.getPrevEpochGroup().iterator();
		this.reqState = new RequestEpochFinalState<NodeIDType>(myID,
				startEpoch.getPrevGroupName(),
				(startEpoch.getPrevEpochNumber()));
		this.key = this.refreshKey();
		this.setPeriod(RESTART_PERIOD);
		this.notifiees.put(this.startEpoch.getInitiator(),
				this.startEpoch.getKey());
	}

	// simply calls start() but only if state not yet received
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if (this.amObviated()) {
			ProtocolExecutor.cancel(this);
			return null;
		}
		if (!this.prevGroupIterator.hasNext())
			this.prevGroupIterator = this.startEpoch.getPrevEpochGroup()
					.iterator();
		GenericMessagingTask<NodeIDType, ?>[] mtasks = start();
		if (mtasks != null)
			log.log(Level.WARNING, MyLogger.FORMAT[2], new Object[] { getKey(),
					" resending request to ", mtasks[0].recipients[0] });
		return mtasks;
	}

	// Will try once from each prev node and then give up
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		if (!this.prevGroupIterator.hasNext())
			return null;
		this.sleepOptimization();
		log.log(Level.INFO, "{0} initiating request for final epoch state {1}",
				new Object[] { this, reqState.getSummary() });
		// Try myself first if I am in both old and new groups
		NodeIDType target = this.positionIterator();
		GenericMessagingTask<NodeIDType, ?> mtask = new GenericMessagingTask<NodeIDType, Object>(
				target, this.reqState);
		return mtask.toArray();
	}

	private NodeIDType positionIterator() {
		// firstTry is me or first prev epoch candidate
		NodeIDType firstTry = this.startEpoch.getFirstPrevEpochCandidate() != null ? this.startEpoch
				.getFirstPrevEpochCandidate() : this.appCoordinator.getMyID();
		// if not contains firstTry or not first time
		if (!this.startEpoch.getPrevEpochGroup().contains(firstTry)
				|| !this.first || (this.first = false))
			return this.prevGroupIterator.next();
		// else contains firstTry and first time
		while (this.prevGroupIterator.hasNext()
				&& !this.prevGroupIterator.next().equals(firstTry))
			;
		return firstTry; // leave iterator at self
	}

	private boolean amObviated() {
		Integer curEpoch = this.appCoordinator.getEpoch(this.startEpoch
				.getServiceName());
		if (curEpoch != null
				&& curEpoch - this.startEpoch.getEpochNumber() >= 0)
			return true;
		return false;
	}

	private void sleepOptimization() {
		try {
			/*
			 * FIXME: An optimization to wait a tiny bit to increase the
			 * likelihood that the previous epoch final state is readily
			 * available at most any previous epoch replica so that we avoid a
			 * restart timeout. For service names, this is not an issue because
			 * we supply a hint in startEpoch to try the candidate that acked
			 * the stop of the previous epoch. But for split reconfiguration
			 * operations, the timeout may still be triggered as there is no
			 * stop phase in a split operation. Ideally, we would sleep just
			 * until the splittee group has been stopped, but we don't have an
			 * easy way of determining that here.
			 */
			if (this.startEpoch.getFirstPrevEpochCandidate() == null
					|| this.startEpoch.isSplitOrMerge())
				Thread.sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return The refreshed key.
	 */
	public String refreshKey() {
		return (Reconfigurator.getTaskKey(getClass(), reqState,
				this.appCoordinator.getMyID().toString()) +
		// need different key for split/merge operations
		(!reqState.getServiceName().equals(this.startEpoch.getServiceName()) ? ":"
				+ this.startEpoch.getServiceName()
				+ ":"
				+ this.startEpoch.getEpochNumber()
				: ""));
	}

	protected static final ReconfigurationPacket.PacketType[] types = { ReconfigurationPacket.PacketType.EPOCH_FINAL_STATE };

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
		ReconfigurationPacket.PacketType type = event.getType();
		if (type == null)
			return false;
		boolean handled = false;
		/*
		 * handleEvent returns true only if replica group creation succeeds.
		 * Note that replica group creation can fail because either
		 */
		switch (type) {
		case EPOCH_FINAL_STATE:
			@SuppressWarnings("unchecked")
			EpochFinalState<NodeIDType> state = (EpochFinalState<NodeIDType>) event;
			if (!checkEpochFinalState(event))
				break;
			log.log(Level.INFO, "{0} received {1}; state=[{2}]", new Object[] {
					this, state.getSummary(), state.getState() });
			handled = this.appCoordinator.createReplicaGroup(
					this.startEpoch.getServiceName(),
					this.startEpoch.getEpochNumber(), state.getState(),
					this.startEpoch.getCurEpochGroup());

			// if !handled, we will be stuck retrying until it is true

		default:
			break;
		}
		return handled;
	}

	public String toString() {
		return this.getKey();
	}

	private boolean checkEpochFinalState(ProtocolEvent<PacketType, String> event) {
		// FIXME: What is there to check here other than the type?
		return true;
	}

	/**
	 * @param node
	 * @param key
	 * @return Node to be notified with AckStartEpoch when epoch final state
	 *         becomes ready.
	 */
	public synchronized String addNotifiee(NodeIDType node, String key) {
		return this.notifiees.put(node, key);
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {

		return this.getAckStarts();
	}

	/**
	 * @return Messaging tasks to be performed to send acks to notifiees.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] getAckStarts() {
		Set<GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>>> mtasks = new HashSet<GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>>>();
		for (NodeIDType node : new HashSet<NodeIDType>(this.notifiees.keySet())) {

			AckStartEpoch<NodeIDType> ackStartEpoch = new AckStartEpoch<NodeIDType>(
					node, startEpoch.getServiceName(),
					startEpoch.getEpochNumber(), this.appCoordinator.getMyID());
			/*
			 * Need to explicitly set key as ackStart is going to different
			 * task. This is really an abuse of protocoltask as protocoltask is
			 * designed to relieve the developer of worrying about matching
			 * packets to tasks. But we are aggregating different StartEpoch
			 * requests into a single WaitEpochFinalState task, so we have to
			 * key the responses going back to their (different) initiators
			 * manually.
			 */
			ackStartEpoch.setKey(this.notifiees.get(node));
			mtasks.add(new GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>>(
					node, ackStartEpoch));
			log.log(Level.INFO, "{0} sending {1} to RC {2} with key {3}",
					new Object[] { this, ackStartEpoch.getSummary(), node,
							this.notifiees.get(node) });
		}
		return mtasks.toArray(mtasks.iterator().next().toArray());
	}

}
