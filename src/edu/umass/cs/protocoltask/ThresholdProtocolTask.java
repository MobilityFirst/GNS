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
package edu.umass.cs.protocoltask;

import java.util.Set;
import java.util.TreeSet;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.json.ProtocolPacket;
import edu.umass.cs.utils.Waitfor;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * @param <EventType>
 * @param <KeyType>
 * 
 *            This abstract class is concretized for Long keys, int node IDs,
 *            and ProtocolPackets. Its purpose is to receive responses from a
 *            threshold number of nodes in the specified set. To enable this,
 *            instantiators of this abstract class must implement (1) a
 *            "boolean handleEvent(event)" method that says whether or not the
 *            response was valid, which helps ThresholdProtocolTask
 *            automatically stop retrying with nodes that have already
 *            responded; (2) a handleThresholdEvent(.) method that returns a
 *            protocol task to be executed when the threshold is reached
 *            (returning null automatically cancels the task as it is considered
 *            complete).
 */
public abstract class ThresholdProtocolTask<NodeIDType, EventType, KeyType>
		implements SchedulableProtocolTask<NodeIDType, EventType, KeyType> {
	private final Waitfor<NodeIDType> waitfor;
	private final int threshold;
	private final boolean autoCancel;
	private boolean thresholdHandlerInvoked = false;
	private long restartPeriod = ProtocolExecutor.DEFAULT_RESTART_PERIOD;

	/**
	 * Set of nodes from which we expect to hear back.
	 * 
	 * @param nodes
	 */
	public ThresholdProtocolTask(Set<NodeIDType> nodes) { // default all
		this(nodes, nodes.size());
	}

	/**
	 * Set of nodes out of which we expect to hear back from {@code threshold}
	 * number of nodes.
	 * 
	 * @param nodes
	 * @param threshold
	 */
	public ThresholdProtocolTask(Set<NodeIDType> nodes, int threshold) {
		this(nodes, threshold, true);
	}

	/**
	 * Set of nodes out of which we expect to hear back from {@code threshold}
	 * number of nodes.
	 * 
	 * @param nodes
	 * @param threshold
	 * @param autoCancel 
	 */
	public ThresholdProtocolTask(Set<NodeIDType> nodes, int threshold, boolean autoCancel) {
		this.waitfor = new Waitfor<NodeIDType>(nodes);
		this.threshold = threshold;
		this.autoCancel = autoCancel;
	}

	/**
	 * 
	 * @param event
	 * @return Return value indicates if the event is a valid response.
	 */
	public abstract boolean handleEvent(ProtocolEvent<EventType, KeyType> event);

	/**
	 * Default action is to cancel the protocol task when the threshold is
	 * reached.
	 * 
	 * @param ptasks
	 * @return Returns messaging tasks to be performed when the threshold is
	 *         reached. Additional protocol task actions may also be spawned via
	 *         ptasks[0].
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, EventType, KeyType>[] ptasks) {
		ProtocolExecutor.cancel(this);

		return null;
	}

	/**
	 * This method automatically keeps track of senders from which we have
	 * already received valid responses in order to keep track of whether the
	 * threshold has been reached, and if so, it invokes
	 * {@link #handleThresholdEvent(ProtocolTask[]) handleThresholdEvent}
	 */
	@SuppressWarnings("unchecked")
	public final GenericMessagingTask<NodeIDType, ?>[] handleEvent(
			ProtocolEvent<EventType, KeyType> event,
			ProtocolTask<NodeIDType, EventType, KeyType>[] ptasks) {
		boolean validResponse = this.handleEvent(event);
		assert (event.getMessage() instanceof ProtocolPacket);
		if (validResponse)
			this.waitfor
					.updateHeardFrom(((ThresholdProtocolEvent<NodeIDType, ?, ?>) event)
							.getSender());
		GenericMessagingTask<NodeIDType, ?>[] mtasks = null;
		if (this.waitfor.getHeardCount() >= this.threshold
				&& testAndInvokeThresholdHandler()) {
			// got valid responses from threshold nodes
			mtasks = this.handleThresholdEvent(ptasks);
			if(autoCancel) {
				if (GenericMessagingTask.isEmpty(mtasks) && ptasks[0] == null)
					ProtocolExecutor.cancel(this);
				else
					ProtocolExecutor.enqueueCancel(this.getKey());
			}
		}
		return mtasks;
	}

	protected GenericMessagingTask<NodeIDType, ?>[] fix(
			GenericMessagingTask<NodeIDType, ?>[] mtasks) {
		if (mtasks == null || mtasks.length == 0 || mtasks[0] == null
				|| mtasks[0].msgs == null || mtasks[0].msgs.length == 0
				|| mtasks[0].msgs[0] == null)
			return mtasks;
		for (int i = 0; i < mtasks.length; i++) {
			mtasks[i] = fix(mtasks[i]);
		}
		return mtasks;
	}

	private GenericMessagingTask<NodeIDType, ?> fix(
			GenericMessagingTask<NodeIDType, ?> mtask) {
		if (mtask == null || mtask.msgs == null || mtask.msgs.length == 0)
			return null;
		return new GenericMessagingTask<NodeIDType, Object>(fix(
				this.waitfor.getMembersHeardFrom(), mtask.recipients),
				mtask.msgs);
	}

	/**
	 * @param restartPeriod
	 *            The period after which restart messages will be sent (only to
	 *            nodes from which valid responses have not yet been received).
	 */
	public void setPeriod(long restartPeriod) {
		this.restartPeriod = restartPeriod;
	}

	/**
	 * The current restart period.
	 */
	public long getPeriod() {
		return this.restartPeriod;
	}

	private Object[] fix(Set<NodeIDType> filter, Object[] original) {
		if (filter == null || filter.size() == 0)
			return original;
		TreeSet<NodeIDType> filtered = new TreeSet<NodeIDType>();
		for (Object obji : original) {
			@SuppressWarnings("unchecked")
			NodeIDType i = (NodeIDType) obji;
			boolean toFilter = false;
			for (NodeIDType objj : filter) {
				if (i.equals(objj))
					toFilter = true;
			}
			if (!toFilter)
				filtered.add(i);
		}
		return filtered.toArray();
	}

	private synchronized boolean testAndInvokeThresholdHandler() {
		if (!this.thresholdHandlerInvoked)
			return (this.thresholdHandlerInvoked = true);
		return false;
	}
}
