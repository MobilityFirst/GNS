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
import java.util.concurrent.ScheduledFuture;

import edu.umass.cs.nio.GenericMessagingTask;

/**
 * @author V. Arun
 */
/*
 * The point of this class is to wrap the user's ProtocolTask interface
 * compliant task into one wherein we could store and manipulate internal
 * variables, e.g., for killing after a long idle timeout. Alternatively, we
 * could have made ProtocolTask itself an abstract class instead of an
 * interface, but the latter is preferable.
 */
class ProtocolTaskWrapper<NodeIDType, EventType, KeyType> implements
		SchedulableProtocolTask<NodeIDType, EventType, KeyType> {

	protected static final long MAX_IDLE_TIME = 300000; // 5 mins
	protected static final long MAX_LIFETIME = 1800000; // 30 mins

	public final ProtocolTask<NodeIDType, EventType, KeyType> task;
	private long startTime = System.currentTimeMillis();
	private long lastActiveTime = System.currentTimeMillis();
	private ScheduledFuture<?> future;

	ProtocolTaskWrapper(ProtocolTask<NodeIDType, EventType, KeyType> task) {
		this.task = task;
	}

	@Override
	public KeyType getKey() {
		return this.task.getKey();
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleEvent(
			ProtocolEvent<EventType, KeyType> event,
			ProtocolTask<NodeIDType, EventType, KeyType>[] ptasks) {
		this.lastActiveTime = System.currentTimeMillis();
		return this.task.handleEvent(event, ptasks);
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		this.startTime = System.currentTimeMillis();
		return this.task.start();
	}

	//@Override
	public KeyType refreshKey() {
		throw new RuntimeException("This method should not have been called");
		//return this.task.refreshKey();
	}

	protected void setFuture(final ScheduledFuture<?> future) {
		this.future = future;
	}

	protected ScheduledFuture<?> getFuture() {
		return this.future;
	}

	protected boolean isLongIdle() {
		return (System.currentTimeMillis() - this.lastActiveTime > MAX_IDLE_TIME) ? true
				: false;
	}

	protected boolean expired() {
		return (System.currentTimeMillis() - this.startTime > MAX_LIFETIME) ? true
				: false;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		// restart if current time > start time and task has restart method
		GenericMessagingTask<NodeIDType, ?>[] mtasks = (this.task instanceof SchedulableProtocolTask
				&& (System.currentTimeMillis() > this.startTime) ? ((SchedulableProtocolTask<NodeIDType, EventType, KeyType>) (this.task))
				.restart() : this.task.start()); // if schedulable, call restart
		// if threshold task, filter out members already heard from
		if (this.task instanceof ThresholdProtocolTask) {
			((ThresholdProtocolTask<NodeIDType, ?, ?>) (this.task)).fix(mtasks);
		}
		return mtasks;
	}

	@Override
	public Set<EventType> getEventTypes() {
		return this.task.getEventTypes();
	}

	@Override
	public long getPeriod() {
		if (this.task instanceof SchedulableProtocolTask)
			return ((SchedulableProtocolTask<?, ?, ?>) this.task).getPeriod();
		return ProtocolExecutor.DEFAULT_RESTART_PERIOD;
	}
}
