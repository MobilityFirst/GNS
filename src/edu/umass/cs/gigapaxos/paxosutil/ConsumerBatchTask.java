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
package edu.umass.cs.gigapaxos.paxosutil;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * @author arun
 *
 * @param <TaskType>
 * 
 * A utility task to "consume" batched requests.
 */
@SuppressWarnings("javadoc")
public abstract class ConsumerBatchTask<TaskType> extends
		ConsumerTask<TaskType> {

	private final TaskType[] dummy;

	public ConsumerBatchTask(Collection<TaskType> lock, TaskType[] dummy) {
		super(lock);
		this.dummy = Arrays.copyOf(dummy, 0);
	}
	public ConsumerBatchTask(Map<?,TaskType> lock, TaskType[] dummy) {
		super(lock);
		this.dummy = Arrays.copyOf(dummy, 0);
	}

	public abstract void enqueueImpl(TaskType task);

	public abstract TaskType dequeueImpl();

	public abstract void process(TaskType task);

	public abstract void process(TaskType[] tasks);

	@SuppressWarnings("unchecked")
	protected TaskType[] dequeueAll() {
		synchronized (lock) {
			TaskType[] tasks = null;
			if(this.lock instanceof Collection) {
				tasks = ((Collection<?>)this.lock).toArray(dummy);
				((Collection<?>)this.lock).clear();
				return tasks;
			}
			else if(this.lock instanceof Map) {
				tasks = ((Map<?,TaskType>)this.lock).values().toArray(dummy);
				((Map<?,TaskType>)this.lock).clear();
				return tasks;
				
			}
			throw new RuntimeException("ConsumerBatchTask lock must be of type Collection<TaskType");
		}
	}

	public void run() {
		while (true) {
			waitForNotEmptyOrStopped();
			if (isStopped())
				break;

			this.setProcessing(true);
			process(dequeueAll());
			this.setProcessing(false);
		}
	}
}
