/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
 *            A utility task to "consume" batched requests.
 */
public abstract class ConsumerBatchTask<TaskType> extends
		ConsumerTask<TaskType> {

	private final TaskType[] dummy;

	private final boolean peek;

	/**
	 * @param lock
	 * @param dummy
	 */
	public ConsumerBatchTask(Collection<TaskType> lock, TaskType[] dummy) {
		this(lock, dummy, false);
	}

	/**
	 * @param lock
	 * @param dummy
	 */
	public ConsumerBatchTask(Map<?, TaskType> lock, TaskType[] dummy) {
		this(lock, dummy, false);
	}

	/**
	 * @param lock
	 * @param dummy
	 * @param peek
	 */
	public ConsumerBatchTask(Collection<TaskType> lock, TaskType[] dummy,
			boolean peek) {
		super(lock);
		this.dummy = Arrays.copyOf(dummy, 0);
		this.peek = peek;
	}

	/**
	 * @param lock
	 * @param dummy
	 * @param peek
	 */
	public ConsumerBatchTask(Map<?, TaskType> lock, TaskType[] dummy,
			boolean peek) {
		super(lock);
		this.dummy = Arrays.copyOf(dummy, 0);
		this.peek = peek;
	}

	public abstract void enqueueImpl(TaskType task);

	public abstract TaskType dequeueImpl();

	public abstract void process(TaskType task);

	/**
	 * @param tasks
	 */
	public abstract void process(TaskType[] tasks);

	@SuppressWarnings("unchecked")
	private TaskType[] dequeueAll(boolean remove) {
		synchronized (lock) {
			TaskType[] tasks = null;
			if (this.lock instanceof Collection) {
				tasks = ((Collection<?>) this.lock).toArray(dummy);
				if (remove)
					((Collection<?>) this.lock).clear();
				return tasks;
			} else if (this.lock instanceof Map) {
				tasks = ((Map<?, TaskType>) this.lock).values().toArray(dummy);
				if (remove)
					((Map<?, TaskType>) this.lock).clear();
				return tasks;

			}
			throw new RuntimeException(
					"ConsumerBatchTask lock must be of type Collection<TaskType");
		}
	}

	protected TaskType[] dequeueAll() {
		return this.dequeueAll(true);
	}

	protected TaskType[] peekAll() {
		return this.dequeueAll(false);
	}

	@SuppressWarnings("unchecked")
	protected void remove(TaskType[] tasks) {
		synchronized (lock) {
			for (TaskType task : tasks) {
				if (this.lock instanceof Collection) {
					if (((Collection<?>) this.lock).remove(task))
						this.lockNotify();
					;
				} else if (this.lock instanceof Map) {
					if (((Map<?, TaskType>) this.lock).values().remove(task))
						this.lockNotify();
					;
				} else
					throw new RuntimeException(
							"ConsumerBatchTask lock must be of type Collection<TaskType");
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected boolean contains(TaskType task) {
		synchronized (lock) {
			if (this.lock instanceof Collection) {
				return ((Collection<?>) this.lock).contains(task);
			} else if (this.lock instanceof Map) {
				return ((Map<?, TaskType>) this.lock).values().contains(task);
			} else
				throw new RuntimeException(
						"ConsumerBatchTask lock must be of type Collection<TaskType");
		}
	}

	/**
	 * @param task
	 */
	public void enqueueAndWait(TaskType task) {
		this.enqueue(task);
		synchronized (this.lock) {
			while (this.contains(task))
				this.lockWait();
		}
	}

	public void run() {
		while (true) {
			waitForNotEmptyOrStopped();
			if (isStopped())
				break;

			this.setProcessing(true);
			process(peek ? peekAll() : dequeueAll());
			this.setProcessing(false);
		}
	}
}
