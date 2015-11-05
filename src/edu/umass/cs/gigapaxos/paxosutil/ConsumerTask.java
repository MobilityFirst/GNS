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

import java.util.Collection;
import java.util.Map;


/**
 * @author arun
 *
 * @param <TaskType>
 * 
 * A utility for a standard "consumer" task in a producer/consumer setup.
 */
public abstract class ConsumerTask<TaskType> implements Runnable {
	
	protected final Object lock;
	private boolean processing = false;
	private boolean stopped = false;

	/**
	 * @param lock
	 */
	public ConsumerTask(Object lock) {
		this.lock = lock;
	}

	/**
	 * @param task
	 */
	public abstract void enqueueImpl(TaskType task);

	/**
	 * @return Task to be consumed.
	 */
	public abstract TaskType dequeueImpl();

	/**
	 * @param task
	 */
	public abstract void process(TaskType task);

	private TaskType dequeue() {
		//if (Util.oneIn(20)) DelayProfiler.updateMovAvg("sleep", this.sleepDuration);

		long millis = sleepDuration >= 1 ? (long) sleepDuration : (Math
				.random() < sleepDuration ? 1 : 0);		
		if(millis > 0)
			try {
				Thread.sleep(millis);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		synchronized (lock) {
			return this.dequeueImpl();
		}
	}

	/**
	 * @param task
	 */
	public void enqueue(TaskType task) {
		synchronized (lock) {
			try {
				this.enqueueImpl(task);
			} finally {
				this.lockNotify();;
			}
		}
	}

	/**
	 * 
	 */
	public void start() {
		(new Thread(this)).start();
	}
	/**
	 * @param name
	 */
	public void start(String name) {
		Thread me = new Thread(this);
		me.setName(name);
		me.start();
	}

	protected boolean isEmpty() {
		synchronized (lock) {
			if (this.lock instanceof Collection)
				return ((Collection<?>) this.lock).isEmpty();
			else if (this.lock instanceof Map)
				return ((Map<?, ?>) this.lock).isEmpty();
			else
				throw new RuntimeException(
						"ConsumerTask lock must be of type Collection or Map");
		}
	}

	protected void lockWait() {
		synchronized (this.lock) {
			try {
				this.lock.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	protected void lockNotify() {
		synchronized (this.lock) {
			this.lock.notifyAll();
		}
	}

	/**
	 * 
	 */
	public void waitToFinish() {
		synchronized (this.lock) {
			while (!this.isEmpty() || this.getProcessing())
				this.lockWait();
			this.stop();
		}
	}

	protected boolean getProcessing() {
		synchronized (this.lock) {
			return this.processing;
		}
	}

	protected void setProcessing(boolean b) {
		synchronized (this.lock) {
			this.processing = b;
			this.lockNotify();
		}
	}

	protected void waitForNotEmptyOrStopped() {
		synchronized (this.lock) {
			while (this.isEmpty() && !isStopped())
				this.lockWait();
		}
		//assert (!isEmpty() || isStopped());
	}

	public void run() {
		while (true) {
			waitForNotEmptyOrStopped();
			if (isStopped())
				break;

			this.setProcessing(true);
			process(dequeue());
			this.setProcessing(false);
		}
	}

	/**
	 * 
	 */
	public void stop() {
		synchronized (this.lock) {
			this.stopped = true;
			this.lock.notify();
		}
	}
	
	protected boolean isStopped() {
		synchronized (this.lock) {
			return this.stopped;
		}
	}

	// nonzero only for testing
	private double sleepDuration = 0;
	protected void setSleepDuration(double sleep) {
		this.sleepDuration = sleep;
	}
}
