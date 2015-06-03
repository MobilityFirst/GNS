package edu.umass.cs.gigapaxos.paxosutil;

import java.util.Collection;
import java.util.Map;

public abstract class ConsumerTask<TaskType> implements Runnable {
	protected final Object lock;
	private boolean processing = false;
	private boolean stopped = false;

	public ConsumerTask(Object lock) {
		this.lock = lock;
	}

	public abstract void enqueueImpl(TaskType task);

	public abstract TaskType dequeueImpl();

	public abstract void process(TaskType task);

	private TaskType dequeue() {
		synchronized (lock) {
			return this.dequeueImpl();
		}
	}

	public void enqueue(TaskType task) {
		synchronized (lock) {
			this.enqueueImpl(task);
			this.lock.notify();
		}
	}

	public void start() {
		(new Thread(this)).start();
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
		assert (!isEmpty() || isStopped());
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
}
