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
