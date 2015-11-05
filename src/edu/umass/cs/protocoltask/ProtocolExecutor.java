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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.protocoltask.json.ProtocolPacket;
import edu.umass.cs.utils.MultiArrayMap;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * @param <EventType>
 * @param <KeyType>
 * 
 *            The purpose of this class is to store ProtocolTasks and activate
 *            them when a corresponding event arrives.
 */
public class ProtocolExecutor<NodeIDType, EventType, KeyType> {
	protected static final int MAX_TASKS = 10000;
	protected static final int MAX_THREADS = 10;

	/*
	 * The restart period below if used to enable retransmissions for
	 * reliability should be no less than a minute (except for testing). NIO
	 * already ensures reliable transmission (except during high congestion or
	 * intermittent crashes of the remote destination), so the the restart
	 * below, if used for reliable transmission, should keep in mind that
	 * aggressive retransmission is detrimental when NIO can't do it for you.
	 * 
	 * Anything more aggressive than DEFAULT_RESTART_PERIOD is a bad idea if the
	 * motivation is to get around network losses. Other system- or
	 * protocol-specific reasons are okay for setting lower periods.
	 */
	protected static final long DEFAULT_RESTART_PERIOD = 60000;
	protected static final long TOO_MANY_TASKS_CHECK_PERIOD = 300; // seconds

	private final NodeIDType myID;
	private final Messenger<NodeIDType, ?> messenger;
	private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
			MAX_THREADS);
	private static final HashSet<Object> canceledKeys = new HashSet<Object>();

	private final MultiArrayMap<KeyType, ProtocolTaskWrapper<NodeIDType, EventType, KeyType>> protocolTasks = new MultiArrayMap<KeyType, ProtocolTaskWrapper<NodeIDType, EventType, KeyType>>(
			MAX_TASKS);
	private final HashMap<EventType, ProtocolTaskWrapper<NodeIDType, EventType, KeyType>> defaultTasks = new HashMap<EventType, ProtocolTaskWrapper<NodeIDType, EventType, KeyType>>();

	private static final Logger log = Logger.getLogger(ProtocolExecutor.class
			.getName());

	/**
	 * @return Logger.
	 */
	public static Logger getLogger() {
		return log;
	}

	/**
	 * @param messenger
	 */
	public ProtocolExecutor(Messenger<NodeIDType, ?> messenger) {
		this.messenger = messenger;
		this.myID = messenger.getMyID();
		this.executor.scheduleWithFixedDelay(new TooManyTasksWarner(), 0,
				TOO_MANY_TASKS_CHECK_PERIOD, TimeUnit.SECONDS);
	}

	/*
	 * The register methods tell ProtocolExecutor which events to demultiplex to
	 * which queued tasks.
	 */
	/**
	 * @param event
	 * @param task
	 */
	public void register(EventType event,
			ProtocolTask<NodeIDType, EventType, KeyType> task) {
		this.defaultTasks.put(event,
				new ProtocolTaskWrapper<NodeIDType, EventType, KeyType>(task));
		assert (this.defaultTasks.size() > 0);
	}

	/**
	 * @param events
	 * @param task
	 */
	public void register(Set<EventType> events,
			ProtocolTask<NodeIDType, EventType, KeyType> task) {
		for (EventType event : events) {
			this.defaultTasks.put(event,
					new ProtocolTaskWrapper<NodeIDType, EventType, KeyType>(
							task));
		}
		assert (this.defaultTasks.size() > 0);
	}

	/**
	 * @param task
	 */
	public void register(ProtocolTask<NodeIDType, EventType, KeyType> task) {
		for (EventType event : task.getEventTypes()) {
			this.defaultTasks.put(event,
					new ProtocolTaskWrapper<NodeIDType, EventType, KeyType>(
							task));
		}
		assert (this.defaultTasks.size() > 0);
	}

	/**
	 * @param event
	 * @param task
	 */
	public void unRegister(EventType event,
			ProtocolTask<NodeIDType, EventType, KeyType> task) {
		if (this.defaultTasks.get(event) == task) {
			this.defaultTasks.remove(event);
		}
	}

	/*
	 * spawn is a one-time invocation of task.start and insertion into the
	 * hashmap in order to match protocol events against the task.
	 * 
	 * Refer also to schedule.
	 */
	/**
	 * @param actualTask
	 */
	public void spawn(ProtocolTask<NodeIDType, EventType, KeyType> actualTask) {
		if (actualTask instanceof SchedulableProtocolTask)
			schedule((SchedulableProtocolTask<NodeIDType, EventType, KeyType>) actualTask);
		else
			wrapSpawn(actualTask);
	}

	/**
	 * @param actualTask
	 * @return True if spawned.
	 */
	public synchronized boolean spawnIfNotRunning(
			ProtocolTask<NodeIDType, EventType, KeyType> actualTask) {
		if (this.isRunning(actualTask.getKey())) {
			log.log(Level.FINE, "{0} unable to re-spawn already running task",
					new Object[] { this, actualTask.getKey() });
			return false;
		}
		this.spawn(actualTask);
		return true;
	}

	public String toString() {
		return this.getClass().getSimpleName() + myID;
	}

	// wraps protocol task into wrapper before spawning
	private ProtocolTaskWrapper<NodeIDType, EventType, KeyType> wrapSpawn(
			ProtocolTask<NodeIDType, EventType, KeyType> actualTask) {
		ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task = new ProtocolTaskWrapper<NodeIDType, EventType, KeyType>(
				actualTask);
		this.insert(task);
		//send(this.start(task), task.getKey());
		this.kickStart(task);
		// task inserted, but start() may be ongoing
		return task;
	}
	
	private void kickStart(final ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
		// don't wait for the future
		this.executor.submit(new Runnable() {
			public void run() {
				send(start(task), task.getKey());
			}
		});
	}

	/**
	 * 
	 */
	public void stop() {
		this.messenger.stop();
		this.executor.shutdown();
	}

	// can also ask executor to act like a simple execpool
	/**
	 * @param task
	 * @return Future corresponding to scheduled task.
	 */
	public Future<?> submit(Runnable task) {
		return this.executor.submit(task);
	}
	// can also ask executor to act like a simple execpool
	/**
	 * @param task
	 * @param initialDelay
	 * @param period
	 * @param unit
	 * @return Future corresponding to scheduled task.
	 */
	public Future<?> scheduleWithFixedDelay(Runnable task, long initialDelay, long period, TimeUnit unit) {
		return this.executor.scheduleWithFixedDelay(task, 0, period, unit);
	}
	// can also ask executor to act like a simple execpool
	/**
	 * @param task
	 * @param initialDelay
	 * @param unit
	 * @return Future corresponding to scheduled task.
	 */
	public Future<?> scheduleSimple(Runnable task, long initialDelay, TimeUnit unit) {
		return this.executor.schedule(task, initialDelay, unit);
	}

	private synchronized void insert(
			ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
		if (task.getKey() == null || this.isRunning(task.getKey())) {
			String errorMsg = "Node" + myID + " trying to insert "
					+ (task.getKey() == null ? "null" : "duplicate") + " key "
					+ task.getKey();
			log.warning(errorMsg);
			throw new ProtocolTaskCreationException(errorMsg);
		}
		log.log(Level.FINE, "{0} inserting key {1} for task {2}", new Object[] {
				this, task.getKey(), task.task.getClass() });
		this.protocolTasks.put(task.getKey(), task);
	}

	private GenericMessagingTask<NodeIDType, ?>[] start(
			ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
		try {
			return task.start();
		} catch (Exception e) {
			handleException(e, task);
		}
		return null;
	}

	private GenericMessagingTask<NodeIDType, ?>[] restart(
			ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
		if (task instanceof SchedulableProtocolTask) {
			try {
				return ((SchedulableProtocolTask<NodeIDType, EventType, KeyType>) task)
						.restart();
			} catch (Exception e) {
				handleException(e, task);
			}
		} else // FIXME: will never come here
			return start(task);
		return null;
	}

	/*
	 * schedule will periodically re-invoke task.start() for as long as it is
	 * present in the hashmap. The periodic invocation will stop once the task
	 * is removed from the hashmap.
	 * 
	 * FIXME: why synchronized?
	 */
	/**
	 * @param actualTask
	 * @param period
	 */
	public synchronized void schedule(
			SchedulableProtocolTask<NodeIDType, EventType, KeyType> actualTask,
			long period) {
		ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task = wrapSpawn(actualTask);
		// schedule restarts
		Restarter restarter = new Restarter(task);
		log.log(Level.FINE, "{0} scheduling {1} for periodic restarts",
				new Object[] { this, task.getKey() });
		task.setFuture(this.executor.scheduleWithFixedDelay(restarter, period,
				period, TimeUnit.MILLISECONDS));
	}

	/**
	 * @param actualTask
	 */
	public void schedule(
			SchedulableProtocolTask<NodeIDType, EventType, KeyType> actualTask) {
		this.schedule(actualTask, actualTask.getPeriod());
	}

	/**
	 * @param key
	 * @return ProtocolTask if any mapped to {@code key}.
	 */
	public ProtocolTask<NodeIDType, EventType, KeyType> getTask(KeyType key) {
		ProtocolTaskWrapper<NodeIDType, EventType, KeyType> wrapper = this
				.retrieve(key);
		return (wrapper != null ? wrapper.task : null);
	}

	private synchronized ProtocolTaskWrapper<NodeIDType, EventType, KeyType> retrieve(
			KeyType key) {
		return this.protocolTasks.get(key);
	}

	private void handleException(Exception e,
			ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
		if (!(e instanceof CancelProtocolTaskException)) {
			log.severe("Exception in protocol task " + task.getClass() + ":"
					+ task.getKey());
			e.printStackTrace();
		}
		remove(task);
	}

	/*
	 * remove must also cancel the scheduled future if one exists.
	 */
	private synchronized ProtocolTask<?, ?, ?> remove(
			ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
		if (task == null)
			return null;
		if (task.getFuture() != null) {
			log.log(Level.FINE, "{0} canceling protocol task {1}",
					new Object[] { this, task.task.getKey() });
			task.getFuture().cancel(true);
		}
		return this.protocolTasks.remove(task.getKey());
	}

	/**
	 * @param key
	 * @return The removed task if any.
	 */
	public synchronized ProtocolTask<?, ?, ?> remove(KeyType key) {
		return remove(this.retrieve(key));
	}

	private class Restarter implements Runnable {
		final ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task;

		Restarter(ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
			this.task = task;
		}

		public void run() {
			// calling parent send and start
			GenericMessagingTask<NodeIDType, ?>[] mtasks = restart(task);
			send(mtasks, this.task.getKey());
		}
	}

	/*
	 * When an event arrives, (1) call the corresponding task's action, (2) send
	 * any messages specified in the return value, and (3) schedule a new
	 * protocol task if one is spawned.
	 */
	/**
	 * @param event
	 * @return True if handled.
	 */
	public boolean handleEvent(ProtocolEvent<EventType, KeyType> event) {
		ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task = null;
		/*
		 * default task gets tried first, otherwise protocol task. Not sure if
		 * this would be problematic in situations when a protocol task needs to
		 * rely on a default task step.
		 */
		if ((task = this.defaultTasks.get(event.getType())) != null) {
			log.fine("Node" + myID + " handling default event "
					+ event.getType());
		} else if (event.getKey() != null
				&& (task = this.retrieve(event.getKey())) != null) {
			log.fine("Node" + myID + " handling protocol task for "
					+ event.getType() + ":" + event.getKey());
		} else if (event.getKey() == null) {
			log.warning("No default handler and null key for event "
					+ event.getType() + " " + event);
		}

		if (task == null) {
			return false;
		}
		@SuppressWarnings("unchecked")
		// FIXME: Not sure if there is a way to prevent this warning.
		ProtocolTask<NodeIDType, EventType, KeyType>[] ptasks = new ProtocolTask[1];
		try {
			send(task.handleEvent(event, ptasks), event.getKey());
		} catch (Exception e) {
			handleException(e, task);
		}
		if (ptasks[0] != null)
			spawn((ptasks[0]));
		if (removable(task.getKey()))
			remove(task);
		return true;
	}

	/**
	 * @return True if no scheduled tasks.
	 */
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * @param key
	 */
	public synchronized static void enqueueCancel(Object key) {
		canceledKeys.add(key);
	}

	private synchronized static boolean removable(Object key) {
		return canceledKeys.remove(key);
	}

	/*
	 * We only check in protocolTasks, not defaultTasks, as the latter is
	 * supposed to be always running.
	 */
	/**
	 * @param key
	 * @return True if task corresponding to {@code key} is running.
	 */
	public boolean isRunning(KeyType key) {
		return this.protocolTasks.containsKey(key);
	}

	/**
	 * @param task
	 */
	public static void cancel(ProtocolTask<?, ?, ?> task) {
		if (task != null)
			throw new CancelProtocolTaskException("Canceling task "
					+ task.getClass() + " with key " + task.getKey());
	}
	
	/**
	 * @return Number of active tasks.
	 */
	public int getActiveCount() {
		return this.executor.getActiveCount();
	}
	/**
	 * @return Number of tasks in underlying Executor. 
	 */
	public long getTaskCount() {
		return this.executor.getTaskCount();
	}
	/**
	 * @return Completed task count in underlying Executor.
	 */
	public long getCompletedTaskCount() {
		return this.executor.getCompletedTaskCount();
	}


	private boolean send(GenericMessagingTask<NodeIDType, ?>[] mtasks,
			KeyType key) {
		boolean allSent = true;

		if (mtasks == null || mtasks.length == 0)
			return true;
		mtasks = setKey(mtasks, key);
		for (GenericMessagingTask<NodeIDType, ?> mtask : mtasks) {
			try {
				this.messenger.send(mtask);
			} catch (IOException ioe) {
				ioe.printStackTrace();
				allSent = false;
			} catch (JSONException je) {
				je.printStackTrace();
				allSent = false;
			}
		}
		return allSent;
	}

	/*
	 * This method is sets the key in outgoing protocol packets so that they can
	 * be matched correctly to the queued task at the recipient
	 */
	@SuppressWarnings("unchecked")
	private GenericMessagingTask<NodeIDType, ?>[] setKey(
			GenericMessagingTask<NodeIDType, ?>[] mtasks, KeyType key) {
		if (GenericMessagingTask.isEmpty(mtasks))
			return mtasks;
		for (GenericMessagingTask<NodeIDType, ?> mtask : mtasks) {
			if (!mtask.isEmpty()) {
				assert (mtask.msgs != null);
				for (int i = 0; i < mtask.msgs.length; i++) {
					if (mtask.msgs[i] instanceof ProtocolPacket) {
						if (((ProtocolEvent<?, KeyType>) (mtask.msgs[i]))
								.getKey() == null)
							((ProtocolEvent<?, KeyType>) (mtask.msgs[i]))
									.setKey(key);
					} else {
						// do nothing
					}
				}
			}
		}
		return mtasks;
	}

	/**
	 * @return Number of scheduled tasks.
	 */
	public int size() {
		return this.protocolTasks.size();
	}

	// To check periodically and warn if too many tasks are scheduled
	private class TooManyTasksWarner implements Runnable {
		public void run() {
			if (size() > MAX_TASKS) {
				log.severe("Too many tasks (" + size()
						+ ") scheduled with ProtocolExecutor");
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Not directly testable. Run ExampleNode instead.");
	}

}
