package edu.umass.cs.gns.protocoltask;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.protocoltask.json.ProtocolPacket;
import edu.umass.cs.gns.util.MultiArrayMap;

/**
 * @author V. Arun
 */

/*
 * The purpose of this class is to store ProtocolTasks and activate them when a
 * corresponding event arrives.
 */
public class ProtocolExecutor<NodeIDType, EventType, KeyType> {
	public static final boolean DEBUG = NIOTransport.DEBUG;
	public static final int MAX_TASKS = 10000;
	public static final int MAX_THREADS = 10;

	/*
	 * The restart period below if used to enable retransmissions for reliability
	 * should be no less than a minute (except for testing). NIO already ensures
	 * reliable transmission (except during high congestion or intermittent
	 * crashes of the remote destination), so the the restart below, if used for
	 * reliable transmission, should keep in mind that aggressive retransmission
	 * is detrimental when NIO can't do it for you.
	 */
	public static final long DEFAULT_RESTART_PERIOD = 60000; // anything more aggressive is bad
	public static final long TOO_MANY_TASKS_CHECK_PERIOD = 300; // seconds

	private final NodeIDType myID;
	private final JSONMessenger<NodeIDType> messenger;
	private final ScheduledThreadPoolExecutor executor =
			new ScheduledThreadPoolExecutor(MAX_THREADS);
	private final MultiArrayMap<KeyType, ProtocolTaskWrapper<NodeIDType, EventType, KeyType>> protocolTasks =
			new MultiArrayMap<KeyType, ProtocolTaskWrapper<NodeIDType, EventType, KeyType>>(
					MAX_TASKS);
	private final HashMap<EventType, ProtocolTaskWrapper<NodeIDType, EventType, KeyType>> defaultTasks =
			new HashMap<EventType, ProtocolTaskWrapper<NodeIDType, EventType, KeyType>>();

	private Logger log =
			NIOTransport.LOCAL_LOGGER ? Logger.getLogger(getClass().getName())
					: GNS.getLogger();

	public ProtocolExecutor(JSONMessenger<NodeIDType> m) {
		this.messenger = m;
		this.myID = m.getMyID();
		this.executor.scheduleWithFixedDelay(new TooManyTasksWarner(), 0,
				TOO_MANY_TASKS_CHECK_PERIOD, TimeUnit.SECONDS);
	}

	public void register(EventType event, ProtocolTask<NodeIDType, EventType, KeyType> task) {
		this.defaultTasks.put(event,
				new ProtocolTaskWrapper<NodeIDType, EventType, KeyType>(task));
		assert (this.defaultTasks.size() > 0);
	}

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
	public void spawn(ProtocolTask<NodeIDType, EventType, KeyType> actualTask) {
		wrapSpawn(actualTask);
	}

	private ProtocolTaskWrapper<NodeIDType, EventType, KeyType> wrapSpawn(
			ProtocolTask<NodeIDType, EventType, KeyType> actualTask) {
		ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task =
				new ProtocolTaskWrapper<NodeIDType, EventType, KeyType>(actualTask);
		this.insert(task);
		send(this.start(task), task.getKey()); // FIXME: Not sure if send should be done before or after enqueuing
		return task;
	}

	public void stop() {
		this.messenger.stop();
		this.executor.shutdown();
	}

	private synchronized void insert(
			ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
		while (task.getKey() == null ||
				this.protocolTasks.containsKey(task.getKey()))
			task.refreshKey();
		log.info("Node" + myID + " inserting key " + task.getKey() +
				" for task " + task.getClass());
		this.protocolTasks.put(task.getKey(), task);
	}

	private GenericMessagingTask<NodeIDType,?>[] start(ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
		try {
			return task.start();
		} catch (Exception e) {
			handleException(e, task);
		}
		return null;
	}

	private GenericMessagingTask<NodeIDType,?>[] restart(ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
		if (task instanceof SchedulableProtocolTask) {
			return ((SchedulableProtocolTask<NodeIDType, EventType, KeyType>) task).restart();
		} else
			return start(task);
	}

	/*
	 * schedule will periodically re-invoke task.start() for as long as it is
	 * present in the hashmap. The periodic invocation will stop once the task
	 * is removed from the hashmap.
	 */
	public void schedule(ProtocolTask<NodeIDType, EventType, KeyType> actualTask,
			long period) {
		ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task = wrapSpawn(actualTask);
		// schedule restarts
		Restarter restarter = new Restarter(task);
		if (period > DEFAULT_RESTART_PERIOD)
			log.severe("Specifying a period of less than " +
					DEFAULT_RESTART_PERIOD + " milliseconds is a bad idea");
		task.setFuture(this.executor.scheduleWithFixedDelay(restarter, period,
				period, TimeUnit.MILLISECONDS));
	}

	public void schedule(ProtocolTask<NodeIDType, EventType, KeyType> actualTask) {
		this.schedule(actualTask, DEFAULT_RESTART_PERIOD);
	}

	private synchronized ProtocolTaskWrapper<NodeIDType, EventType, KeyType> retrieve(
			KeyType key) {
		return this.protocolTasks.get(key);
	}

	private void handleException(Exception e,
			ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
		if (!(e instanceof CancelProtocolTaskException)) {
			log.severe("Exception in protocol task " + task.getClass() + ":" +
					task.getKey());
			e.printStackTrace();
		}
		remove(task);
	}

	/*
	 * remove must also cancel the scheduled future if one exists.
	 */
	private synchronized void remove(
			ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
		if (task.getFuture() != null)
			task.getFuture().cancel(true);
		this.protocolTasks.remove(task.getKey());
	}

	private class Restarter implements Runnable {
		final ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task;

		Restarter(ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task) {
			this.task = task;
		}

		public void run() {
			// calling parent send and start
			GenericMessagingTask<NodeIDType,?>[] mtasks = restart(task);
			send(mtasks, this.task.getKey());
		}
	}

	/*
	 * When an event arrives, (1) call the corresponding task's action, (2) send
	 * any messages specified in the return value, and (3) schedule a new
	 * protocol task if one is spawned.
	 */
	public boolean handleEvent(ProtocolEvent<EventType, KeyType> event) {
		ProtocolTaskWrapper<NodeIDType, EventType, KeyType> task = null;
		/*
		 * default task gets tried first, otherwise protocol task. Not sure if
		 * this would be problematic in situations when a protocol task needs to
		 * rely on a default task step.
		 */
		if ((task = this.defaultTasks.get(event.getType())) != null) {
			log.fine("Node" + myID + " handling default event " +
					event.getType());
		} else if ((task = this.retrieve(event.getKey())) != null) {
			log.fine("Node" + myID + " handling protocol task for " +
					event.getType() + ":" + event.getKey());
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
		return true;
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	// FIXME: Throw exception in order to cancel a task
	public static void cancel(ProtocolTask<?, ?, ?> task) {
		throw new CancelProtocolTaskException("Canceling task " +
				task.getClass() + " with key " + task.getKey());
	}

	private boolean send(GenericMessagingTask<NodeIDType,?>[] mtasks, KeyType key) {
		boolean allSent = true;
		if (mtasks == null || mtasks.length == 0)
			return true;
		mtasks = setKey(mtasks, key);
		for (GenericMessagingTask<NodeIDType,?> mtask : mtasks) {
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
	 * This method is concretized for Long keys and ProtocolPacket messages.
	 */
	private GenericMessagingTask<NodeIDType,?>[] setKey(GenericMessagingTask<NodeIDType,?>[] mtasks, KeyType key) {
		if (GenericMessagingTask.isEmpty(mtasks))
			return mtasks;
		for (GenericMessagingTask<NodeIDType,?> mtask : mtasks) {
			for (int i = 0; i < mtask.msgs.length; i++) {
				if (mtask.msgs[i] instanceof ProtocolPacket &&
						key instanceof Long) {
					((ProtocolPacket) (mtask.msgs[i])).setKey((Long) key);
				}
			}
		}
		return mtasks;
	}

	public int size() {
		return this.protocolTasks.size();
	}

	// To check periodically and warn if too many tasks are scheduled
	private class TooManyTasksWarner implements Runnable {
		public void run() {
			if (size() > MAX_TASKS) {
				log.severe("Too many tasks (" + size() +
						") scheduled with ProtocolExecutor");
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
