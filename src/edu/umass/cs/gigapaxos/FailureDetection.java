package edu.umass.cs.gigapaxos;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxospackets.FailureDetectionPacket;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig;
import edu.umass.cs.nio.InterfaceNIOTransport;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            <p>
 * 
 *            FailureDetection provides failure detection for all nodes
 *            specified by its sendKeepAlive(.) interface. It really doesn't
 *            have much to do with paxos coz it is just a node monitoring
 *            utility. It allows the failure detection overhead across all paxos
 *            instances on a node to be amortized so that the overhead is no
 *            greater than all nodes pinging all other nodes.
 * 
 *            There is one failure detection instance per machine. This class
 *            could be static, but it is not so that we can test emulations
 *            involving multiple "machines" within a JVM.
 * 
 *            Testability: This class is not yet unit-testable. Both
 *            PaxosManager and TESTPaxosMain test this class.
 */
@SuppressWarnings("javadoc")
public class FailureDetection<NodeIDType> {
	// final static
	// 1 ping per 100ms total at each node
	private static final double MAX_FAILURE_DETECTION_TRAFFIC = 1 / 100.0;
	// pings randomly spaced within inter_ping_period_millis times this factor
	private static final double PING_PERTURBATION_FACTOR = 0.25;

	// static
	private static long node_detection_timeout_millis = 6000; // ms
	private static long inter_ping_period_millis = node_detection_timeout_millis / 2;
	// run for coordinator even if not next-in-line
	private static long coordinator_failure_detection_timeout = 3 * node_detection_timeout_millis;
	private static long pessimism_offset = 0;

	/*
	 * If initially optimistic, we assume that the last ping from some node came
	 * just before we booted and we might have just missed it, so we consider a
	 * node as dead only if we don't hear from that node for at least
	 * failure_detection_timeout after we start. If pessimistic, we assume that
	 * the last ping from that node came inter_ping_period before we booted. We
	 * could be even more pessimistic and assume that the last ping came
	 * failure_detection_timeout before we booted, i.e., we assume that all
	 * nodes are dead when we boot, but this is probably too pessimistic.
	 */
	private final long initTime = System.currentTimeMillis()
			- getPessimismOffset();

	// final
	private final ScheduledExecutorService execpool = Executors
			.newScheduledThreadPool(5);
	private final NodeIDType myID;
	private final InterfaceNIOTransport<NodeIDType, JSONObject> nioTransport;

	// non-final
	private Set<NodeIDType> keepAliveTargets;
	private HashMap<NodeIDType, Long> lastHeardFrom;
	private HashMap<NodeIDType, ScheduledFuture<PingTask>> futures;

	private static Logger log = PaxosManager.getLogger();// Logger.getLogger(FailureDetection.class.getName());

	FailureDetection(NodeIDType id,
			InterfaceNIOTransport<NodeIDType, JSONObject> niot,
			String paxosLogFolder) {
		nioTransport = niot;
		myID = id;
		lastHeardFrom = new HashMap<NodeIDType, Long>();
		keepAliveTargets = new TreeSet<NodeIDType>();
		futures = new HashMap<NodeIDType, ScheduledFuture<PingTask>>();
		initialize(paxosLogFolder);
	}

	FailureDetection(NodeIDType id,
			InterfaceNIOTransport<NodeIDType, JSONObject> niot) {
		this(id, niot, null);
	}

	protected synchronized static void setPessimistic() {
		pessimism_offset = inter_ping_period_millis;
	}

	protected synchronized static void setParanoid() {
		pessimism_offset = node_detection_timeout_millis;
	}

	protected synchronized static long getPessimismOffset() {
		return pessimism_offset;
	}

	public void close() {
		this.execpool.shutdownNow();
	}

	// should really not be taking this from outside
	private void initialize(String paxosLogFolder) {
		if (paxosLogFolder == null)
			return;
	}

	// makes sure that FD params are reasonable
	private synchronized boolean adjustFDParams() {
		boolean adjusted = false;
		int numMonitored = this.keepAliveTargets.size();
		double load = ((double) numMonitored) / inter_ping_period_millis;
		if (load > MAX_FAILURE_DETECTION_TRAFFIC) {
			inter_ping_period_millis = (long) (numMonitored
					/ MAX_FAILURE_DETECTION_TRAFFIC + 1); // +1 for strictly <
			node_detection_timeout_millis = inter_ping_period_millis * 2;
			coordinator_failure_detection_timeout = node_detection_timeout_millis * 3;
			assert (inter_ping_period_millis > 0);
			adjusted = true;
		}
		/*
		 * If there was any adjustment above, we need to kill and restart
		 * periodic ping tasks because there is no way to just change their
		 * period midway.
		 */
		
		// copy easiest way to avoid concurrent modification exceptions
		Set<NodeIDType> copy = new HashSet<NodeIDType>(this.keepAliveTargets);
		if (adjusted) {
			for (NodeIDType id : copy) {
				dontSendKeepAlive(id);
				// single-depth recursive call to adjustFDParams
				sendKeepAlive(id);
			}
		}
		return adjusted;
	}

	public void sendKeepAlive(NodeIDType[] nodes) {
		for (int i = 0; i < nodes.length; i++) {
			sendKeepAlive(nodes[i]);
		}
	}

	public void sendKeepAlive(Set<NodeIDType> nodes) {
		for (NodeIDType id : nodes) {
			sendKeepAlive(id);
		}
	}

	protected synchronized boolean dontSendKeepAlive(NodeIDType id) {
		if (this.futures.containsKey(id)) {
			ScheduledFuture<PingTask> pingTask = futures.get(id);
			pingTask.cancel(true);
			futures.remove(id);
			return true;
		}
		return false;
	}

	/*
	 * Synchronized as it touches keepAliveTargets.
	 */
	@SuppressWarnings("unchecked")
	public synchronized void sendKeepAlive(NodeIDType id) {
		if (!this.keepAliveTargets.contains(id))
			this.keepAliveTargets.add(id);
		try {
			if (!this.futures.containsKey(id)) {
				PingTask pingTask = new PingTask(id, getPingPacket(id),
						this.nioTransport);
				/*
				 * Not sure how to remove the warnings below. The compiler
				 * doesn't seem to like ScheduledFuture<PingTask> and spews a
				 * cryptic message.
				 */
				ScheduledFuture<?> future = execpool
						.scheduleAtFixedRate(pingTask,
								(long) (PING_PERTURBATION_FACTOR
										* node_detection_timeout_millis * Math
										.random()),
								FailureDetection.inter_ping_period_millis,
								TimeUnit.MILLISECONDS);
				futures.put(
						id,
						(ScheduledFuture<FailureDetection<NodeIDType>.PingTask>) future);
			}
		} catch (JSONException e) {
			log.severe("Can not create ping packet at node " + this.myID
					+ " for node " + id);
			e.printStackTrace();
		}
		adjustFDParams(); // check to adjust every time sendKeepAlive is invoked
	}

	protected void receive(FailureDetectionPacket<NodeIDType> fdp) {
		log.log(Level.FINEST, "{0}{1}{2}{3}", new Object[] { "Node ",
				this.myID, " received ping from node ", fdp.senderNodeID });
		this.heardFrom(fdp.senderNodeID);
	}

	/*
	 * protected in order to allow paxos instances to provide useful liveliness
	 * information through the paxos manager.
	 */
	protected synchronized void heardFrom(NodeIDType id) {
		this.lastHeardFrom.put(id, System.currentTimeMillis());
	}

	protected synchronized boolean isNodeUp(NodeIDType id) {
		if (id == this.myID)
			return true;
		return ((System.currentTimeMillis() - lastHeardTime(id)) < node_detection_timeout_millis);
	}

	protected synchronized boolean lastCoordinatorLongDead(NodeIDType id) {
		return ((System.currentTimeMillis() - lastHeardTime(id)) > FailureDetection.coordinator_failure_detection_timeout);
	}

	private long lastHeardTime(NodeIDType id) {
		Long lastHeard = this.lastHeardFrom.get(id);
		if (lastHeard == null)
			lastHeard = initTime;
		return lastHeard;
	}

	private JSONObject getPingPacket(NodeIDType id) throws JSONException {
		FailureDetectionPacket<NodeIDType> fdp = new FailureDetectionPacket<NodeIDType>(
				myID, id, true);
		JSONObject fdpJson = fdp.toJSONObject();
		return fdpJson;
	}

	private class PingTask implements Runnable {
		private final NodeIDType destID;
		private final JSONObject pingJson;
		private final InterfaceNIOTransport<NodeIDType, JSONObject> nioTransport;

		PingTask(NodeIDType id, JSONObject fdpJson,
				InterfaceNIOTransport<NodeIDType, JSONObject> niot) {
			destID = id;
			pingJson = fdpJson;
			nioTransport = niot;
		}

		public void run() {
			try {
				if (!TESTPaxosConfig.isCrashed(myID)) // only to simulate
														// crashes while testing
					nioTransport.sendToID(destID, pingJson);
			} catch (IOException e) {
				try {
					log.log(Level.INFO,
							"{0}{1}{2}{3}",
							new Object[] {
									"Encountered IOException while sending keepalive from node ",
									pingJson.getInt("sender"), " to node ",
									destID });
					cleanupFailedPingTask(destID);
				} catch (JSONException je) {
					e.printStackTrace();
				}
			}
		}
	}

	/*
	 * Currently unused. Will be useful for cleaning up ping tasks that fail
	 * because of exceptions.
	 */
	private synchronized void cleanupFailedPingTask(NodeIDType id) {
		ScheduledFuture<PingTask> pingTask = this.futures.get(id);
		if (pingTask != null) {
			pingTask.cancel(true);
			this.futures.remove(id);
			sendKeepAlive(id);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out
				.println("FAILURE: I am not testable. Try running PaxosManager's test for now.");
	}
}
