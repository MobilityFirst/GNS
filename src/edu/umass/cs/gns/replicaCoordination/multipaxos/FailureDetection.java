package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.FailureDetectionPacket;

/**
@author V. Arun
 */

/* FailureDetection provides failure detection for all nodes specified
 * by its monitor(.) interface. It really doesn't have much to do 
 * with paxos coz it is just a node monitoring utility. It allows
 * the failure detection overhead across all paxos instances on a
 * node to be amortized so that the overhead is no greater than
 * all nodes monitoring all other nodes.
 * 
 * There is one failure detection instance per machine. This class could 
 * be static, but it is not so that we can test emulations involving
 * multiple "machines" within a JVM.
 * 
 * Testability: This class is not yet unit-testable. Both PaxosManager 
 * and TESTPaxosMain test this class. 
 */
public class FailureDetection {
	// final static 
	private static final double MAX_FAILURE_DETECTION_TRAFFIC = 1/100.0; // 1 ping per 100ms total at each node
	private static final double PING_PERTURBATION_FACTOR = 0.25; // pings randomly spaced within inter_ping_period_millis times this factor
	
	// static
	private static long node_detection_timeout_millis = 6000; // ms
	private static long inter_ping_period_millis = node_detection_timeout_millis/2;
	private static long coordinator_failure_detection_timeout = 3*node_detection_timeout_millis; // run for coordinator even if not next-in-line 

	// final 
	private final ScheduledExecutorService execpool = Executors.newScheduledThreadPool(5);
	private final int myID;
	private final InterfaceJSONNIOTransport nioTransport;

	// non-final 
	private Set<Integer> monitoredNodes;  // other nodes to which keepalives will be sent
	private HashMap<Integer,Long> lastHeardFrom;
	private HashMap<Integer,ScheduledFuture<PingTask>> futures;

	private static Logger log = Logger.getLogger(FailureDetection.class.getName());

	FailureDetection(int id, InterfaceNodeConfig nc, InterfaceJSONNIOTransport niot, PaxosConfig pc) {
		nioTransport = niot;
		myID = id;
		lastHeardFrom = new HashMap<Integer,Long>();
		monitoredNodes = new TreeSet<Integer>();
		futures = new HashMap<Integer,ScheduledFuture<PingTask>>();
		initialize(pc);  // this line needs to be commented for paxos tests to make sense
	}
	// FIXME: Should really not be taking this from outside
	private void initialize(PaxosConfig pc) {
		if(pc==null) return;
		log.severe("Configuring paxos with external failure detection parameters is a bad idea, doing it anyway.");
		node_detection_timeout_millis = pc.getFailureDetectionTimeoutMillis();
		inter_ping_period_millis = pc.getFailureDetectionPingMillis();
		coordinator_failure_detection_timeout = 2*node_detection_timeout_millis;
	}
	// makes sure that FD params are reasonable
	private synchronized void adjustFDParams() {
		boolean adjusted = false;
		int numMonitored = this.monitoredNodes.size();
		double load = ((double)numMonitored)/inter_ping_period_millis;
		if(load > MAX_FAILURE_DETECTION_TRAFFIC) {
			inter_ping_period_millis = (long)(numMonitored/MAX_FAILURE_DETECTION_TRAFFIC);
			node_detection_timeout_millis = inter_ping_period_millis*2;
			coordinator_failure_detection_timeout = node_detection_timeout_millis*3;
			assert(inter_ping_period_millis>0); // just to make sure we didn't accidentally (int) cast it 0 above :)
			adjusted = true;
		}
		/* If there was any adjustment above, we need to kill and restart periodic ping tasks
		 * because there is no way to just change their period midway.
		 */
		if(adjusted) {
			for(int id : this.monitoredNodes) {
				unMonitor(id);
				monitor(id);
			}
		}
	}

	/* Synchronized because we don't want monitoredNodes 
	 * getting concurrently read by pingAll().
	 */
	public void monitor(int[] nodes) {
		for(int i=0; i<nodes.length; i++) {
			monitor(nodes[i]);
		}
	}
	public void monitor(Set<Integer> nodes) {
		for(int id : nodes) {
			monitor(id);
		}
	}
	/* Used only during testing */
	public synchronized void unMonitor(int id) {
		boolean wasPresent = this.monitoredNodes.remove(id); 
		if(wasPresent && this.futures.containsKey(id)) {
			ScheduledFuture<PingTask> pingTask = futures.get(id);
			pingTask.cancel(true);
			futures.remove(id);
		}
	}
	/* Synchronized as it touches monitoredNodes.
	 */
	public synchronized void monitor(int id)  {
		if(!this.monitoredNodes.contains(id)) this.monitoredNodes.add(id);
		try {
			if(!this.futures.containsKey(id)) {
				PingTask pingTask = new PingTask(id, getPingPacket(id), this.nioTransport);
				/* Not sure how to remove the warnings below. The compiler doesn't seem to like 
				 * ScheduledFuture<PingTask> and spews a cryptic message.
				 */
				ScheduledFuture future = execpool.scheduleAtFixedRate(pingTask, 
						(long)(PING_PERTURBATION_FACTOR*node_detection_timeout_millis*Math.random()), 
						FailureDetection.inter_ping_period_millis, TimeUnit.MILLISECONDS);
				futures.put(id, future);
			}
		} catch(JSONException e) {
			log.severe("Can not create ping packet at node " + this.myID + " to monitor node " + id);
			e.printStackTrace();
		}
	}

	protected void receive(FailureDetectionPacket fdp) {
		log.finest("Node " + this.myID + " received ping from node " + fdp.senderNodeID);
		this.heardFrom(fdp.senderNodeID);
	}
	/* protected in order to allow paxos instances to provide useful liveliness 
	 * information through the paxos manager.
	 */
	protected synchronized void heardFrom(int id) {
		this.lastHeardFrom.put(id, System.currentTimeMillis());
	}

	protected synchronized boolean isNodeUp(int id) {
		Long lastHeard = 0L;
		if(id==this.myID) return true;
		if((lastHeard = this.lastHeardFrom.get(id))!=null && 
				((System.currentTimeMillis() - lastHeard) < node_detection_timeout_millis)) {
				return true;
		}
		return false;
	}
	protected synchronized boolean lastCoordinatorLongDead(int id) {
		Long lastHeard = 0L;
		long now = System.currentTimeMillis();
		if((lastHeard = this.lastHeardFrom.get(id))!=null) {
			return (now - lastHeard) > FailureDetection.coordinator_failure_detection_timeout;
		}
		return true;
	}

	private JSONObject getPingPacket(int id) throws JSONException {
		FailureDetectionPacket fdp = new FailureDetectionPacket(myID, id, true);
		JSONObject fdpJson = fdp.toJSONObject();
		return fdpJson;
	}

	private class PingTask implements Runnable {
		private final int destID;
		private final JSONObject pingJson;
		private final InterfaceJSONNIOTransport nioTransport;

		PingTask(int id, JSONObject fdpJson, InterfaceJSONNIOTransport niot) {
			destID = id;
			pingJson = fdpJson;
			nioTransport = niot;
		}
		public void run() {
			try {
				if(!TESTPaxosConfig.isCrashed(myID)) // only to simulate crashes while testing
					nioTransport.sendToID(destID, pingJson);
			} catch(IOException e) {
				try {
					log.info("Encountered IOException while sending keepalive from node " + 
							pingJson.getInt("sender") + " to node " + destID);
					cleanupFailedPingTask(destID);
				} catch(JSONException je) {
					e.printStackTrace();
				}
			}
		}
	}

	/* Currently unused. Will be useful for cleaning up
	 * ping tasks that fail because of exceptions.
	 */
	private synchronized void cleanupFailedPingTask(int id) {
		ScheduledFuture<PingTask> pingTask = this.futures.get(id);
		if(pingTask!=null) {
			pingTask.cancel(true);
			this.futures.remove(id);
			monitor(id); 
		}
	}

	// Used only for testing
	protected InterfaceJSONNIOTransport getNIOTransport() {
		return this.nioTransport;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("FAILURE: I am not testable. Try running PaxosManager's test for now.");
	}
}
