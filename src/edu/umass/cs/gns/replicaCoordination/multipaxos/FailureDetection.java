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

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
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
 */
public class FailureDetection {
	// final static
	private static long NODE_DETECTION_TIMEOUT_MILLIS = 5000;
	private static long INTER_PING_PERIOD_MILLIS = 3000;
	private static long COORDINATOR_FAILURE_DETECTION_TIMEOUT_MILLIS = 3*NODE_DETECTION_TIMEOUT_MILLIS; // Triggers a run for coordinator even if not next in line

	// final 
	private final ScheduledExecutorService execpool = Executors.newScheduledThreadPool(5);
	private final int myID;
	private final GNSNIOTransport nioTransport;
	
	// non-final 
	private Set<Integer> monitoredNodes;  // other nodes to which keepalives will be sent
	private HashMap<Integer,Long> lastHeardFrom;
	private HashMap<Integer,ScheduledFuture<PingTask>> futures;

	private static Logger log = Logger.getLogger(FailureDetection.class.getName());
	
	FailureDetection(int id, NodeConfig nc, GNSNIOTransport niot, PaxosConfig pc) {
		nioTransport = niot;
		myID = id;
		lastHeardFrom = new HashMap<Integer,Long>();
		monitoredNodes = new TreeSet<Integer>();
		futures = new HashMap<Integer,ScheduledFuture<PingTask>>();
		initialize(pc);  // this line needs to be commented for paxos tests to make sense
	}
	// FIXME: Need to fix PaxosConfig first
	private void initialize(PaxosConfig pc) {
		if(pc==null) return;
		NODE_DETECTION_TIMEOUT_MILLIS = pc.getFailureDetectionTimeoutMillis();
		INTER_PING_PERIOD_MILLIS = pc.getFailureDetectionPingMillis();
		COORDINATOR_FAILURE_DETECTION_TIMEOUT_MILLIS = 2*NODE_DETECTION_TIMEOUT_MILLIS;
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
	/* Synchronized in case we need to use cleanupFailedPingTask.
	 * Both methods touch 
	 */
	public synchronized void monitor(int id)  {
		if(!this.monitoredNodes.contains(id)) this.monitoredNodes.add(id);
		try {
			if(!this.futures.containsKey(id)) {
				PingTask pingTask = new PingTask(id, getPingPacket(id), this.nioTransport, this);
				/* Not sure how to remove the warning below. The compiler doesn't seem to like 
				 * ScheduledFuture<PingTask> and spews a cryptic message.
				 */
				ScheduledFuture future = execpool.scheduleAtFixedRate(pingTask, 2000, 
						FailureDetection.INTER_PING_PERIOD_MILLIS, TimeUnit.MILLISECONDS);
				futures.put(id, future);
			}
		} catch(JSONException e) {
			log.severe("Can not create ping packet at node " + this.myID + " to monitor node " + id);
			e.printStackTrace();
		}
	}
	
	protected void receive(FailureDetectionPacket fdp) {
		log.finest("Node " + this.myID + " received ping from node " + fdp.senderNodeID);
		this.lastHeardFrom.put(fdp.senderNodeID, System.currentTimeMillis());
	}

	protected boolean isNodeUp(int id) {
		Long lastHeard = 0L;
		if((lastHeard = this.lastHeardFrom.get(id))!=null) {
			if(System.currentTimeMillis() - lastHeard < NODE_DETECTION_TIMEOUT_MILLIS)
				return true;
		}
		return false;
	}
	protected boolean lastCoordinatorLongDead(int id) {
		Long lastHeard = 0L;
		long now = System.currentTimeMillis();
		if((lastHeard = this.lastHeardFrom.get(id))!=null) {
			return (now - lastHeard) > FailureDetection.COORDINATOR_FAILURE_DETECTION_TIMEOUT_MILLIS;
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
		private final GNSNIOTransport nioTransport;
		//private final FailureDetection FD; // Needed if cleanupFailedPing is used.
		
		PingTask(int id, JSONObject fdpJson, GNSNIOTransport niot, FailureDetection fd) {
			destID = id;
			pingJson = fdpJson;
			nioTransport = niot;
			//FD = fd;
		}
		public void run() {
			try {
				nioTransport.sendToID(destID, pingJson);
			} catch(IOException e) {
				try {
				log.info("Encountered IOException while sending keepalive from node " + 
				pingJson.getInt("sender") + " to node " + destID);
				//FD.cleanupFailedPingTask(destID);
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

		
	/***************************************************************/
	/* Unused. May be invoked by PaxosInstanceStateMachine via
	 * PaxosManager later.
	 */
	protected class RunForCoordinatorTask implements Runnable {
		private final PaxosInstanceStateMachine pinstance;
		RunForCoordinatorTask(PaxosInstanceStateMachine p) {pinstance =  p;}
		// Too expensive to call for each instance periodically
		public void run() {pinstance.checkRunForCoordinator();} 
	}
	/* Unused. May be invoked by PaxosInstanceStateMachine via
	 * PaxosManager later.
	 */
	protected void scheduleRunForCoordinatorTask(PaxosInstanceStateMachine pism) {
		RunForCoordinatorTask rctask = new RunForCoordinatorTask(pism);
		execpool.scheduleAtFixedRate(rctask, 0, 5, TimeUnit.SECONDS);
	}
	/***************************************************************/

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("FAILURE: I am not tested, so I am useless and should drown myself in a puddle. Try running PaxosManager's test for now.");
	}
}
