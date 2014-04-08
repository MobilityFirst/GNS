package edu.umass.cs.gns.replicaCoordination.multipaxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;
import edu.umass.cs.gns.nsdesign.packet.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.FindReplicaGroupPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.MessagingTask;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Messenger;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.PaxosPacketDemultiplexer;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.RecoveryInfo;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


import edu.umass.cs.gns.nio.DefaultPacketDemultiplexer;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.nio.SampleNodeConfig;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.util.MultiArrayMap;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */

/* PaxosManager is the primary interface to create
 * and use paxos by creating a paxos instance. 
 * 
 * PaxosManager manages all paxos instances at a node.
 * There is typically one paxos manager per machine. This 
 * class could be static, but it is not so that we can test 
 * emulations involving multiple "machines" within a JVM.
 * 
 * PaxosManager has four functions at a machine that are
 * useful across paxos instances of all applications on 
 * the machine: (1) logging, (2) failure detection, 
 * (3) messaging, and (4) paxos instance mapping. The 
 * fourth is key to allowing the manager to demultiplex
 * incoming messages to the appropriate application paxos
 * instance.
 */
public class PaxosManager {
	public static final boolean DEBUG=NIOTransport.DEBUG;
	
	private static final long MORGUE_DELAY = 30000;
	private static final boolean MAINTAIN_CORPSES=false;
	private static final int PINSTANCES_CAPACITY = 2000000;
	private static final int LEVELS = 6;
	private static final boolean KILL_AND_RECOVER_ME_OPTION = false;

	// final
	private final AbstractPaxosLogger paxosLogger;  // logging
	private final FailureDetection FD;  // failure detection
	private final Messenger messenger;  // messaging
	private final int myID;
	private final Replicable app; // default app for all paxosIDs

	// non-final
	private MultiArrayMap<String,PaxosInstanceStateMachine> pinstances=null; // paxos instance mapping
	private HashMap<String, PaxosInstanceStateMachine> corpses=null; // stopped paxos instances about to be incinerated

	private Timer timer = new Timer();

	/* Note: PaxosManager itself maintains no NIO transport
	 * instance as it delegates all communication
	 * related activities to other objects. PaxosManager
	 * is only responsible for managing state for and 
	 * demultiplexing incoming packets to a number of 
	 * paxos instances at this node.
	 */

	private static Logger log = GNS.getLogger();; //Logger.getLogger(PaxosManager.class.getName()); // 


	PaxosManager(int id, NodeConfig nc, GNSNIOTransport niot, Replicable pi, PaxosConfig pc) {
		this.myID = id;
		this.app = pi;
		this.paxosLogger = new DerbyPaxosLogger(id, (pc!=null?pc.getPaxosLogFolder():null));
		this.FD = new FailureDetection(id, nc, niot, pc);
		this.pinstances = new MultiArrayMap<String,PaxosInstanceStateMachine>(PINSTANCES_CAPACITY, LEVELS);
		this.corpses = new HashMap<String,PaxosInstanceStateMachine>();
		this.messenger = new Messenger(id, niot);

		// Networking is needed for replaying messages during recovery
		niot.addPacketDemultiplexer(new PaxosPacketDemultiplexer(this)); // so paxos packets will come to me.
		initiateRecovery();

	}

	public PaxosInstanceStateMachine createPaxosInstance(String paxosID, short version, int id, Set<Integer> gms, Replicable app) {
		PaxosInstanceStateMachine pism = this.pinstances.get(paxosID);
		if(pism!=null) return pism;

		pism = new PaxosInstanceStateMachine(paxosID, version, id, gms, app, this);
		pinstances.put(paxosID, pism);
		this.FD.monitor(gms);
		return pism;
	}

	public void handleIncomingPacket(JSONObject jsonMsg) {
		PaxosPacketType incomingPacketType;
		try {
			assert(Packet.getPacketType(jsonMsg)==PacketType.PAXOS_PACKET);
			incomingPacketType = PaxosPacket.getPaxosPacketType(jsonMsg);

			switch (incomingPacketType){
			case FAILURE_DETECT:
				FailureDetectionPacket fdp = new FailureDetectionPacket(jsonMsg);
				FD.receive(fdp);
				break;
			case FIND_REPLICA_GROUP:
				FindReplicaGroupPacket findGroup = new FindReplicaGroupPacket(jsonMsg);
				processFindReplicaGroup(findGroup);
				break;
			default:
				String paxosID = jsonMsg.getString(PaxosPacket.PAXOS_ID);
				PaxosInstanceStateMachine pism = pinstances.get(paxosID); // exact match (including version) expected here
				if(pism!=null) pism.handlePaxosMessage(jsonMsg);
				else if(MAINTAIN_CORPSES) {
					log.warning("Node "+ this.myID + " received paxos message for non-existent instance " + paxosID);
					PaxosInstanceStateMachine zombie = this.corpses.get(paxosID);
					if(jsonMsg.has(PaxosPacket.PAXOS_VERSION)) {
						short version = (short)jsonMsg.getInt(PaxosPacket.PAXOS_VERSION);
						if(zombie==null || zombie.getVersion()<version) findReplicaGroup(jsonMsg, paxosID, version);
					}
				}
				break;
			}
		} catch(JSONException je) {
			log.severe("Node " + this.myID + " received bad JSON message: " + jsonMsg); je.printStackTrace();
		}
	}

	/* paxosIDNoVersion is the GUID, without the version number denoting the current
	 * paxos group managing the GUID.
	 */
	public String propose(String paxosIDNoVersion, RequestPacket requestPacket) throws JSONException {
		boolean matched=false;
		JSONObject jsonReq = requestPacket.toJSONObject();
		PaxosInstanceStateMachine pism = pinstances.get(paxosIDNoVersion);
		if(pism!=null) {
			matched = true;
			jsonReq.put(PaxosPacket.PAXOS_ID, pism.getPaxosID());
			this.handleIncomingPacket(jsonReq); 
		}
		return matched ? paxosIDNoVersion : null;
	}

	public void resetAll() {
		assert(false) : "This method has not yet been implemented.";
	}

	/********************* End of public methods ***********************/

	/* For each paxosID in the logs, this method creates the corresponding
	 * paxos instance and rolls it forward from the last checkpointed state.
	 */
	private void initiateRecovery() {
		boolean found=false;
		while(this.paxosLogger.initiateReadCheckpoints());
		RecoveryInfo pri=null;
		while((pri = this.paxosLogger.readNextCheckpoint())!=null) {
			int[] group = pri.getMembers();
			String paxosID = pri.getPaxosID();
			if(paxosID!=null) {
				// start paxos instance, restore app state from checkpoint if any and roll forward
				this.recover(paxosID, pri.getVersion(), this.myID, group, app);
				found = true;
			}
		}
		this.paxosLogger.closeReadAll();
		if(!found) {
			log.warning("No checkpoint state found for node " + this.myID +". This can only happen if\n" +
					"(1) the node is newly joining the system, or\n(2) the node previously crashed before " +
					"completing even a single checkpoint, or\n(3) the node's checkpoint was manually deleted.");
		}
	}


	/* All messaging is done using PaxosMessenger and MessagingTask. 
	 * This method 
	 */
	protected void send(MessagingTask mtask)  throws JSONException, IOException {
		if(mtask==null) return;
		if(mtask instanceof LogMessagingTask) {
			AbstractPaxosLogger.logAndMessage(this.paxosLogger, (LogMessagingTask)mtask, this.messenger);
		} else {
			messenger.send(mtask);
		}
	}

	/* synchronized because we want to move the paxos instance atomically
	 * from pinstances to corpses. If not atomic, it can result in the 
	 * corpse (to-be) getting resurrected if a packet for the instance 
	 * arrives in between.
	 */
	protected synchronized void kill(PaxosInstanceStateMachine pism) {
		while(pism.forceStop()) {log.severe("Problem stopping paxos instance "+pism.getPaxosID());}
		this.pinstances.remove(pism.getPaxosID()); 
		this.corpses.put(pism.getPaxosID(), pism);
		timer.schedule(new Cremator(pism.getPaxosID(), this.corpses), MORGUE_DELAY);
	}
	protected void killAndRecoverMe(PaxosInstanceStateMachine pism) {
		if(KILL_AND_RECOVER_ME_OPTION) {
			log.severe("OVERLOAD: Restarting overloaded coordinator node " + this.myID + "of " + pism.getPaxosID());
			pism.forceStop();
			this.pinstances.remove(pism.getPaxosID());
			this.recover(pism.getPaxosID(), pism.getVersion(), pism.getNodeID(), pism.getMembers(), pism.getApp());
		}
	}
	/* Create paxos instance restoring app state from checkpoint if any and roll forward */
	private PaxosInstanceStateMachine recover(String paxosID, short version, int id, int[] members, Replicable app) {
		log.info("Node " +this.myID + " recovering and about to roll forward: " + 
				AbstractPaxosLogger.listToString(this.paxosLogger.getLoggedMessages(paxosID)));
		PaxosInstanceStateMachine pism = this.createPaxosInstance(paxosID, version, id, Util.arrayToSet(members), app);
		/* Note: rollForward can not be done inside the instance as we 
		 * first need to update the instance map here so that networking
		 * (trivially sending message to self) works. That happens in 
		 * createPaxosInstance (after the creation is complete).
		 */
		AbstractPaxosLogger.rollForward(paxosLogger, paxosID, messenger);
		/* After rollForward, recovery is complete. In particular, we don't have 
		 * to wait for any more processing of messages, e.g., out of order decisions
		 * to "settle", because the only important thing is to replay and 
		 * process ACCEPTs and PREPAREs so as to bring the acceptor state (paxos'
		 * memory) up to speed, which is a purely local and non-blocking 
		 * sequence of operations. Coordinator state in general is not 
		 * recoverable; the easiest way to recover it is to simply call
		 * checkRunForCoordinator, which will happen automatically
		 * upon the receipt of any external packet.
		 */
		TESTPaxosConfig.setRecovered(pism.getNodeID(), pism.getPaxosID(), true);
		return pism;
	}

	protected AbstractPaxosLogger getPaxosLogger() {
		return paxosLogger;
	}
	protected boolean isNodeUp(int id) {
		if(FD!=null) return FD.isNodeUp(id);  
		return false; 
	}
	protected boolean lastCoordinatorLongDead(int id) {
		if(FD!=null) return FD.lastCoordinatorLongDead(id);  
		return true;
	}

	/* monitor means start sending the node(s) pings.
	 * We don't expect anything in return. It should 
	 * really be called amAlive instead of monitor.
	 */
	protected void monitor(Set<Integer> nodes) {
		FD.monitor(nodes);
	}
	/* Stop sending pings to the node */
	protected void unMonitor(int id) {
		FD.unMonitor(id);
	}

	private void printLog(String paxosID) {
		System.out.println("State for " + paxosID + ": Checkpoint: " + this.paxosLogger.getStatePacket(paxosID));
		//System.out.println("Logged messages for " + paxosID + ": " + PaxosLogger.listToString(this.paxosLogger.getLoggedMessages(paxosID)));
	}
	// send a request asking for your group
	private void findReplicaGroup(JSONObject msg, String paxosID, short version) throws JSONException {
		FindReplicaGroupPacket findGroup = new FindReplicaGroupPacket(this.myID, msg); // paxosID and version should be within
		int nodeID = FindReplicaGroupPacket.getNodeID(msg);
		if(nodeID >= 0) {
			try {
				this.send(new MessagingTask(nodeID, findGroup));
			} catch(IOException ioe) {ioe.printStackTrace();}
		} else log.severe("Can't find group member in paxosID:version " + paxosID + ":" + version);
	}
	// process a request or send an answer
	private void processFindReplicaGroup(FindReplicaGroupPacket findGroup) throws JSONException {
		MessagingTask mtask = null;
		if(findGroup.group==null && findGroup.nodeID!=this.myID) { // process a request
			PaxosInstanceStateMachine pism = this.pinstances.get(findGroup.getPaxosID());
			if(pism!=null && pism.getVersion()==findGroup.getVersion()) {
				FindReplicaGroupPacket frgReply = new FindReplicaGroupPacket(pism.getMembers(), findGroup);
				mtask = new MessagingTask(findGroup.nodeID, frgReply);
			}
		} else if(findGroup.group!=null && findGroup.nodeID==this.myID) { // process an answer
			PaxosInstanceStateMachine pism = this.pinstances.get(findGroup.getPaxosID());
			if(pism==null || pism.getVersion()<findGroup.getVersion()) {
				// kill lower versions if any and create new paxos instance
				if(pism.getVersion()<findGroup.getVersion()) this.kill(pism);
				this.createPaxosInstance(findGroup.getPaxosID(), findGroup.getVersion(), 
						this.myID, Util.arrayToSet(findGroup.group), app);
			}
		}
		try {
			if(mtask!=null) this.send(mtask);
		} catch(IOException ioe) {ioe.printStackTrace();}
	}

	private class Cremator extends TimerTask {
		String id = null;
		HashMap<String,PaxosInstanceStateMachine> map = null;
		Cremator(String paxosID, HashMap<String,PaxosInstanceStateMachine> zombies) {
			this.id = paxosID;
			this.map = zombies;
		}
		public void run() {
			map.remove(id);
		}
	}
	
	/************************* Testing methods below ***********************************/
	public synchronized void waitRecover() throws InterruptedException {wait();}
	
	public static void main(String[] args) {
		int nNodes = 3;

		TreeSet<Integer> gms = new TreeSet<Integer>();
		int[] members = new int[nNodes];
		for(int i=0; i<nNodes; i++) {
			gms.add(i+100); 
			members[i] = i+100;
		}

		SampleNodeConfig snc = new SampleNodeConfig(2000);
		snc.localSetup(gms);
		JSONMessageExtractor[] jmws = new JSONMessageExtractor[nNodes];
		GNSNIOTransport[] niots = new GNSNIOTransport[nNodes];
		PaxosManager[] pms = new PaxosManager[nNodes];
		DefaultPaxosInterfaceApp[] apps = new DefaultPaxosInterfaceApp[nNodes];
		TESTPaxosConfig.crash(members[0]);


		try {
			for(int i=0; i<nNodes; i++) {
				System.out.println("Testing: initiating node " + i);
				jmws[i] = new JSONMessageExtractor(new DefaultPacketDemultiplexer());
				niots[i] = new GNSNIOTransport(members[i], snc, jmws[i]);
				new Thread(niots[i]).start();

				for(int j=0; j<apps.length; j++) apps[i] = new DefaultPaxosInterfaceApp();
				pms[i] = new PaxosManager(members[i], snc, niots[i], apps[i], null);
				if(!TESTPaxosConfig.isCrashed(members[i])) pms[i].monitor(gms);
			}

			System.out.println("Initiated all " + nNodes + " paxos managers with failure detectors, sleeping for a few seconds..\n");
			Thread.sleep(1000);

			for(int i=0; i<nNodes; i++) {
				for(int j=0; j<nNodes; j++) {
					if(i!=j) {
						System.out.println("Testing: Node " + members[i] + " finds node " + 
					members[j] + " " + (pms[i].isNodeUp(members[j]) ? "up":"down"));
					}
				}
			}

			// We don't really test with multiple groups as they are independent, but this is useful for memory testing
			int paxosGroups=2; 

			System.out.println("\nTesting: Creating " + paxosGroups + " paxos groups each with " + nNodes + 
					" members each, one each at each of the " + nNodes + " nodes");
			PaxosInstanceStateMachine[] pisms = new PaxosInstanceStateMachine[nNodes*paxosGroups];
			for(int i=0; i<nNodes; i++) {
				int k=1;
				for(int j=0; j<paxosGroups; j++) {
					pisms[j*nNodes + i] = pms[i].createPaxosInstance("paxos"+j, (short)0, members[i], gms, apps[i]);
					if(paxosGroups>1000 && (j%k==0 || j%100000==0)) {System.out.print(j+" "); k*=2;}
				}
				for(int m=1; m<nNodes; m++) {
					pms[0].unMonitor(m);
				}
				System.out.println("");
			}

			System.out.println("\nTesting: Finished creating " + paxosGroups + " paxos groups each with " + nNodes + 
					" members each, one each at each of the " + nNodes + " nodes");
			Thread.sleep(1000);
			/*********** Finished creating paxos instances for testing *****************/

			ScheduledExecutorService execpool = Executors.newScheduledThreadPool(5);
			class ClientRequestTask implements Runnable {
				private final RequestPacket request;
				private final PaxosManager paxosManager;
				ClientRequestTask(RequestPacket req, PaxosManager pm) {
					request = req;
					paxosManager = pm;
				}
				public void run() {
					try {
						JSONObject reqJson =  request.toJSONObject();
						Packet.putPacketType(reqJson, PacketType.PAXOS_PACKET);
						paxosManager.propose(request.getPaxosID(), request);
					} catch(JSONException e) {
						e.printStackTrace();
					}
				}
			}
			int numReqs=1000;
			RequestPacket[] reqs = new RequestPacket[numReqs];
			ScheduledFuture<ClientRequestTask>[] futures = new ScheduledFuture[numReqs];
			String[] GUIDs = new String[paxosGroups];
			for(int i=0; i<GUIDs.length; i++) GUIDs[i] = "paxos"+i;

			for(int i=0;i<nNodes; i++) {
				while(!TESTPaxosConfig.isCrashed(members[i]) && !TESTPaxosConfig.getRecovered(members[i], GUIDs[0])) {
					log.info("Waiting for " + members[i] + " to recover ");
					Thread.sleep(1000);
				}
				log.info("Node "+members[i] + " finished recovery including rollback.");
			}
			
			int numExceptions=0;
			double scheduledDelay=0; 
			for(int i=0; i<numReqs; i++) {
				reqs[i] = new RequestPacket(i, i, "[ Sample write request numbered " + i + " ]", false);
				reqs[i].putPaxosID(GUIDs[0], (short)0);
				JSONObject reqJson =  reqs[i].toJSONObject();
				Packet.putPacketType(reqJson, PacketType.PAXOS_PACKET);
				try {
					ClientRequestTask crtask = new ClientRequestTask(reqs[i], pms[1]);
					futures[i] = (ScheduledFuture<ClientRequestTask>)execpool.schedule(crtask, (long)scheduledDelay, TimeUnit.MILLISECONDS);
					scheduledDelay += 0.4;
				} catch(Exception e) {
					e.printStackTrace();
					continue; 
				}
			}
			log.info("Waiting for future gets");
			for(int i=0; i<numReqs; i++) {
				try {
					futures[i].get();
				} catch(Exception e) {
					e.printStackTrace();
					numExceptions++;
				}
			}
			log.info("All futures finished; numExceptions="+numExceptions);
			Thread.sleep(1000);
			
			for(int i=0; i<nNodes; i++) {
				while(!TESTPaxosConfig.isCrashed(members[i]) && apps[i].getNumExecuted(GUIDs[0]) < numReqs) 
					Thread.sleep(1000);
			}
			
			int numCommitted=0;
			for(int i=0; i<nNodes; i++) {
				for(int j=i+1; j<nNodes; j++) {
					if(!TESTPaxosConfig.isCrashed(members[i]) && !TESTPaxosConfig.isCrashed(members[j])) {
						int committed1 = apps[i].getNumCommitted(GUIDs[0]);
						int committed2 = apps[j].getNumCommitted(GUIDs[0]);
						int diff = committed1-committed2; if(diff<0) diff = -diff;
						while(committed1!=committed2) {
							(committed1>committed2 ? apps[j] : apps[i]).waitToFinish();
							log.info("Waiting : (slot,hash1)=(" +committed1 +","+
									apps[i].getHash(GUIDs[0])+"(; (slot2,hash2="+committed2+","+apps[j].getHash(GUIDs[0])+")");
							Thread.sleep(1000);
						}
						assert(committed1==committed2) : 
							"numCommitted@" + i + "="+committed1 + ", numCommitted@" + j +"="+committed2;
						numCommitted = apps[i].getNumCommitted(GUIDs[0]);
						assert(apps[i].getHash(GUIDs[0]) == apps[j].getHash(GUIDs[0])); // SMR invariant
					}
				}
			}
			String preemptedReqs = "[ ";
			int numPreempted=0;
			for(int i=0; i<numReqs; i++) {
				if(!TESTPaxosConfig.isCommitted(i)){
					preemptedReqs += (i + " ");
					numPreempted++;
				}
			}
			preemptedReqs += "]";

			DecimalFormat df = new DecimalFormat();
			df.setMaximumFractionDigits(2);

			System.out.println("\n\nTest completed. Executed " + numCommitted + " requests consistently including " + 
					(numReqs-numPreempted) + " of " + numReqs + " received requests;\nPreempted requests = " + preemptedReqs + 
					"; numExceptions="+ numExceptions+"; average message log time="+df.format(PaxosLogTask.getAvgLogTime())+"ms.\n"+
					"\nNote that it is possible for the test to be successful even if the number of consistently\n" +
					"executed requests is less than the number of received requests as paxos only guarantees\n" +
					"consistency, i.e., that all replicas executed requests in the same order, not that all requests\n" +
					"issued will get executed. The latter property can be achieved by clients reissuing requests\n" +
					"until successfully executed. With reissuals, clients do need to worry about double execution,\n" +
					"so they should be careful. A client is not guaranteed to get a failure message if the request fails,\n" +
					"e.g., if the replica receiving a request dies immediately. If the client uses a timeout to detect\n" +
					"failure and thereupon reissue its request, it is possible that both the original and re-issued\n" +
					"requests are executed. Clients can get around this problem by using sequence numbers within\n" +
					"their app, reading the current sequence number, and then trying to commit their write provided the\n" +
					"sequence number has not changed in the meantime. There are other alternatives, but all of these\n" +
					"are application-specific; they are not paxos's problem\n");
			for(int i=0; i<nNodes;i++) {
				pms[i].printLog(GUIDs[0]);
			} System.out.println("");
			System.exit(1);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
