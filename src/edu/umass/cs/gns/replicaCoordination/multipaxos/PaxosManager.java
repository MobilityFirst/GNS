package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.DefaultPacketDemultiplexer;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageWorker;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.nio.SampleNodeConfig;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.packet.PaxosPacket;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;

/**
@author V. Arun
 */

/* There is one paxos manager per machine. This class could 
 * be static, but it is not so that we can test emulations 
 * involving multiple "machines" within a JVM.
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

	// final
	private final PaxosLogger paxosLogger;  // logging
	private final FailureDetection FD;  // failure detection
	private final PaxosMessenger messenger;  // messaging
	
	// non-final
	private HashMap<String,PaxosInstanceStateMachine> pinstances=null; // paxos instance mapping

	/* Note: PaxosManager itself maintains no NIO transport
	 * instance as it delegates all communication
	 * related activities to other objects. PaxosManager
	 * is only responsible for managing state for and 
	 * demultiplexing incoming packets to a number of 
	 * paxos instances at this node.
	 */
	
	PaxosManager(int id, NodeConfig nc, GNSNIOTransport niot) {
		paxosLogger = new PaxosLogger();
		FD = new FailureDetection(id, nc, niot);
		pinstances = new HashMap<String,PaxosInstanceStateMachine>();
		messenger = new PaxosMessenger(niot);
	}

	public PaxosInstanceStateMachine createPaxosInstance(String paxosID, int id, Set<Integer> gms, PaxosInterface app) {
		PaxosInstanceStateMachine pism = new PaxosInstanceStateMachine(paxosID, id, gms, app, this);
		pinstances.put(paxosID, pism);
		this.FD.monitor(gms);
		return pism;
	}

	public void handleIncomingPacket(JSONObject jsonMsg) throws JSONException {
		int incomingPacketType;
		assert(jsonMsg.getInt("type")==PacketType.PAXOS_PACKET.getInt());

		incomingPacketType = jsonMsg.getInt(PaxosPacketType.ptype);
		
		switch (incomingPacketType){
		case PaxosPacketType.FAILURE_DETECT:
			FailureDetectionPacket fdp = new FailureDetectionPacket(jsonMsg);
			FD.receive(fdp);
			break;
		default:
			String paxosID = jsonMsg.getString(PaxosPacket.paxosIDKey);
			pinstances.get(paxosID).handlePaxosMessage(jsonMsg);
			break;
		}
	}
	/********************* End of public methods ***********************/

	/* All messaging is done using PaxosMessenger and MessagingTask. 
	 * This method 
	 */
	protected void send(MessagingTask mtask)  throws JSONException, IOException {
		messenger.send(mtask);
	}

	protected PaxosLogger getPaxosLogger() {
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
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	    ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINEST);
        Logger log = Logger.getLogger(NIOTransport.class.getName()); 
        log.addHandler(handler);
	    log.setLevel(Level.INFO);

		int nNodes = 3;

		TreeSet<Integer> gms = new TreeSet<Integer>();
		int[] members = new int[nNodes];
		for(int i=0; i<nNodes; i++) {
			gms.add(i+100); 
			members[i] = i+100;
		}

		SampleNodeConfig snc = new SampleNodeConfig(2000);
		snc.localSetup(gms);
		JSONMessageWorker[] jmws = new JSONMessageWorker[nNodes];
		GNSNIOTransport[] niots = new GNSNIOTransport[nNodes];
		PaxosManager[] pms = new PaxosManager[nNodes];

	
		try {
			for(int i=0; i<nNodes; i++) {
				System.out.println("Initiating node " + i);
				jmws[i] = new JSONMessageWorker(new DefaultPacketDemultiplexer());
				niots[i] = new GNSNIOTransport(members[i], snc, jmws[i]);
				new Thread(niots[i]).start();
				pms[i] = new PaxosManager(members[i], snc, niots[i]);
				jmws[i].setMessageWorker(new PaxosPacketDemultiplexer(pms[i]));
				
				//pms[i].monitor(gms);
				if(i!=0) pms[i].monitor(gms);
			}

			System.out.println("Initiated all " + nNodes + " paxos managers with failure detectors, sleeping for a few seconds..\n");
			Thread.sleep(1000);

			for(int i=0; i<nNodes; i++) {
				for(int j=0; j<nNodes; j++) {
					if(i!=j) {
						System.out.println("Node " + members[i] + " finds node " + members[j] + " " + (pms[i].isNodeUp(members[j]) ? "up":"down"));
					}
				}
			}
			
			// We don't really test with multiple groups as they are independent, but this is useful for memory testing
			int paxosGroups=2; 
			
			System.out.println("\nCreating " + paxosGroups + " paxos groups each with " + nNodes + " members each, one each at each of the " + nNodes + " nodes");
			DefaultPaxosInterfaceApp[] apps = new DefaultPaxosInterfaceApp[nNodes*paxosGroups];
			for(int i=0; i<apps.length; i++) apps[i] = new DefaultPaxosInterfaceApp();
			PaxosInstanceStateMachine[] pisms = new PaxosInstanceStateMachine[nNodes*paxosGroups];
			for(int i=0; i<nNodes; i++) {
				for(int j=0; j<paxosGroups; j++) {
					pisms[j*nNodes + i] = pms[i].createPaxosInstance("paxos"+j, members[i], gms, apps[j*nNodes + i]);
				}
				for(int k=1; k<nNodes; k++) {
					pms[0].unMonitor(k);
				}
			}

			System.out.println("\nFinished creating " + paxosGroups + " paxos groups each with " + nNodes + " members each, one each at each of the " + nNodes + " nodes");
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
					paxosManager.handleIncomingPacket(reqJson);
					} catch(JSONException e) {
						e.printStackTrace();
					}
				}
			}
			int numReqs=1000;
			RequestPacket[] reqs = new RequestPacket[numReqs];
			ScheduledFuture<ClientRequestTask>[] futures = new ScheduledFuture[numReqs];
			int numExceptions=0;
			for(int i=0; i<numReqs; i++) {
				reqs[i] = new RequestPacket(i, "[ Sample write request numbered " + i + " ]", PaxosPacketType.REQUEST, false);
				reqs[i].putPaxosID("paxos"+0);
				JSONObject reqJson =  reqs[i].toJSONObject();
				Packet.putPacketType(reqJson, PacketType.PAXOS_PACKET);
				//pisms[0].handlePaxosMessage(reqJson);
				try {
					//pms[1].handleIncomingPacket(reqJson); // round-robin across nodes
					ClientRequestTask crtask = new ClientRequestTask(reqs[i], pms[1]);
					futures[i] = (ScheduledFuture<ClientRequestTask>)execpool.schedule(crtask, 0, TimeUnit.MILLISECONDS);
				} catch(Exception e) {
					e.printStackTrace();
					numExceptions++;
					continue; 
				}
			}
			for(int i=0; i<numReqs; i++) {
				futures[i].get();
			}
			Thread.sleep(5000);

			System.out.println("\n\nTest completed. Currently unable to produce an automated success/failure message.\n" +
			"Number of exceptions (including assert violations) = " + numExceptions+".");
			System.exit(1);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
