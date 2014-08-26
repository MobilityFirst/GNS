package edu.umass.cs.gns.reconfigurator;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.PacketTypeStamper;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.paxos.AbstractPaxosManager;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.replicaCoordination.ReplicaControllerCoordinator;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;

/**
 * @author V. Arun
 * Based on code created by abhigyan on 3/30/14.
 * 
 * Coordinates requests among replicas of replica controllers by using paxos.
 * 
 */
public class ReplicaControllerCoordinatorPaxos implements ReplicaControllerCoordinator {
  private static long HANDLE_DECISION_RETRY_INTERVAL_MILLIS = 1000;
	private final int myID;
	private final AbstractPaxosManager paxosManager;
	private final Replicable replicable;

	private Logger log = Logger.getLogger(getClass().getName()); //log;

	public ReplicaControllerCoordinatorPaxos(int nodeID, InterfaceJSONNIOTransport nioServer, InterfaceNodeConfig nodeConfig,
			Replicable paxosInterface, PaxosConfig paxosConfig) {
		this.myID = nodeID;
		this.replicable = paxosInterface;
		if (Config.multiPaxos) {
			assert false: "Not working yet. Known Issue: we need to fix packet demultiplexing";
		this.paxosManager = new edu.umass.cs.gns.replicaCoordination.multipaxos.PaxosManager(nodeID, nodeConfig,
				(JSONNIOTransport) nioServer, paxosInterface, paxosConfig);
		} else {
			paxosConfig.setConsistentHashCoordinatorOrder(true);
			this.paxosManager = new PaxosManager(nodeID, nodeConfig,
					new PacketTypeStamper(nioServer, Packet.PacketType.REPLICA_CONTROLLER_COORDINATION),
					paxosInterface, paxosConfig);
		}
		createPrimaryPaxosInstances();
	}
	private void createPrimaryPaxosInstances() { // called by constructor above
		HashMap<String, Set<Integer>> groupIDsMembers = ConsistentHashing.getReplicaControllerGroupIDsForNode(myID);
		for (String groupID : groupIDsMembers.keySet()) {
			log.info("Creating paxos instances: " + groupID + "\t" + groupIDsMembers.get(groupID));
			paxosManager.createPaxosInstance(groupID, (short) 1, groupIDsMembers.get(groupID), replicable);
		}
	}

	@Override
	public int coordinateRequest(JSONObject request) {
		if (this.replicable == null) return -1; // replicable app not set
		try {
			Packet.PacketType type = Packet.getPacketType(request);
			if (!type.equals(Packet.PacketType.REPLICA_CONTROLLER_COORDINATION)) {
				log.info(" ReplicaController received msg: " + request);
			}
			switch (type) {
			// packets from coordination modules at replica controller
			case REPLICA_CONTROLLER_COORDINATION:
				Packet.putPacketType(request, Packet.PacketType.PAXOS_PACKET); // paxos is agnostic to what it is being used for
				paxosManager.handleIncomingPacket(request); 
				break;
			case ADD_RECORD:
				AddRecordPacket recordPacket = new AddRecordPacket(request);
				recordPacket.setNameServerID(myID);
				paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(recordPacket.getName()), recordPacket.toString());
				break;
			case REMOVE_RECORD:
				RemoveRecordPacket removePacket = new RemoveRecordPacket(request);
				removePacket.setNameServerID(myID);
				paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(removePacket.getName()), removePacket.toString());
				break;
				// Packets sent from active replica
			case RC_REMOVE:
				removePacket = new RemoveRecordPacket(request);
				paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(removePacket.getName()), removePacket.toString());
				break;
			case NEW_ACTIVE_PROPOSE:
				NewActiveProposalPacket activePropose = new NewActiveProposalPacket(request);
				paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(activePropose.getName()), activePropose.toString());
				break;
			case GROUP_CHANGE_COMPLETE:
				GroupChangeCompletePacket groupChangePkt = new GroupChangeCompletePacket(request);
				paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(groupChangePkt.getName()), groupChangePkt.toString());
				break;
			case REQUEST_ACTIVES:
			case NAMESERVER_SELECTION:
			case NAME_RECORD_STATS_RESPONSE:
			case ACTIVE_ADD_CONFIRM:
			case ACTIVE_REMOVE_CONFIRM:
			case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY:
			case NEW_ACTIVE_START_CONFIRM_TO_PRIMARY:
			case NAME_SERVER_LOAD:
				// no coordination needed for these packet types.
        callHandleDecisionWithRetry(null, request.toString(), false);
				break;
			default:
				log.severe("Packet type not found in coordination: " + type);
				break;
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int initGroupChange(String name) {
		return 0; // FIXME: What is the point of this?
	}

	@Override
	public void reset() {
		paxosManager.resetAll();
		createPrimaryPaxosInstances(); // FIXME: Check if this could cause problems after resetAll()
	}

  /**
   * Retries a request at period interval until successfully executed by application.
   */
  private void callHandleDecisionWithRetry(String name, String value, boolean doNotReplyToClient) {
    while (!replicable.handleDecision(name, value, doNotReplyToClient)) {
      try {
        Thread.sleep(HANDLE_DECISION_RETRY_INTERVAL_MILLIS);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      log.severe("Failed to execute decision. Retry. name = " + name + " value = " + value);
    }
  }

  public static void main(String[] args) {
		int id = 100;
		int faultTolerance = 3;
		HashMap<String,String> configParameters = new HashMap<String,String>();
		GNSNodeConfig gnsNodeConfig = GNSNodeConfig.CreateGNSNodeConfigFromOldStyleFile(Config.ARUN_GNS_DIR_PATH+"/conf/testCodeResources/name-server-info", 100);
		PaxosConfig paxosConfig = new PaxosConfig();
		try {
			JSONNIOTransport niot = new JSONNIOTransport(id, gnsNodeConfig, new JSONMessageExtractor(new PacketDemultiplexerDefault())); 
			MongoRecords mongoRecords = new MongoRecords(id, Config.mongoPort);
			ReplicaController rc = new ReplicaController(id, configParameters, gnsNodeConfig, niot, mongoRecords);
			ConsistentHashing.initialize(faultTolerance, gnsNodeConfig.getNameServerIDs());
			ReplicaControllerCoordinatorPaxos rccPaxos = new ReplicaControllerCoordinatorPaxos(id, niot, gnsNodeConfig, rc, paxosConfig);
			System.out.println("SUCCESS: ReplicaControllerCoordinatorPaxos " + rccPaxos.myID + " started without exceptions." +
					"\nNothing else except startup has been tested yet.");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

  @Override
  public void shutdown() {

  }
}
