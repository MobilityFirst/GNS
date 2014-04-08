package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.GNSNIOTransportInterface;
import edu.umass.cs.gns.nio.NodeConfig;
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

/**
 * Created by abhigyan on 3/30/14.
 */
public class ReplicaControllerCoordinatorPaxos implements ReplicaControllerCoordinator{
  private int nodeID;
  private AbstractPaxosManager paxosManager;

  private Replicable paxosInterface;

  public ReplicaControllerCoordinatorPaxos(int nodeID, GNSNIOTransportInterface nioServer, NodeConfig nodeConfig,
                                           Replicable paxosInterface, PaxosConfig paxosConfig) {
    this.nodeID = nodeID;
    this.paxosInterface = paxosInterface;
    this.paxosManager = new PaxosManager(nodeID, nodeConfig,
            new PacketTypeStamper(nioServer, Packet.PacketType.REPLICA_CONTROLLER_COORDINATION), paxosInterface, paxosConfig);
    createPrimaryPaxosInstances();
  }

  private void createPrimaryPaxosInstances() {
    HashMap<String, Set<Integer>> groupIDsMembers = ConsistentHashing.getReplicaControllerGroupIDsForNode(nodeID);

    for (String groupID : groupIDsMembers.keySet()) {
      GNS.getLogger().info("Creating paxos instances: " + groupID + "\t" + groupIDsMembers.get(groupID));
      paxosManager.createPaxosInstance(groupID, 1, groupIDsMembers.get(groupID), paxosInterface);
    }

  }

  @Override
  public int coordinateRequest(JSONObject request) {
      if (this.paxosInterface == null) return -1; // replicable app not set
      try {
        Packet.PacketType type = Packet.getPacketType(request);
        switch (type) {
          // packets from coordination modules at replica controller
          case REPLICA_CONTROLLER_COORDINATION:
            paxosManager.handleIncomingPacket(request);
            break;
          case ADD_RECORD:
            AddRecordPacket recordPacket = new AddRecordPacket(request);
            paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(recordPacket.getName()), request.toString(), false);
            break;
          case REMOVE_RECORD:
            RemoveRecordPacket removePacket = new RemoveRecordPacket(request);
            paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(removePacket.getName()), request.toString(), false);
            break;
            // Packets sent from active replica
          case RC_REMOVE:
            removePacket = new RemoveRecordPacket(request);
            paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(removePacket.getName()), request.toString(), false);
            break;
          case NEW_ACTIVE_PROPOSE:
            NewActiveProposalPacket activePropose = new NewActiveProposalPacket(request);
            paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(activePropose.getName()), request.toString(), false);
            break;
          case GROUP_CHANGE_COMPLETE:
            GroupChangeCompletePacket startupPacket = new GroupChangeCompletePacket(request);
            paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(startupPacket.getName()), request.toString(), false);
            break;
          case REQUEST_ACTIVES:
          case NAMESERVER_SELECTION:
          case NAME_RECORD_STATS_RESPONSE:
          case ACTIVE_ADD_CONFIRM:
          case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY:
            // no coordination needed for these packet types.
            paxosInterface.handleDecision(null, request.toString(), false);
            break;
          default:
            GNS.getLogger().severe("Packet type not found in coordination: " + type);
            break;
        }
      } catch (JSONException e) {
        e.printStackTrace();
      }
      return 0;
  }

  @Override
  public int initGroupChange(String name) {
    return 0;
  }

  @Override
  public void reset() {
    paxosManager.resetAll();
    createPrimaryPaxosInstances();
  }
}
