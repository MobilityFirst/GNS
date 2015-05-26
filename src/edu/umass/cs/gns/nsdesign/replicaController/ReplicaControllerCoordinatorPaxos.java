package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.gigapaxos.deprecated.AbstractPaxosManager;
import edu.umass.cs.gns.gigapaxos.deprecated.Replicable;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.PacketTypeStampAndSend;
import edu.umass.cs.gns.newApp.packet.AddRecordPacket;
import edu.umass.cs.gns.newApp.packet.deprecated.GroupChangeCompletePacket;
import edu.umass.cs.gns.newApp.packet.deprecated.NewActiveProposalPacket;
import edu.umass.cs.gns.newApp.packet.Packet;
import edu.umass.cs.gns.newApp.packet.RemoveRecordPacket;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.replicaCoordination.ReplicaControllerCoordinator;
//import edu.umass.cs.gns.util.ConsistentHashing;
import java.io.IOException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;

/**
 * Coordinates requests among replicas of replica controllers by using paxos consensus protocol.
 * Created by abhigyan on 3/30/14.
 */
@Deprecated
public class ReplicaControllerCoordinatorPaxos<NodeIDType> extends AbstractReplicaCoordinator implements ReplicaControllerCoordinator {

  private static final long HANDLE_DECISION_RETRY_INTERVAL_MILLIS = 1000;

  private final NodeIDType nodeID;
  //private final InterfaceNodeConfig<NodeIdType> nodeConfig;
  private final ConsistentReconfigurableNodeConfig<NodeIDType> nodeConfig;
  private final AbstractPaxosManager<NodeIDType> paxosManager;

  private final Replicable paxosInterface;

  public ReplicaControllerCoordinatorPaxos(NodeIDType nodeID, InterfaceJSONNIOTransport<NodeIDType> nioServer,
          InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig,
          Replicable paxosInterface, PaxosConfig paxosConfig) {
    super(paxosInterface);
    this.nodeID = nodeID;
    this.nodeConfig = new ConsistentReconfigurableNodeConfig(nodeConfig);

    if (!Config.useOldPaxos) {
      GNS.getLogger().info("Using gigapaxos");
      this.paxosInterface = paxosInterface;
      this.paxosManager = new edu.umass.cs.gns.gigapaxos.PaxosManager<NodeIDType>(nodeID, nodeConfig,
              new PacketTypeStampAndSend<NodeIDType>(nioServer, Packet.PacketType.REPLICA_CONTROLLER_COORDINATION),
              this.paxosInterface, paxosConfig.getPaxosLogFolder());
    } else {
      GNS.getLogger().info("Using old Paxos (not gigapaxos)");
      this.paxosInterface = paxosInterface;
      paxosConfig.setConsistentHashCoordinatorOrder(true);
      this.paxosManager = new PaxosManager<NodeIDType>(nodeID, nodeConfig,
              new PacketTypeStampAndSend<NodeIDType>(nioServer, Packet.PacketType.REPLICA_CONTROLLER_COORDINATION),
              this.paxosInterface, paxosConfig);
    }
    createPrimaryPaxosInstances();
  }

  private void createPrimaryPaxosInstances() {
//    HashMap<String, Set> groupIDsMembers = ConsistentHashing.getReplicaControllerGroupIDsForNode(nodeID);
//    for (String groupID : groupIDsMembers.keySet()) {
//      GNS.getLogger().info("Creating paxos instances: " + groupID + "\t" + groupIDsMembers.get(groupID));
//      paxosManager.createPaxosInstance(groupID, Config.FIRST_VERSION, groupIDsMembers.get(groupID), paxosInterface);
//    }
    Set<NodeIDType> reconfigurators = this.nodeConfig.getReconfigurators();
    // iterate over all nodes
    for (NodeIDType node : reconfigurators) {
      Set<NodeIDType> group = this.nodeConfig.getReplicatedReconfigurators(node.toString());
      if (group.contains(this.nodeID)) {
        GNS.getLogger().info("Creating paxos instances: " + node.toString() + "\t" + group);
        paxosManager.createPaxosInstance(node.toString(), Config.FIRST_VERSION, group, paxosInterface);
      }
    }
  }

  @Override
  public int coordinateRequest(JSONObject request) {

    if (this.paxosInterface == null) {
      return -1; // replicable app not set
    }
    try {
      Packet.PacketType type = Packet.getPacketType(request);
      // Strictly for debugging
      if (Config.debuggingEnabled) {
        int packetTypeInt = request.optInt(edu.umass.cs.gns.paxos.paxospacket.PaxosPacket.PACKET_TYPE_FIELD_NAME, -1);
        if (packetTypeInt != -1) {
          GNS.getLogger().info("###### " + nodeID + " Request: " + type
                  + " PaxosType: " + edu.umass.cs.gns.paxos.paxospacket.PaxosPacketType.getPacketType(packetTypeInt));
        }
        packetTypeInt = request.optInt(edu.umass.cs.gns.gigapaxos.multipaxospacket.PaxosPacket.PAXOS_PACKET_TYPE);
        if (packetTypeInt != -1) {
          GNS.getLogger().info("###### " + nodeID + " Request: " + type
                  + " GigaPaxosType: " + edu.umass.cs.gns.gigapaxos.multipaxospacket.PaxosPacket.PaxosPacketType.getPaxosPacketType(packetTypeInt));
        }
      }
      switch (type) {
        // packets from coordination modules at replica controller
        case REPLICA_CONTROLLER_COORDINATION:

          Packet.putPacketType(request, Packet.PacketType.PAXOS_PACKET);
          paxosManager.handleIncomingPacket(request);
          break;
        case ADD_RECORD:
          AddRecordPacket<NodeIDType> addPacket = new AddRecordPacket<NodeIDType>(request, nodeConfig);
          addPacket.setNameServerID(nodeID);
          // FIXME: HACK ALERT : GIGAPAXIS TRANSITION UNFINISHED
//          if (!Config.useOldPaxos) {
//            edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket requestPacket
//                    = new edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket(-1, addPacket.toString(), false);
//            requestPacket.setReturnRequestValue();
//            paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(addPacket.getName()), requestPacket.toString());
//          } else {
          //paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(addPacket.getName()), addPacket.toString());
          paxosManager.propose((String) nodeConfig.getReconfiguratorHash(addPacket.getName()), addPacket.toString());
          //}
          break;
        case REMOVE_RECORD:
          RemoveRecordPacket<NodeIDType> removePacket = new RemoveRecordPacket<NodeIDType>(request, nodeConfig);
          removePacket.setNameServerID(nodeID);
          // FIXME: HACK ALERT : GIGAPAXIS TRANSITION UNFINISHED
//          if (!Config.useOldPaxos) {
//            edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket requestPacket
//                    = new edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket(-1, removePacket.toString(), false);
//            requestPacket.setReturnRequestValue();
//            paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(removePacket.getName()), requestPacket.toString());
//          } else {
          paxosManager.propose((String) nodeConfig.getReconfiguratorHash(removePacket.getName()), removePacket.toString());
          //paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(removePacket.getName()), removePacket.toString());
          //}
          break;
        // Packets sent from active replica
        case RC_REMOVE:
          removePacket = new RemoveRecordPacket<NodeIDType>(request, nodeConfig);
          // FIXME: HACK ALERT : GIGAPAXIS TRANSITION UNFINISHED
//          if (!Config.useOldPaxos) {
//            edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket requestPacket
//                    = new edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket(-1, removePacket.toString(), false);
//            paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(removePacket.getName()), requestPacket.toString());
//          } else {
          paxosManager.propose((String)nodeConfig.getReconfiguratorHash(removePacket.getName()), removePacket.toString());
          //paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(removePacket.getName()), removePacket.toString());
          //}
          break;
        case NEW_ACTIVE_PROPOSE:
          NewActiveProposalPacket<NodeIDType> activePropose = new NewActiveProposalPacket<NodeIDType>(request, nodeConfig);
          paxosManager.propose((String)nodeConfig.getReconfiguratorHash(activePropose.getName()), activePropose.toString());
          //paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(activePropose.getName()), activePropose.toString());
          break;
        case GROUP_CHANGE_COMPLETE:
          GroupChangeCompletePacket groupChangePkt = new GroupChangeCompletePacket(request);
          paxosManager.propose((String)nodeConfig.getReconfiguratorHash(groupChangePkt.getName()), groupChangePkt.toString());
          //paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(groupChangePkt.getName()), groupChangePkt.toString());
          break;
        case REQUEST_ACTIVES:
        case NAMESERVER_SELECTION:
        //case NAME_RECORD_STATS_RESPONSE:
        case ACTIVE_ADD_CONFIRM:
        case ACTIVE_REMOVE_CONFIRM:
        case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY:
        case NEW_ACTIVE_START_CONFIRM_TO_PRIMARY:
        case NAME_SERVER_LOAD:
          // no coordination needed for these packet types.
          callHandleDecisionWithRetry(null, request.toString(), false);
          break;
        default:
          GNS.getLogger().severe("Packet type not found in coordination: " + type);
          return -1;
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return 0;
  }

  /**
   * Retries a request at period interval until successfully executed by application.
   */
  private void callHandleDecisionWithRetry(String name, String value, boolean doNotReplyToClient) {
    while (!paxosInterface.handleDecision(name, value, doNotReplyToClient)) {
      try {
        Thread.sleep(HANDLE_DECISION_RETRY_INTERVAL_MILLIS);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      GNS.getLogger().severe("Failed to execute decision. Retry. name = " + name + " value = " + value);
    }
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

  @Override
  public void shutdown() {
    // todo how to shutdown multipaxos's PaxosManager?
    if (paxosManager instanceof PaxosManager) {
      ((PaxosManager) paxosManager).shutdown();
    }
  }

  // For ReplicaControllerCoordinator
  @Override
  public boolean coordinateRequest(InterfaceRequest request) throws IOException, RequestParseException {
    try {
      // Who uses ints for boolean status return values?
      return coordinateRequest(new JSONObject(request.toString())) == 0;
    } catch (JSONException e) {
      throw new RequestParseException(e);
    }
  }

  @Override
  public boolean createReplicaGroup(String serviceName, int epoch, String state, Set nodes) {
    return paxosManager.createPaxosInstance(serviceName, (short) epoch, nodes, paxosInterface);
  }

  @Override
  public Set getReplicaGroup(String serviceName) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void deleteReplicaGroup(String serviceName, int epoch) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}
