package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.PacketTypeStamper;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.paxos.AbstractPaxosManager;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Random;
import java.util.Set;

/**
 * Used only for running an experiment for paper. Not used by GNS system. The code may not completely implement
 * all functionality of a GNS coordinator.
 *
 * Unlike GnsPaxosCoordinator, where a paxos group is made for each name, this coordinator maintains a fixed
 * set of paxos groups across the system, and maps a given name to an existing paxos group. This coordinator
 * assumes a fixed replication in system. In this coordinator, the membership of paxos groups is determined
 * based on consistent hash of node IDs. These paxos groups mirror the paxos groups among replica controllers.
 * Names are mapped to paxos groups based on its consistent hash.
 *
 * Created by abhigyan on 4/27/14.
 */
public class StaticReplicationCoordinator<NodeIDType> extends ActiveReplicaCoordinator{

  private NodeIDType nodeID;
  // this is the app object
  private Replicable paxosInterface;

  private AbstractPaxosManager paxosManager;

  // if true, reads are coordinated as well.
  private boolean readCoordination = false;

  public StaticReplicationCoordinator(NodeIDType nodeID, InterfaceJSONNIOTransport nioServer, InterfaceNodeConfig nodeConfig,
                                      Replicable paxosInterface, PaxosConfig paxosConfig, boolean readCoordination) {
    this.nodeID = nodeID;
    this.paxosInterface = paxosInterface;
    this.readCoordination = readCoordination;
    paxosConfig.setConsistentHashCoordinatorOrder(true);
    this.paxosManager = new PaxosManager(nodeID, nodeConfig,
            new PacketTypeStamper(nioServer, Packet.PacketType.ACTIVE_COORDINATION), paxosInterface, paxosConfig);
    createNodePaxosInstances();
  }



  private void createNodePaxosInstances() {
    HashMap<String, Set> groupIDsMembers = ConsistentHashing.getReplicaControllerGroupIDsForNode(nodeID);
    for (String groupID : groupIDsMembers.keySet()) {
      GNS.getLogger().info("Creating paxos instances: " + groupID + "\t" + groupIDsMembers.get(groupID));
      paxosManager.createPaxosInstance(groupID, (short) 1, groupIDsMembers.get(groupID), paxosInterface);
    }
  }

  @Override
  public int coordinateRequest(JSONObject request) {
    if (this.paxosInterface == null) return -1; // replicable app not set
    JSONObject callHandleDecision = null;
    boolean noCoordinatorState = false;
    try {
      Packet.PacketType type = Packet.getPacketType(request);
      switch (type) {
        // coordination packets internal to paxos
        case ACTIVE_COORDINATION:
          paxosManager.handleIncomingPacket(request);
          break;
        // call propose
        case UPDATE: // updates need coordination
          UpdatePacket update = new UpdatePacket(request);
          update.setNameServerID(nodeID);
          Random r = new Random(update.getName().hashCode());
          Set replicaControllers = ConsistentHashing.getReplicaControllerSet(update.getName());
          int selectIndex = r.nextInt(GNS.numPrimaryReplicas);
          int count = 0;
          NodeIDType selectNode = null;
          //int selectNode = 0;
          for (Object x: replicaControllers) {
            if (count == selectIndex) {
              selectNode = (NodeIDType) x;
              break;
            }
            count += 1;
          }
          String proposeToPaxosID = ConsistentHashing.getReplicaControllerGroupID(selectNode.toString());
//          GNS.getLogger().info("Propose to Paxos ID = " + proposeToPaxosID + " select node = " + selectNode);
          String paxosID = paxosManager.propose(proposeToPaxosID, update.toString());
//          GNS.getLogger().info("Propsal reply Paxos ID = " + paxosID);
          if (paxosID == null) {
            callHandleDecision = update.toJSONObject();
            noCoordinatorState = true;
          }
          break;

        // call proposeStop
        case ACTIVE_REMOVE: // stop request for removing a name record
          OldActiveSetStopPacket stopPacket1 = new OldActiveSetStopPacket(request);
          paxosID = paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(stopPacket1.getName()),
                  stopPacket1.toString());
//          GNS.getLogger().info("Proposing remove to paxos ... ");
          if (paxosID == null) {
            callHandleDecision = stopPacket1.toJSONObject();
            noCoordinatorState = true;
          }
          break;
        case OLD_ACTIVE_STOP: // (sent by active replica) stop request on a group change
          // this coordinator assumes a static replication, so group change features are not implemented.
          throw new UnsupportedOperationException();

//          OldActiveSetStopPacket stopPacket2 = new OldActiveSetStopPacket(request);
//          paxosID = paxosManager.proposeStop(stopPacket2.getName(), stopPacket2.toString(), stopPacket2.getVersion());
//          if (paxosID == null) {
//            callHandleDecision = stopPacket2.toJSONObject();
//            noCoordinatorState = true;
//          }
//          break;
        // call createPaxosInstance
        case ACTIVE_ADD:  // createPaxosInstance when name is added for the first time
          // calling handle decision before creating paxos instance to insert state for name in database.
          paxosInterface.handleDecision(null, request.toString(), false);
          break;
        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE: // (sent by active replica) createPaxosInstance after a group change
          throw new UnsupportedOperationException(); // this coordinator does not support group changes

        // no coordination needed for requests below
        case DNS: // todo send latest actives to client with this request.

          if (readCoordination) {
            DNSPacket dnsPacket = new DNSPacket(request);
            if (dnsPacket.isQuery()) {
              dnsPacket.setResponder(nodeID);
              paxosID = paxosManager.propose(ConsistentHashing.getReplicaControllerGroupID(dnsPacket.getGuid()), dnsPacket.toString());
              if (paxosID == null) {
                callHandleDecision = dnsPacket.toJSONObjectQuestion();
                noCoordinatorState = true;
              }
              break;
            }
          }
        case NAME_SERVER_LOAD:
        case SELECT_REQUEST:
        case SELECT_RESPONSE:
        case CONFIRM_UPDATE:
        case CONFIRM_ADD:
        case CONFIRM_REMOVE:
          // Packets sent from replica controller
          callHandleDecision = request;

          break;
        default:
          GNS.getLogger().severe("Packet type not found in coordination: " + type);
          break;
      }
      if (callHandleDecision != null) {
        if (noCoordinatorState) {
          callHandleDecision.put(Config.NO_COORDINATOR_STATE_MARKER, 0);
        }
        paxosInterface.handleDecision(null, callHandleDecision.toString(), false);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return 0;
  }

  @Override
  public void reset() {
    paxosManager.resetAll();
    createNodePaxosInstances();
  }

  @Override
  public void shutdown() {

  }
}
