package edu.umass.cs.gns.nsdesign.activeReconfiguration;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.PacketTypeStamper;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.paxos.AbstractPaxosManager;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * Created by abhigyan on 3/28/14.
 */
public class ActiveReplicaCoordinatorPaxos extends ActiveReplicaCoordinator{

  private int nodeID;
  // this is the app object
  private Replicable paxosInterface;

  private AbstractPaxosManager paxosManager;

  public ActiveReplicaCoordinatorPaxos(int nodeID, GNSNIOTransport nioServer, NodeConfig nodeConfig,
                                       Replicable paxosInterface, PaxosConfig paxosConfig) {
    this.nodeID = nodeID;
    this.paxosInterface = paxosInterface;
    this.paxosManager = new PaxosManager(nodeID, nodeConfig,
            new PacketTypeStamper(nioServer, Packet.PacketType.ACTIVE_COORDINATION), paxosInterface, paxosConfig);
  }

  /**
   * Handles coordination among replicas for a request. Returns -1 in case of error, 0 otherwise.
   * Error could happen if replicable app is not initialized, or paxos instance for this name does not exist.
   */
  @Override
  public int coordinateRequest(JSONObject request) {
    if (this.paxosInterface == null) return -1; // replicable app not set
    try {
      Packet.PacketType type = Packet.getPacketType(request);
      switch (type) {
        case ACTIVE_COORDINATION: // coordination type packets
          paxosManager.handleIncomingPacket(request);
          break;
        case UPDATE:
          UpdatePacket update = new UpdatePacket(request);
          update.setNameServerId(nodeID);
          paxosManager.propose(update.getName(), request.toString());
          break;
        case ACTIVE_REMOVE:
          OldActiveSetStopPacket stopPacket1 = new OldActiveSetStopPacket(request);
          paxosManager.proposeStop(stopPacket1.getName(), request.toString(), stopPacket1.getVersion());
        case OLD_ACTIVE_STOP:
          OldActiveSetStopPacket stopPacket2 = new OldActiveSetStopPacket(request);
          paxosManager.proposeStop(stopPacket2.getName(), request.toString(), stopPacket2.getVersion());
          break;
        case ACTIVE_ADD:
          paxosInterface.handleDecision(null, request.toString(), false, false);
          AddRecordPacket recordPacket = new AddRecordPacket(request);
          paxosManager.createPaxosInstance(recordPacket.getName(), Config.FIRST_VERSION, ConsistentHashing.getReplicaControllerSet(recordPacket.getName()), paxosInterface);
          GNS.getLogger().fine("Added paxos instance:" + recordPacket.getName());
          break;
        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:
          NewActiveSetStartupPacket newActivePacket = new NewActiveSetStartupPacket(request);
          paxosManager.createPaxosInstance(newActivePacket.getName(), newActivePacket.getNewActiveVersion(),
                  newActivePacket.getNewActiveNameServers(), paxosInterface);
          break;
        case DNS:
        case NAME_SERVER_LOAD:
        case SELECT_REQUEST:
        case SELECT_RESPONSE:
          // Packets sent from replica controller
          paxosInterface.handleDecision(null, request.toString(), false, false);
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
  public void reset() {
    paxosManager.resetAll();
  }

}


