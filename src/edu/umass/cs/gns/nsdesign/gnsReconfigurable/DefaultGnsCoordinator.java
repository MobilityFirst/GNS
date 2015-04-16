package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * Trivial coordinator for a single name server GNS, which executes requests without any coordination.
 * Created by abhigyan on 4/8/14.
 * @param <NodeIDType>
 */
public class DefaultGnsCoordinator<NodeIDType> extends ActiveReplicaCoordinator {

  private final NodeIDType nodeID;
  private final GNSNodeConfig<NodeIDType> gnsNodeConfig;
  private final Replicable replicable;

  public DefaultGnsCoordinator(NodeIDType nodeID, GNSNodeConfig<NodeIDType> gnsNodeConfig, Replicable replicable) {
    this.nodeID = nodeID;
    this.gnsNodeConfig = gnsNodeConfig;
    this.replicable = replicable;
  }

  /**
   * Handles coordination among replicas for a request. Returns -1 in case of error, 0 otherwise.
   * Error could happen if replicable app is not initialized, or paxos instance for this name does not exist.
   * 
   * @param json
   * @return 
   */
  @Override
  public int coordinateRequest(JSONObject json) {
    if (this.replicable == null) {
      GNS.getLogger().severe("replicable app not set!");
      return -1; // replicable app not set
    }

    try {
      Packet.PacketType type = Packet.getPacketType(json);
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("MsgType " + type + " Msg " + json);
      }
      switch (type) {
        // no coordination needed for any request.
        
        case UPDATE: // set a field in update packet because LNS may not set this field correctly.
          UpdatePacket<NodeIDType> update = new UpdatePacket<NodeIDType>(json, gnsNodeConfig);
          update.setNameServerID(nodeID);
          replicable.handleDecision(null, update.toString(), false);
          break;
        case ACTIVE_REMOVE: // stop request for removing a name record
        case OLD_ACTIVE_STOP: // (sent by active replica) stop request on a group change
        case ACTIVE_ADD:  // createPaxosInstance when name is added for the first time
        case DNS:
        case NAME_SERVER_LOAD:
        case SELECT_REQUEST:
        case SELECT_RESPONSE:
        case UPDATE_CONFIRM:
        case ADD_CONFIRM:
        case REMOVE_CONFIRM:
          replicable.handleDecision(null, json.toString(), false);
          break;
        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:
          // no action needed
        // (sent by active replica) create any coordination state
        // after a group change. but there is no coordination here
          //
          break;
        case ACTIVE_COORDINATION:
          // we do not expect any coordination packets
          throw new UnsupportedOperationException();
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
    // no action needed because there is no coordination state.
  }

  @Override
  public void shutdown() {
    // nothing to do here currently, may need updating if we create a thread inside this module
  }
}
