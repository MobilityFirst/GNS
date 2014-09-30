package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * Trivial coordinator for a single name server GNS, which executes requests without any coordination.
 * Created by abhigyan on 4/8/14.
 */
public class DefaultGnsCoordinator extends ActiveReplicaCoordinator {

  private NodeId<String> nodeID;

  private Replicable replicable;

  public DefaultGnsCoordinator(NodeId<String> nodeID, Replicable replicable) {
    this.nodeID = nodeID;
    this.replicable = replicable;
  }

  /**
   * Handles coordination among replicas for a request. Returns -1 in case of error, 0 otherwise.
   * Error could happen if replicable app is not initialized, or paxos instance for this name does not exist.
   */
  @Override
  public int coordinateRequest(JSONObject request) {
    if (this.replicable == null) return -1; // replicable app not set

    try {
      Packet.PacketType type = Packet.getPacketType(request);
      switch (type) {
        // no coordination needed for any request.

        case UPDATE: // set a field in update packet because LNS may not set this field correctly.
          UpdatePacket update = new UpdatePacket(request);
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
        case CONFIRM_UPDATE:
        case CONFIRM_ADD:
        case CONFIRM_REMOVE:
          replicable.handleDecision(null, request.toString(), false);
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
