package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.activeReconfiguration.ActiveReplica;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaController;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * * DONT not use any class in package edu.umass.cs.gns.nsdesign **
 */
/**
 * Work in progress. Inactive code.
 *
 * Forwards incoming json objects to either active replica or replica controller at this node.
 *
 * Created by abhigyan on 2/26/14.
 */
public class NSPacketDemultiplexer extends PacketDemultiplexer {

  private NameServerInterface nameServerInterface;

  public NSPacketDemultiplexer(NameServerInterface nameServerInterface) {
    this.nameServerInterface = nameServerInterface;
  }

  /**
   * Entry point for all packets received at name server.
   *
   * Based on the packet type it forwards to active replica or replica controller.
   *
   * @param json JSON object received by NIO package.
   */
  public boolean handleJSONObject(JSONObject json) {
    boolean isPacketTypeFound = true;
    try {
      Packet.PacketType type = Packet.getPacketType(json);

      switch (type) {
        case UPDATE:
          GNS.getLogger().fine(">>>>>>>>>>>>>>>UPDATE ADDRESS PACKET: " + json);
          // TODO define a different packet type for upserts
          UpdatePacket updateAddressPacket = new UpdatePacket(json);
          if (updateAddressPacket.getOperation().isUpsert()) {
            ReplicaController replicaController = nameServerInterface.getReplicaController();
            if (replicaController != null) {
              replicaController.handleJSONObject(json);
            }
          } else {
            GnsReconfigurable gnsReconfigurable = nameServerInterface.getGnsReconfigurable();
            if (gnsReconfigurable != null) {
              gnsReconfigurable.handleIncomingPacket(json);
            }
          }
          break;

        // Packets sent from LNS
        case DNS:
        case NAME_SERVER_LOAD:
        case SELECT_REQUEST:
        case SELECT_RESPONSE:
        // Packets sent from replica controller
        case ACTIVE_ADD:
        case ACTIVE_REMOVE:
        case ACTIVE_COORDINATION:
        // New addition to NSs to support update requests sent back to LNS. This is where the update confirmation
        // coming back from the LNS is handled.
        case CONFIRM_UPDATE:
        case CONFIRM_ADD:
        case CONFIRM_REMOVE:
          GnsReconfigurable gnsReconfigurable = nameServerInterface.getGnsReconfigurable();
          if (gnsReconfigurable != null) {
            gnsReconfigurable.handleIncomingPacket(json);
          }
          break;

        // Packets sent from LNS
        case ADD_RECORD:
        case REQUEST_ACTIVES:
        case REMOVE_RECORD:
        case NAMESERVER_SELECTION:
        case NAME_RECORD_STATS_RESPONSE:
        // Packets sent from active replica
        case ACTIVE_ADD_CONFIRM:
        case ACTIVE_REMOVE_CONFIRM:
        case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY:
        case NEW_ACTIVE_START_CONFIRM_TO_PRIMARY:
        // packets from coordination modules at replica controller
        case REPLICA_CONTROLLER_COORDINATION:
          ReplicaController replicaController = nameServerInterface.getReplicaController();
          if (replicaController != null) {
            replicaController.handleJSONObject(json);
          }
          break;

        case NEW_ACTIVE_START:
        case NEW_ACTIVE_START_FORWARD:
        case NEW_ACTIVE_START_RESPONSE:
        case NEW_ACTIVE_START_PREV_VALUE_REQUEST:
        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:
        case OLD_ACTIVE_STOP:
          ActiveReplica activeReplica = nameServerInterface.getActiveReplica();
          if (activeReplica != null) {
            activeReplica.handleIncomingPacket(json);
          }
          break;
        default:
          isPacketTypeFound = false;
          break;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON Exception here: " + json + " Exception: " + e.getCause());
      e.printStackTrace();
    }
    return isPacketTypeFound;
  }

}
