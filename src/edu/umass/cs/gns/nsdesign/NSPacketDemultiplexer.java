package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.activeReplica.ActiveReplicaInterface;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaControllerInterface;
import edu.umass.cs.gns.packet.Packet;
import org.json.JSONException;
import org.json.JSONObject;

/*** DONT not use any class in package edu.umass.cs.gns.nsdesign ***/

/**
 * Work in progress. Inactive code.
 *
 * Forwards incoming json objects to either active replica or replica controller at this node.
 *
 * Created by abhigyan on 2/26/14.
 */
public class NSPacketDemultiplexer extends PacketDemultiplexer{

  NameServerInterface nameServerInterface;

  public NSPacketDemultiplexer(NameServerInterface nameServerInterface) {
    this.nameServerInterface = nameServerInterface;
  }

//
//  public void handleJSONObjects(ArrayList<JSONObject> jsonObjects) {
//    for (JSONObject json: jsonObjects)
//      handleIncomingPacket(json);
//  }

  /**
   * Entry point for all packets received at name server.
   *
   * Based on the packet type it forwards to active replica or replica controller.
   * @param json JSON object received by NIO package.
   */
  public boolean handleJSONObject(JSONObject json){
    boolean isPacketTypeFound = true;
    try {



      Packet.PacketType type = Packet.getPacketType(json);

      switch (type) {

        // Packets sent from LNS
        case DNS:
        case UPDATE_ADDRESS_LNS:
        case NAME_SERVER_LOAD:
        case SELECT_REQUEST:
        case SELECT_RESPONSE:
          // Packets sent from replica controller
        case ACTIVE_ADD:
        case ACTIVE_REMOVE:
        case ACTIVE_GROUPCHANGE:
          // packets from coordination modules at active replica
        case ACTIVE_COORDINATION:
          ActiveReplicaInterface activeReplica = nameServerInterface.getActiveReplica();
          if (activeReplica != null)  activeReplica.handleIncomingPacket(json);
          break;

        // Packets sent from LNS
        case ADD_RECORD_LNS:
        case REQUEST_ACTIVES:
        case REMOVE_RECORD_LNS:
        case NAMESERVER_SELECTION:
        case NAME_RECORD_STATS_RESPONSE:
          // Packets sent from active replica
        case ACTIVE_ADD_CONFIRM:
        case ACTIVE_REMOVE_CONFIRM:
        case ACTIVE_GROUPCHANGE_CONFIRM:
          // packets from coordination modules at replica controller
        case REPLICA_CONTROLLER_COORDINATION:
          ReplicaControllerInterface replicaController = nameServerInterface.getReplicaController();
          if (replicaController != null)  replicaController.handleIncomingPacket(json);
          break;

        default:
          isPacketTypeFound = false;
//          GNS.getLogger().warning("No handler for packet type: " + type.toString());
          break;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON Exception here: " + json + " Exception: " + e.getCause());
      e.printStackTrace();
    }
    return isPacketTypeFound;
  }
}
