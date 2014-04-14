package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.activeReconfiguration.ActiveReplica;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.replicaCoordination.ReplicaControllerCoordinator;
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

  private NameServer nameServer;

  public NSPacketDemultiplexer(NameServer nameServer) {
    this.nameServer = nameServer;
  }

  /**
   * Entry point for all packets received at name server.
   *
   * Based on the packet type it forwards to active replica or replica controller.
   *
   * @param json JSON object received by NIO package.
   */
  public boolean handleJSONObject(final JSONObject json) {
    nameServer.getExecutorService().submit(new Runnable() {
      @Override
      public void run() {
        try {
          Packet.PacketType type = Packet.getPacketType(json);

          switch (type) {
            case UPDATE:
              // TODO define a different packet type for upserts
              UpdatePacket updateAddressPacket = new UpdatePacket(json);
              if (updateAddressPacket.getOperation().isUpsert()) {
                ReplicaControllerCoordinator replicaController = nameServer.getReplicaControllerCoordinator();
                if (replicaController != null) {
                  replicaController.coordinateRequest(json);
                }
              } else {
                ActiveReplicaCoordinator gnsReconfigurable = nameServer.getActiveReplicaCoordinator();
                if (gnsReconfigurable != null) {
                  gnsReconfigurable.coordinateRequest(json);
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
              ActiveReplicaCoordinator gnsReconfigurable = nameServer.getActiveReplicaCoordinator();
              if (gnsReconfigurable != null) {
                gnsReconfigurable.coordinateRequest(json);
              }
              break;

            // Packets sent by client to replica controller
            case ADD_RECORD:
            case REQUEST_ACTIVES:
            case REMOVE_RECORD:
            case RC_REMOVE:

              // Packets sent by active replica
            case ACTIVE_ADD_CONFIRM:
            case ACTIVE_REMOVE_CONFIRM:
            case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY:
            case NEW_ACTIVE_START_CONFIRM_TO_PRIMARY:

              // Packets sent by replica controller to itself (this will proposed for coordination)
            case NEW_ACTIVE_PROPOSE:
            case GROUP_CHANGE_COMPLETE:
            case NAMESERVER_SELECTION:
            case NAME_RECORD_STATS_RESPONSE:
              // packets from coordination modules at replica controller
            case REPLICA_CONTROLLER_COORDINATION:
              ReplicaControllerCoordinator replicaController = nameServer.getReplicaControllerCoordinator();
              if (replicaController != null) {
                replicaController.coordinateRequest(json);
              }
              break;

            case NEW_ACTIVE_START:
            case NEW_ACTIVE_START_FORWARD:
            case NEW_ACTIVE_START_RESPONSE:
            case NEW_ACTIVE_START_PREV_VALUE_REQUEST:
            case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:
            case OLD_ACTIVE_STOP:
            case DELETE_OLD_ACTIVE_STATE:
              ActiveReplica activeReplica = nameServer.getActiveReplica();
              if (activeReplica != null) {
                activeReplica.handleIncomingPacket(json);
              }
              break;
            default:
              GNS.getLogger().severe("Packet type not found: " + json);
              break;
          }
        } catch (JSONException e) {
          GNS.getLogger().severe("JSON Exception here: " + json + " Exception: " + e.getCause());
          e.printStackTrace();
        } catch (Exception e) {
          GNS.getLogger().severe("Caught exception with message: " + json + " Exception: " + e.getCause());
          e.printStackTrace();
        }
      }
    }) ;

    return true;
  }

}
