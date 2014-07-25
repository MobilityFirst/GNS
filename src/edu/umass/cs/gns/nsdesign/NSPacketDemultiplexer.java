package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.activeReconfiguration.ActiveReplica;
import edu.umass.cs.gns.nsdesign.commands.CommandProcessor;
import edu.umass.cs.gns.nsdesign.packet.LNSToNSCommandPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.replicaCoordination.ReplicaControllerCoordinator;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * Forwards incoming json objects to either active replica or replica controller at this node.
 * <p/>
 * Created by abhigyan on 2/26/14.
 */
public class NSPacketDemultiplexer extends AbstractPacketDemultiplexer {

  private NameServer nameServer;

  private int msgCount = 0;

  private int prevMsgCount = 0;

  private int intervalCount = 0;

  public NSPacketDemultiplexer(final NameServer nameServer, final int nodeID) {
    this.nameServer = nameServer;
    this.nameServer.getExecutorService().scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        intervalCount += 1;

        GNS.getStatLogger().info(" Interval " + intervalCount + " TotalMsgCount " + getMsgCount() + " IntervalMsgCount " +
                (getMsgCount() - prevMsgCount) + " Node " + nodeID + " ");
        prevMsgCount = msgCount;
      }
    },0, 10, TimeUnit.SECONDS);
    register(Packet.PacketType.LNS_TO_NS_COMMAND);
    register(Packet.PacketType.UPDATE);
    register(Packet.PacketType.DNS);
    register(Packet.PacketType.SELECT_REQUEST);
    register(Packet.PacketType.SELECT_RESPONSE);
    // Packets sent from replica controller
    register(Packet.PacketType.ACTIVE_ADD);
    register(Packet.PacketType.ACTIVE_REMOVE);
    register(Packet.PacketType.ACTIVE_COORDINATION);
    // New addition to NSs to support update requests sent back to LNS. This is where the update confirmation
    // coming back from the LNS is handled.
    register(Packet.PacketType.CONFIRM_UPDATE);
    register(Packet.PacketType.CONFIRM_ADD);
    register(Packet.PacketType.CONFIRM_REMOVE);


    register(Packet.PacketType.ADD_RECORD);
    register(Packet.PacketType.REQUEST_ACTIVES);
    register(Packet.PacketType.REMOVE_RECORD);
    register(Packet.PacketType.RC_REMOVE);
    // Packets sent by active replica
    register(Packet.PacketType.ACTIVE_ADD_CONFIRM);
    register(Packet.PacketType.ACTIVE_REMOVE_CONFIRM);
    register(Packet.PacketType.OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY);
    register(Packet.PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY);
    // Packets sent by replica controller to itself (this will proposed for coordination)
    register(Packet.PacketType.NEW_ACTIVE_PROPOSE);
    register(Packet.PacketType.GROUP_CHANGE_COMPLETE);
    register(Packet.PacketType.NAMESERVER_SELECTION);
    register(Packet.PacketType.NAME_RECORD_STATS_RESPONSE);
    register(Packet.PacketType.NAME_SERVER_LOAD);
    // packets from coordination modules at replica controller
    register(Packet.PacketType.REPLICA_CONTROLLER_COORDINATION);



    register(Packet.PacketType.NEW_ACTIVE_START);
    register(Packet.PacketType.NEW_ACTIVE_START_FORWARD);
    register(Packet.PacketType.NEW_ACTIVE_START_RESPONSE);
    register(Packet.PacketType.NEW_ACTIVE_START_PREV_VALUE_REQUEST);
    register(Packet.PacketType.NEW_ACTIVE_START_PREV_VALUE_RESPONSE);
    register(Packet.PacketType.OLD_ACTIVE_STOP);
    register(Packet.PacketType.DELETE_OLD_ACTIVE_STATE);

  }



  /**
   * Entry point for all packets received at name server.
   * <p/>
   * Based on the packet type it forwards to active replica or replica controller.
   *
   * @param json JSON object received by NIO package.
   */
  @Override
  public boolean handleJSONObject(final JSONObject json) {

    incrementMsgCount();
    try {
      final Packet.PacketType type = Packet.getPacketType(json);

      // return value should be true if packet type matches these packets:
      if (Config.debugMode) GNS.getLogger().fine(" MsgType " + type + " Msg " + json);
      nameServer.getExecutorService().submit(new Runnable() {
        @Override
        public void run() {
          try {
            switch (type) {
              case LNS_TO_NS_COMMAND:
                CommandProcessor.processCommandPacket(new LNSToNSCommandPacket(json), nameServer.getGnsReconfigurable());
                break;

              // Packets sent from LNS
              case UPDATE:
              case DNS:
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
                ActiveReplicaCoordinator appCoordinator = nameServer.getActiveReplicaCoordinator();
                if (appCoordinator != null) {
                  appCoordinator.coordinateRequest(json);
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
              case NAME_SERVER_LOAD:
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
                GNS.getLogger().severe("Packet type not found: " + type + " JSON: " + json);
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
      });

    } catch (JSONException e) {
      e.printStackTrace();
    }
    return true;
  }

  private synchronized void incrementMsgCount() {
    msgCount += 1;
  }

  private synchronized int getMsgCount() {
    return msgCount;
  }
}
