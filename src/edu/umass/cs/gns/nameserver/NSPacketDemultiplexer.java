package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.replicacontroller.ListenerNameRecordStats;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.paxos.PaxosManager;
import java.io.IOException;
import java.util.ArrayList;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * All packets received at name server (TCP/UDP) pass through this demultiplexer.
 * This thread implements PacketDemultiplex interface for 
 * @author abhigyan
 *
 */
public class NSPacketDemultiplexer extends PacketDemultiplexer {

  @Override
  public void handleJSONObjects(ArrayList jsonObjects) {
    for (Object j : jsonObjects) {
      handleJSONObject((JSONObject) j);
    }

  }

  public void handleJSONObject(JSONObject json) {
    try {
      Packet.PacketType type = Packet.getPacketType(json);
      switch (type) {
        // client requests: ADD/REMOVE/UPDATE/LOOKUP

        case ADD_RECORD_LNS:
        case ADD_RECORD_NS:
        case CONFIRM_ADD_NS:
        case ADD_COMPLETE:
        case REMOVE_RECORD_LNS:
        case REQUEST_ACTIVES:
        case UPDATE_ADDRESS_LNS:
        case SELECT_REQUEST:
        case DNS:
          ClientRequestWorker.handleIncomingPacket(json, type);
          break;

        // Statistics: Read/write rate, votes for name record
        case NAMESERVER_SELECTION:
        case NAME_RECORD_STATS_RESPONSE:
          ListenerNameRecordStats.handleIncomingPacket(json);
          break;

        // Replication: Transition from old actives to new actives
        // msgs to actives
        case NEW_ACTIVE_START:
        case NEW_ACTIVE_START_FORWARD:
        case NEW_ACTIVE_START_RESPONSE:
        case NEW_ACTIVE_START_PREV_VALUE_REQUEST:
        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:
        case OLD_ACTIVE_STOP:
          ListenerReplicationPaxos.handleIncomingPacket(json);
          break;
        // msgs to primary
        case NEW_ACTIVE_START_CONFIRM_TO_PRIMARY:
        case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY:
          ReplicaController.handleIncomingPacket(json);
          break;

        case KEEP_ALIVE_PRIMARY:
        case DELETE_PRIMARY:
//      case KEEP_ALIVE_ACTIVE:
          // TODO uncomment this.
//          KeepAliveWorker.handleIncomingPacket(json);
          break;



        // Paxos: internal Paxos messages
        case PAXOS_PACKET:
          PaxosManager.handleIncomingPacket(json);
          break;
        default:
          GNS.getLogger().warning("No handler for packet type: " + type.toString());
          break;
      }

    } catch (JSONException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
