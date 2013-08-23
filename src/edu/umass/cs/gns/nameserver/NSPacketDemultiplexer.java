package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.nameserver.replicacontroller.KeepAliveWorker;
import edu.umass.cs.gns.nameserver.replicacontroller.ListenerNameRecordStats;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gns.paxos.PaxosManager;

import java.util.ArrayList;

/**
 * All packets recieved at name server (TCP/UDP) pass through this demultiplexer.
 * This thread implements PacketDemultiplex interface for 
 * @author abhigyan
 *
 */
public class NSPacketDemultiplexer extends PacketDemultiplexer{

  @Override
  public void handleJSONObjects(ArrayList jsonObjects) {
    for (Object j: jsonObjects) {
      handleJSONObject((JSONObject) j);
    }

  }

  public void handleJSONObject(JSONObject json) {

    try {
      Packet.PacketType type = Packet.getPacketType(json);
      switch(type) {
        // client requests: ADD/REMOVE/UPDATE/LOOKUP

        case ADD_RECORD_LNS:
        case ADD_RECORD_NS:
        case CONFIRM_ADD_NS:
        case ADD_COMPLETE:
        case REMOVE_RECORD_LNS:
        case REQUEST_ACTIVES:
        case UPDATE_ADDRESS_LNS:
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
          KeepAliveWorker.handleIncomingPacket(json);
          break;



        // Paxos: internal Paxos messages
        case PAXOS_PACKET:
          PaxosManager.handleIncomingPacket(json);
          break;
      }

    } catch (JSONException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

//	public void handleJSONObject(JSONObject json) {
//		
//		try {
//			switch(Packet.getPacketType(json)) {
//			case ADD_RECORD_LNS:
//			case ADD_RECORD_NS:
//			case UPDATE_ADDRESS_NS:
//			case TINY_UPDATE:
//			case CONFIRM_UPDATE_NS:
//			case REMOVE_RECORD_LNS:
//			case UPDATE_ADDRESS_LNS:
//			
//				// TODO: add packet types.
//				NSListenerUpdate.handleIncomingPacket(json);
//				break;
//			case NAME_RECORD_STATS_RESPONSE:
////				GNRS.getLogger().finer("NIO: Handed off packet to Listener name record stats");
//				ListenerNameRecordStats.handleIncomingPacket(json);
//				break;
//			case ACTIVE_NAMESERVER_UPDATE:
//			case REPLICATE_RECORD:
//			case NAMESERVER_SELECTION:
//			case REMOVE_REPLICATION_RECORD:
//				ListenerReplication.handleIncomingMessage(json);
//				break;
//			case PAXOS_PACKET:
//				PaxosManager.handleIncomingMessage(json);
//				break;
//			case NEW_ACTIVE_START:
//			case NEW_ACTIVE_START_FORWARD:
//			case NEW_ACTIVE_START_RESPONSE:
//			case NEW_ACTIVE_START_PREV_VALUE_REQUEST:
//			case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:
//			case OLD_ACTIVE_STOP:
//				ListenerReplicationPaxos.handleIncomingMessage(json);
//				break;
//			case NEW_ACTIVE_START_CONFIRM_TO_PRIMARY:
//			case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY:
//				ReplicaController.handleIncomingMessage(json);
//				break;
//			default:
//				break;
//			}
//			
//		} catch (JSONException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
}
