package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.packet.NameRecordStatsPacket;
import edu.umass.cs.gns.packet.NameServerSelectionPacket;
import edu.umass.cs.gns.packet.Packet;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

/**
 * This class updates the <code>ReplicaControllerRecord</code> for a name with statistics
 * such as read rate, write rate, and votes for name servers that are best suited to be the active replicas
 * for a name. These statistics are used by replica controllers in choosing the set of active replicas for a name.
 * This information is received from packets sent by other nodes in the system including
 * active replicas of a name and local name servers.
 * <p>
 * This class handles two types of messages: <code>NameServerSelectionPacket</code> and
 * <code>NameRecordStatsPacket</code>. <code>NameServerSelectionPacket</code> packets are sent by local name servers
 * and <code>NameRecordStatsPacket</code> packets are sent by active replicas. Currently,
 * we have disabled sending of <code>NameRecordStatsPacket</code> type messages by active replicas,  because
 * all information could be supplied by local name servers only.
 * <p>
 * Future work: we should not depend on local name servers to send these statistics, and use active replicas to send
 * them. For this reason, I have not removed code related to <code>NameRecordStatsPacket</code>.
 * <p>
 * @see edu.umass.cs.gns.nameserver.replicacontroller.ComputeNewActivesTask
 * @see edu.umass.cs.gns.packet.NameServerSelectionPacket
 * @see edu.umass.cs.gns.localnameserver.NameServerVoteThread
 * @see edu.umass.cs.gns.packet.NameRecordStatsPacket
 * @see edu.umass.cs.gns.nameserver.SendNameRecordStats
 * @deprecated
 * @author abhigyan
 */
public class ListenerNameRecordStats extends Thread {


  public static void handleIncomingPacket(JSONObject json) throws JSONException, IOException {
    switch (Packet.getPacketType(json)) {
      case NAME_RECORD_STATS_RESPONSE:
        handleNameRecordStatsPacket(json);
        break;
      case NAMESERVER_SELECTION:
        handleNameServerSelectionPacket(json);
        break;
      default:
        GNS.getLogger().severe("Unknown packet type: " + json);
        break;
    }
  }


  /**
   * This is the only method in this file which actually runs and updates stats for name servers.
   * @param incomingJSON
   * @throws JSONException
   * @throws IOException
   */
  private static void handleNameServerSelectionPacket(JSONObject incomingJSON) throws JSONException, IOException {

    if (StartNameServer.debugMode) GNS.getLogger().fine("NS: received  NAMESERVER_SELECTION " + incomingJSON.toString());
    NameServerSelectionPacket selectionPacket = new NameServerSelectionPacket(incomingJSON);
    try {


      ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(NameServer.getReplicaController(), selectionPacket.getName());
      try {
        rcRecord.addReplicaSelectionVote(selectionPacket.getNameserverID(), selectionPacket.getVote(),selectionPacket.getUpdate());

      } catch (FieldNotFoundException e) {
        GNS.getLogger().severe("Field not found exception. " + e.getMessage());
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }

    } catch (Exception e) {
      GNS.getLogger().severe("Exception here " + e.getMessage());
      e.printStackTrace();
    }
    if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION: Name Sever Vote: " + selectionPacket.toString());

  }



  private static void handleNameRecordStatsPacket(JSONObject json) {
    if (StartNameServer.debugMode) GNS.getLogger().fine("ListenerNameRecordStats: received " + json.toString());
    NameRecordStatsPacket statsPacket;
    try {
      statsPacket = new NameRecordStatsPacket(json);
    } catch (JSONException e) {
      e.printStackTrace();
      return;
    }

    ReplicaControllerRecord rcRecord;
    try {
      rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(NameServer.getReplicaController(), statsPacket.getName(), ReplicaControllerRecord.STATS_MAP);
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Record not found exception. " + statsPacket.getName());
      e.printStackTrace();
      return;
    }
    // TODO: convert read and write to directly write
    try {
      rcRecord.addNameServerStats(statsPacket.getActiveNameServerId(),
              statsPacket.getReadFrequency(), statsPacket.getWriteFrequency());
    } catch (FieldNotFoundException e) {
      GNS.getLogger().fine("Field not found exception. " + e.getMessage());
      e.printStackTrace();
    }
  }

}


