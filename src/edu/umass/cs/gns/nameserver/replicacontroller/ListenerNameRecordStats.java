package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.fields.Field;
import edu.umass.cs.gns.nameserver.recordExceptions.FieldNotFoundException;
import edu.umass.cs.gns.nameserver.recordExceptions.RecordNotFoundException;
import edu.umass.cs.gns.packet.NameRecordStatsPacket;
import edu.umass.cs.gns.packet.NameServerSelectionPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;

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
  public static void handleNameServerSelectionPacket(JSONObject incomingJSON) throws JSONException, IOException {

    if (StartNameServer.debugMode) GNS.getLogger().fine("NS: received  NAMESERVER_SELECTION " + incomingJSON.toString());
    NameServerSelectionPacket selectionPacket = new NameServerSelectionPacket(incomingJSON);
//    ArrayList<Field> readFields = new ArrayList<Field>();
//    readFields.add(ReplicaControllerRecord.VOTES_MAP);
    try {


      ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(selectionPacket.getName());
      try {
//      rcRecord = NameServer.getNameRecordPrimaryMultiField(selectionPacket.getName(), readFields);
        rcRecord.addReplicaSelectionVote(selectionPacket.getNameserverID(), selectionPacket.getVote(),selectionPacket.getUpdate());
//        try {
//          if (StartNameServer.debugMode) GNS.getLogger().fine("NS: received  NAMESERVER_SELECTION (after) " + NameServer.getNameRecordPrimary(selectionPacket.getName()));
//        } catch (RecordNotFoundException e) {
//          // no exception possible here
//        }
      } catch (FieldNotFoundException e) {
        GNS.getLogger().severe("Field not found exception. " + e.getMessage());
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }

    } catch (Exception e) {
      GNS.getLogger().severe("Exception here " + e.getMessage());
      e.printStackTrace();
    }
//    GNS.getLogger().severe("Record read = " + rcRecord.toString());

//    if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION: Name Sever Vote: " + selectionPacket.toString());

  }

//  public static void handleNameServerSelectionPacket(JSONObject incomingJSON) throws JSONException, IOException {
//
//    if (StartNameServer.debugMode) GNS.getLogger().fine("NS: received  NAMESERVER_SELECTION " + incomingJSON.toString());
//    NameServerSelectionPacket selectionPacket = new NameServerSelectionPacket(incomingJSON);
//    // Send ACK to local name server immediately, that vote is received.
//    NameServer.tcpTransport.sendToID(selectionPacket.getLocalnameserverID(),incomingJSON);
//    RequestPacket request = new RequestPacket(PacketType.NAMESERVER_SELECTION.getInt(),
//            selectionPacket.toString(), PaxosPacketType.REQUEST, false);
//    PaxosManager.propose(ReplicaController.getPrimaryPaxosID(selectionPacket.getName()), request);
//    if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS PROPOSAL: NameSever Vote: " + incomingJSON.toString());
//
//
//  }


  private static void handleNameRecordStatsPacket(JSONObject json) {
    if (StartNameServer.debugMode) GNS.getLogger().fine("ListenerNameRecordStats: received " + json.toString());
    NameRecordStatsPacket statsPacket;
    try {
      statsPacket = new NameRecordStatsPacket(json);
    } catch (JSONException e) {
      e.printStackTrace();
      return;
    }
    String paxosID = ReplicaController.getPrimaryPaxosID(statsPacket.getName());
    RequestPacket requestPacket = new RequestPacket(PacketType.NAME_RECORD_STATS_RESPONSE.getInt(),
            statsPacket.toString(), PaxosPacketType.REQUEST, false);
    PaxosManager.propose(paxosID, requestPacket);
    if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS PROPOSAL: Stats Packet proposed. ");

  }

//  private static void handleNameRecordStatsPacket(JSONObject json) {
//    if (StartNameServer.debugMode) GNS.getLogger().fine("ListenerNameRecordStats: received " + json.toString());
//    NameRecordStatsPacket statsPacket;
//    try {
//      statsPacket = new NameRecordStatsPacket(json);
//    } catch (JSONException e) {
//      e.printStackTrace();
//      return;
//    }
//    String paxosID = ReplicaController.getPrimaryPaxosID(statsPacket.getName());
//    RequestPacket requestPacket = new RequestPacket(PacketType.NAME_RECORD_STATS_RESPONSE.getInt(),
//            statsPacket.toString(), PaxosPacketType.REQUEST, false);
//    PaxosManager.propose(paxosID, requestPacket);
//    if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS PROPOSAL: Stats Packet proposed. ");
//
//  }

  /**
   * Apply the decision from Paxos: Packet = NameRecordStatsPacket.
   */
  public static void applyNameRecordStatsPacket(String decision) {

    NameRecordStatsPacket statsPacket;
    try {
      statsPacket = new NameRecordStatsPacket(new JSONObject(decision));
    } catch (JSONException e) {
      e.printStackTrace();
      return;
    }
    if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION: StatsPacket for name " + statsPacket.getName()
            + " Decision: " + decision);
    ArrayList<Field> readFields = new ArrayList<Field>();
    readFields.add(ReplicaControllerRecord.STATS_MAP);
    ReplicaControllerRecord rcRecord;
    try {
      rcRecord = NameServer.getNameRecordPrimaryMultiField(statsPacket.getName(),readFields);
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

  public static void applyNameServerSelectionPacket(String decision) {
    NameServerSelectionPacket selectionPacket;
    try {
      selectionPacket = new NameServerSelectionPacket(new JSONObject(decision));
      ArrayList<Field> readFields = new ArrayList<Field>();
      readFields.add(ReplicaControllerRecord.VOTES_MAP);
      ReplicaControllerRecord rcRecord = NameServer.getNameRecordPrimaryMultiField(selectionPacket.getName(),readFields);
      GNS.getLogger().severe("Record read = " + rcRecord.toString());
      // TODO: convert read and write to directly write
      if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION: Name Sever Vote: " + selectionPacket.toString());
      rcRecord.addReplicaSelectionVote(selectionPacket.getNameserverID(), selectionPacket.getVote(), selectionPacket.getUpdate());

    } catch (JSONException e) {
      e.printStackTrace();
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Record not found exception. Packet = " + decision);
      e.printStackTrace();  
      return;
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage());

      e.printStackTrace();  
    }

  }
}


