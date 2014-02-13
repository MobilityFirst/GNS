package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.replicacontroller.ComputeNewActivesTask;
import edu.umass.cs.gns.nameserver.replicacontroller.ListenerNameRecordStats;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.paxos.PaxosInterface;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 6/29/13
 * Time: 8:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class NSPaxosInterface implements PaxosInterface {

  //    @Override
  public void proposeRequestToPaxos(String paxosID, RequestPacket requestPacket) {
//    NameServer.paxosManager.propose(paxosID, requestPacket);
  }

  @Override
  public void handlePaxosDecision(String paxosID, RequestPacket req, boolean recovery) {
    long t0 = System.currentTimeMillis();
    try {

      // messages decided in to paxos between actives
      if (req.clientID == Packet.PacketType.ACTIVE_PAXOS_STOP.getInt()) {
        // current paxos instance stopped
        ListenerReplicationPaxos.handleActivePaxosStop(new JSONObject(req.value));
      }
      else if (req.clientID == Packet.PacketType.UPDATE_ADDRESS_NS.getInt()) {
        // address update is applied
        ClientRequestWorker.handleUpdateAddressNS(new JSONObject(req.value));
      }

      // messages decided for paxos between primaries
      else if (req.clientID == Packet.PacketType.NEW_ACTIVE_PROPOSE.getInt()) {
        ComputeNewActivesTask.applyNewActivesProposed(req.value);
      }
      else if (req.clientID == Packet.PacketType.NAME_RECORD_STATS_RESPONSE.getInt()) {
        ListenerNameRecordStats.applyNameRecordStatsPacket(req.value);
      }
      else if (req.clientID == Packet.PacketType.NAMESERVER_SELECTION.getInt()) {
        ListenerNameRecordStats.applyNameServerSelectionPacket(req.value);
      }
      else if (req.clientID == Packet.PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY.getInt()) {
        ReplicaController.applyActiveNameServersRunning(req.value);
      }
//            else if (req.clientID == Packet.PacketType.OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY.getInt()) { // not used
//                ReplicaController.oldActiveStoppedWriteToNameRecord(req.value);
//            }
      else if (req.clientID  == Packet.PacketType.ADD_RECORD_NS.getInt()) {
        ClientRequestWorker.handleAddRecordNS(new JSONObject(req.value), recovery);
      }
      else if (req.clientID  == Packet.PacketType.REMOVE_RECORD_LNS.getInt()) {
        ReplicaController.applyMarkedForRemoval(req.value);
      }
      else if (req.clientID == Packet.PacketType.PRIMARY_PAXOS_STOP.getInt()) {
        ReplicaController.applyStopPrimaryPaxos(req.value);
      }


    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      GNS.getLogger().severe(" Exception Exception Exception ... " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    long t1 = System.currentTimeMillis();
    if (t1 - t0 > 100)
      GNS.getLogger().severe("Long delay " + (t1 - t0) + "ms. Packet: " + req.value);
  }

  @Override
  public void handleFailureMessage(FailureDetectionPacket fdPacket) {

    ReplicaController.handleNodeFailure(fdPacket);
  }

  @Override
  public String getState(String paxosID) {

    if (ReplicaController.isPrimaryPaxosID(paxosID)) {
      BasicRecordCursor iterator = NameServer.replicaController.getAllRowsIterator();
//    if (StartNameServer.debugMode) GNS.getLogger().info("Got iterator : " + replicationRound);
      StringBuilder sb = new StringBuilder();
      int recordCount = 0;
      while (iterator.hasNext()) {
        try {
          JSONObject jsonObject = iterator.next();
          sb.append(jsonObject.toString());
          sb.append("\n");
          recordCount += 1;
        } catch (Exception e) {
          GNS.getLogger().severe("Problem creating ReplicaControllerRecord from JSON" + e);
        }
      }
      GNS.getLogger().info("Number of records whose state is read from DB: " + recordCount);
      return sb.toString();
    }
    else {
      String name = ReplicaController.getNameFromActivePaxosID(paxosID);
      // read all fields of the record
      try {
        NameRecord nameRecord = NameServer.getNameRecord(name);
        return  (nameRecord == null) ? null: nameRecord.toString();
      } catch (RecordNotFoundException e) {
        GNS.getLogger().warning("Exception Record not found. " + e.getMessage() + "\t" + paxosID);
        return null;
      }
    }
  }

  @Override
    public void updateState(String paxosID, String state) {
    try {
      GNS.getLogger().info("Update state: " + paxosID  + "\tState-length: " + state.length());
      if (ReplicaController.isPrimaryPaxosID(paxosID)) {

        if  (state.length() == 0) {
          return;
        }
        GNS.getLogger().info("Here: " + paxosID);
        int recordCount = 0;
        int startIndex = 0;
        GNS.getLogger().info("Update state: " + paxosID);
        while (true) {
          int endIndex = state.indexOf('\n', startIndex);
          if (endIndex == -1) break;
          String x = state.substring(startIndex, endIndex);
          if (x.length() > 0) {
            recordCount += 1;
            JSONObject json = new JSONObject(x);
            ReplicaControllerRecord rcr = new ReplicaControllerRecord(json);
            GNS.getLogger().fine("Inserting rcr into DB ....: " + rcr + "\tjson = " + json);
            try {
              NameServer.addNameRecordPrimary(rcr);
            } catch (RecordExistsException e) {
              NameServer.updateNameRecordPrimary(rcr);
            }

//            try {
//              ReplicaControllerRecord rc2 = NameServer.getNameRecordPrimary(new ReplicaControllerRecord(json).getName());
//              GNS.getLogger().info("Read fresh copy RC from DB ....: " + rc2 + "\t");
//            } catch (RecordNotFoundException e) {
//              e.printStackTrace();
//            } catch (FieldNotFoundException e) {
//              e.printStackTrace();
//            }

            startIndex = endIndex;
          } else {
            startIndex += 1;
          }
        }
        GNS.getLogger().info("Number of rc records updated in DB: " + recordCount);
      } else {
        JSONObject json = new JSONObject(state);
        GNS.getLogger().info("Updated name record in DB: " + paxosID + "\t" + state);
        try {
          NameServer.addNameRecord(new NameRecord(json));
        } catch (RecordExistsException e) {
          NameServer.updateNameRecord(new NameRecord(json));
        }
//        try {
//          NameRecord nr2 = NameServer.getNameRecord(new NameRecord(json).getName());
//          GNS.getLogger().info("Read fresh copy from DB ....: " + nr2 + "\t");
//        } catch (RecordNotFoundException e) {
//          e.printStackTrace();
//        } catch (FieldNotFoundException e) {
//          e.printStackTrace();
//        }
      }
    } catch (JSONException e) {

      e.printStackTrace();
    }
  }

  @Override
  public String getPaxosKeyForPaxosID(String paxosID) {
    if (ReplicaController.isPrimaryPaxosID(paxosID)) return paxosID; // paxos between primaries
    else { // paxos between actives.
      int index = paxosID.lastIndexOf("-");
      if (index == -1) return paxosID;
      return paxosID.substring(0, index);
    }
  }


}
