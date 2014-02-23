package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.util.BestServerSelection;
import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;

/**
 * Informs new active replicas for a name until this node gets a confirmation from an active replicas that a majority of
 * the new active replicas have successfully obtained a copy of the most up to date name record.
 *
 * @author abhigyan
 *
 */
public class StartActiveSetTask extends TimerTask {

  String name;
  Set<Integer> oldActiveNameServers;
  Set<Integer> newActiveNameServers;
  Set<Integer> newActivesQueried;
  String newActivePaxosID;
  String oldActivePaxosID;
  ValuesMap initialValue;
  int numAttempts = 0;

  /**
   * Constructor object
   *
   * @param name
   * @param oldActiveNameServers
   * @param newActiveNameServers
   */
  public StartActiveSetTask(String name,Set<Integer> oldActiveNameServers, Set<Integer> newActiveNameServers,
                            String newActivePaxosID, String oldActivePaxosID, ValuesMap initialValue) {
    this.name = name;
    this.oldActiveNameServers = oldActiveNameServers;
    this.newActiveNameServers = newActiveNameServers;
    this.newActivesQueried = new HashSet<Integer>();
    this.newActivePaxosID = newActivePaxosID;
    this.oldActivePaxosID = oldActivePaxosID;
    this.initialValue = initialValue;
  }


  @Override
  public void run() {
    try {

      numAttempts ++;

      ReplicaControllerRecord nameRecordPrimary;
      try {
        nameRecordPrimary = NameServer.getNameRecordPrimaryMultiField(name, getGetStartupActiveSetFields());
      } catch (RecordNotFoundException e) {
        e.printStackTrace();
        if (StartNameServer.debugMode) {
          GNS.getLogger().severe(" Name record does not exist. Name = " + name);
        }
        this.cancel();
        return;
      }

      try {
        if (!nameRecordPrimary.getActivePaxosID().equals(newActivePaxosID)) {
          if (StartNameServer.debugMode) {
            GNS.getLogger().info(" Actives got accepted and replaced by new actives. Quitting. ");
          }
          this.cancel();
          return;
        }
        Integer progress = ReplicaController.groupChangeProgress.get(name);
        if (progress == null || progress >= ReplicaController.NEW_ACTIVE_START) {
//      if (ReplicaController.groupChangeProgress.get(newActivePaxosID) .isActiveRunning()) {
          String msg = "New active name servers running. Name = " + name + " All Actives: "
                  + newActiveNameServers + " Actives Queried: " + newActivesQueried;
          if (StartNameServer.experimentMode) GNS.getLogger().severe(msg);
          else  GNS.getLogger().info(msg);
//        if (StartNameServer.debugMode) {
//
//        }
          this.cancel();
          return;
        }
      } catch (FieldNotFoundException e) {
        GNS.getLogger().severe("Field not found exception. " + e.getMessage());
        e.printStackTrace();
        this.cancel();
        return;
      }

      int selectedActive = BestServerSelection.getSmallestLatencyNSNotFailed(newActiveNameServers, newActivesQueried);

      if (selectedActive == -1) {
        ReplicaController.groupChangeProgress.remove(name);
        ReplicaController.groupChangeStartTimes.remove(name);
        if (StartNameServer.debugMode) {
          GNS.getLogger().severe("ERROR: No more active left to query. "
                  + "Active name servers queried: " + newActivesQueried + " Actives not started.");
        }
        this.cancel();
        return;
      }
      else {
        newActivesQueried.add(selectedActive);
      }

      if (StartNameServer.debugMode) {
        GNS.getLogger().info(" Active Name Server Selected to Query: " + selectedActive);
      }

      NewActiveSetStartupPacket packet = new NewActiveSetStartupPacket(name, //nameRecordKey,
              NameServer.nodeID, selectedActive, newActiveNameServers, oldActiveNameServers,
              oldActivePaxosID, newActivePaxosID, PacketType.NEW_ACTIVE_START, initialValue, false);
      try {
        NameServer.tcpTransport.sendToID(selectedActive, packet.toJSONObject());
      } catch (IOException e) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("IO Exception in sending NewActiveSetStartupPacket: " + e.getMessage());
        }
        e.printStackTrace();
      } catch (JSONException e) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("JSON Exception in sending NewActiveSetStartupPacket: " + e.getMessage());
        }
        e.printStackTrace();
      }
      if (StartNameServer.debugMode) {
        GNS.getLogger().info(" NEW ACTIVE STARTUP PACKET SENT: " + packet.toString());
      }

    } catch (Exception e) {
      GNS.getLogger().severe("Exception in Start Active Set Task. " + e.getMessage());
      e.printStackTrace();
    }
  }


  private static ArrayList<ColumnField> getStartupActiveSetFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getGetStartupActiveSetFields() {
    synchronized (getStartupActiveSetFields) {
      if (getStartupActiveSetFields.size() > 0) return getStartupActiveSetFields;
      getStartupActiveSetFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
      getStartupActiveSetFields.add(ReplicaControllerRecord.ACTIVE_PAXOS_ID);
      return getStartupActiveSetFields;
    }
  }

}
