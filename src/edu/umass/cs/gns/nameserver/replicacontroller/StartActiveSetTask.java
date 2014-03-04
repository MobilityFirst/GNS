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
 * On a change in the set of active replicas for a name, this class informs the new set of active replicas.
 * It informs one of the new replicas and checks after a timeout value if it received a confirmation that
 * at least a majority of new replicas have been informed. Otherwise, it resends to another new replica.
 *
 * Note: this class is executed using a timer object and not an executor service.
 *
 * @see edu.umass.cs.gns.nameserver.replicacontroller.StopActiveSetTask
 * @see edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController
 * @see edu.umass.cs.gns.nameserver.ListenerReplicationPaxos
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
        nameRecordPrimary = ReplicaControllerRecord.getNameRecordPrimaryMultiField(NameServer.getReplicaController(), name,
                getGetStartupActiveSetFields());
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
        Integer progress = GroupChangeProgress.groupChangeProgress.get(name);
        if (progress == null || progress >= GroupChangeProgress.NEW_ACTIVE_START) {
          String msg = "New active name servers running. Name = " + name + " All Actives: "
                  + newActiveNameServers + " Actives Queried: " + newActivesQueried;
          if (StartNameServer.experimentMode) GNS.getLogger().severe(msg);
          else  GNS.getLogger().info(msg);
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
        GroupChangeProgress.groupChangeProgress.remove(name);
        ReplicaController.groupChangeStartTimes.remove(name);
        GNS.getLogger().severe("ERROR: No more active left to query. "
                + "Active name servers queried: " + newActivesQueried + " Actives not started.");
        this.cancel();
        return;
      }
      else {
        newActivesQueried.add(selectedActive);
      }

      if (StartNameServer.debugMode) {
        GNS.getLogger().info(" Active Name Server Selected to Query: " + selectedActive);
      }

      NewActiveSetStartupPacket packet = new NewActiveSetStartupPacket(name, NameServer.getNodeID(), selectedActive, newActiveNameServers, oldActiveNameServers,
              oldActivePaxosID, newActivePaxosID, PacketType.NEW_ACTIVE_START, initialValue, false);
      try {
        NameServer.getTcpTransport().sendToID(selectedActive, packet.toJSONObject());
      } catch (IOException e) {
        GNS.getLogger().severe("IO Exception in sending NewActiveSetStartupPacket: " + e.getMessage());
        e.printStackTrace();
      } catch (JSONException e) {
        GNS.getLogger().severe("JSON Exception in sending NewActiveSetStartupPacket: " + e.getMessage());
        e.printStackTrace();
      }
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" NEW ACTIVE STARTUP PACKET SENT: " + packet.toString());
      }

    } catch (Exception e) {

      // this exception handling here in case the timer task was executed using an executor service,
      // in which case we would not see an exception printed at all.
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
