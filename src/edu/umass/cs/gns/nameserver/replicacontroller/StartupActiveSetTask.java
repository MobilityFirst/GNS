package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.nameserver.fields.Field;
import edu.umass.cs.gns.nameserver.recordExceptions.FieldNotFoundException;
import edu.umass.cs.gns.nameserver.recordExceptions.RecordNotFoundException;
import edu.umass.cs.gns.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.paxos.PaxosManager;
import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;

/**
 * This class sends message to active replica to startup a new paxos instance for a name record.
 *
 * @author abhigyan
 *
 */
public class StartupActiveSetTask extends TimerTask {

  int MAX_ATTEMPTS = 3;		 // number of actives contacted to start replica
  String name;
  //NameRecordKey nameRecordKey;
  Set<Integer> oldActiveNameServers;
  Set<Integer> newActiveNameServers;
  Set<Integer> newActivesQueried;
  String newActivePaxosID;
  String oldActivePaxosID;
  ValuesMap initialValue;

  /**
   * Constructor object
   *
   * @param name
   * @param oldActiveNameServers
   * @param newActiveNameServers
   */
  public StartupActiveSetTask(String name, //NameRecordKey nameRecordKey, 
                              Set<Integer> oldActiveNameServers, Set<Integer> newActiveNameServers,
                              String newActivePaxosID, String oldActivePaxosID, ValuesMap initialValue) {
    this.name = name;
    //this.nameRecordKey = nameRecordKey;
    this.oldActiveNameServers = oldActiveNameServers;
    this.newActiveNameServers = newActiveNameServers;
    this.newActivesQueried = new HashSet<Integer>();
    this.newActivePaxosID = newActivePaxosID;
    this.oldActivePaxosID = oldActivePaxosID;
    this.initialValue = initialValue;
  }

  @Override
  public void run() {

    ArrayList<Field> readFields = new ArrayList<Field>();
    readFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
    readFields.add(ReplicaControllerRecord.ACTIVE_PAXOS_ID);

    ReplicaControllerRecord nameRecordPrimary;
    try {
      nameRecordPrimary = NameServer.getNameRecordPrimaryMultiField(name, readFields);
    } catch (RecordNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe(" Name Record Does not Exist. Name = " + name //+ " Record Key = " + nameRecordKey
        );
      }
      this.cancel();
      return;
    }

    try {
      if (!nameRecordPrimary.getActivePaxosID().equals(newActivePaxosID)) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(" Actives got accepted and replaced by new actives. Quitting. ");
        }
        this.cancel();
        return;
      }

      if (nameRecordPrimary.isActiveRunning()) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("New active name servers running. Startup done. All Actives: "
                  + nameRecordPrimary.getActiveNameservers() + " Actives Queried: " + newActivesQueried);
        }
        this.cancel();
        return;
      }
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      this.cancel();
      return;
    }

    if (newActivesQueried.size() == MAX_ATTEMPTS) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe("ERROR: New Actives failed to start after " + MAX_ATTEMPTS + ". "
                + "Active name servers queried: " + newActivesQueried);
      }
      this.cancel();
      return;
    }

    int selectedActive = selectNextActiveToQuery();

    if (selectedActive == -1) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe("ERROR: No more active left to query. "
                + "Active name servers queried: " + newActivesQueried + " Actives not started.");
      }
      this.cancel();
      return;
    }

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Active Name Server Selected to Query: " + selectedActive);
    }

    NewActiveSetStartupPacket packet = new NewActiveSetStartupPacket(name, //nameRecordKey,
            NameServer.nodeID, selectedActive, newActiveNameServers, oldActiveNameServers,
            oldActivePaxosID, newActivePaxosID, PacketType.NEW_ACTIVE_START, initialValue, false);
    try {
//      NameServer.tcpTransport.sendToID(packet.toJSONObject(), selectedActive, GNS.PortType.PERSISTENT_TCP_PORT);
      NameServer.tcpTransport.sendToID(selectedActive, packet.toJSONObject());
    } catch (IOException e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("IO Exception in sending NewActiveSetStartupPacket: " + e.getMessage());
      }
      e.printStackTrace();
    } catch (JSONException e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("JSON Exception in sending NewActiveSetStartupPacket: " + e.getMessage());
      }
      e.printStackTrace();
    }
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" NEW ACTIVE STARTUP PACKET SENT: " + packet.toString());
    }

    // 
    // if first time:  
    // 		send message to an active replica to get started. schedule next timer event.
    // else if active replica started:
    // 		cancel timer.
    // else if no more active replicas to send message to:
    // 		log error message. cancel timer. 
    // else: 
    // 		send to a next active replica. schedule next timer event.
  }

  /**
   * the next active name server that will be queried to start active replicas
   *
   * @return
   */
  private int selectNextActiveToQuery() {
    int selectedActive = -1;
    for (int x : newActiveNameServers) {
      if (newActivesQueried.contains(x) || !PaxosManager.isNodeUp(x)) {
        continue;
      }
      selectedActive = x;
      break;
    }
    if (selectedActive != -1) {
      newActivesQueried.add(selectedActive);
    }
    return selectedActive;
  }
}
