package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.fields.Field;
import edu.umass.cs.gns.nameserver.recordExceptions.FieldNotFoundException;
import edu.umass.cs.gns.nameserver.recordExceptions.RecordNotFoundException;
import edu.umass.cs.gns.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.paxos.PaxosManager;
import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;


/**
 * This class sends a message to current active replicas to stop an active replica
 *
 * @author abhigyan
 *
 */
public class StopActiveSetTask extends TimerTask {

  int MAX_ATTEMPTS = 3;		 // number of actives contacted to start replica
  String name;
  Set<Integer> oldActiveNameServers;
  Set<Integer> oldActivesQueried;
  String oldPaxosID;

  /**
   * Constructor object
   *
   * @param name
   * @param oldActiveNameServers
   */
  public StopActiveSetTask(String name, //NameRecordKey nameRecordKey, 
                           Set<Integer> oldActiveNameServers, String oldPaxosID) {
    this.name = name;
    //this.nameRecordKey = nameRecordKey;
    this.oldActiveNameServers = oldActiveNameServers;
    this.oldActivesQueried = new HashSet<Integer>();
    this.oldPaxosID = oldPaxosID;
  }

  @Override
  public void run() {

    //ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(name);

    ArrayList<Field> readFields = new ArrayList<Field>();
    readFields.add(ReplicaControllerRecord.OLD_ACTIVE_NAMESERVERS_RUNNING);
    readFields.add(ReplicaControllerRecord.OLD_ACTIVE_PAXOS_ID);
    ReplicaControllerRecord nameRecordPrimary = null;
    try {
      nameRecordPrimary = NameServer.getNameRecordPrimaryMultiField(name, readFields);
    } catch (RecordNotFoundException e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe(" Name Record Does not Exist. Name = " + name // + " Record Key = " + nameRecordKey
        );
      }
      this.cancel();
      return;
    }

    // is active with paxos ID have stopped return true
    try {
      if (nameRecordPrimary.isOldActiveStopped(oldPaxosID)) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Old active name servers stopped. Paxos ID: " + oldPaxosID
                  + " Old Actives : " + oldActiveNameServers);
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

    if (oldActivesQueried.size() == MAX_ATTEMPTS) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe("ERROR: Old Actives failed to STOP after " + MAX_ATTEMPTS + ". "
                + "Old active name servers queried: " + oldActivesQueried);
      }
      this.cancel();
      return;
    }

    int selectedOldActive = selectNextActiveToQuery();

    if (selectedOldActive == -1) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe("ERROR: No more old active left to query. "
                + "Old Active name servers queried: " + oldActivesQueried + ". Old Actives not STOPped yet..");
      }
      this.cancel();
      return;
    }

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Old Active Name Server Selected to Query: " + selectedOldActive);
    }

    OldActiveSetStopPacket packet = new OldActiveSetStopPacket(name, //nameRecordKey, 
            NameServer.nodeID, selectedOldActive, oldPaxosID, PacketType.OLD_ACTIVE_STOP);
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Old active stop Sent Packet: " + packet);
    }
    try {
//      NameServer.tcpTransport.sendToID(packet.toJSONObject(), selectedOldActive, GNS.PortType.PERSISTENT_TCP_PORT);
      NameServer.tcpTransport.sendToID(selectedOldActive, packet.toJSONObject());
    } catch (IOException e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("IO Exception in sending OldActiveSetSTOPPacket: " + e.getMessage());
      }
      e.printStackTrace();
    } catch (JSONException e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("JSON Exception in sending OldActiveSetSTOPPacket: " + e.getMessage());
      }
      e.printStackTrace();
    }

    // if first time:  
    // 		Send message to one of the old actives to stop replica set. schedule next timer event.
    // else if active replica stopped:
    // 		cancel timer. return
    // else if no more active replicas to send message to:
    // 		log error message. cancel timer. return 
    // else:
    // 		send to next active replica. schedule next timer event.

  }

  /**
   * the next active name server that will be queried to start active replicas
   *
   * @return
   */
  private int selectNextActiveToQuery() {
    int selectedActive = -1;
    for (int x : oldActiveNameServers) {
      if (oldActivesQueried.contains(x) || !PaxosManager.isNodeUp(x)) {
        continue;
      }
      selectedActive = x;
      break;
    }
    if (selectedActive != -1) {
      oldActivesQueried.add(selectedActive);
    }
    return selectedActive;
  }
}
