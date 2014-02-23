package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.util.BestServerSelection;
import org.json.JSONException;

import java.io.IOException;
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
  int numAttempts = 0;

  /**
   * Constructor object
   *
   * @param name
   * @param oldActiveNameServers
   */
  public StopActiveSetTask(String name, Set<Integer> oldActiveNameServers, String oldPaxosID) {
    this.name = name;
    this.oldActiveNameServers = oldActiveNameServers;
    this.oldActivesQueried = new HashSet<Integer>();
    this.oldPaxosID = oldPaxosID;
    MAX_ATTEMPTS = oldActiveNameServers.size();
  }


  @Override
  public void run() {
    try {
      numAttempts ++;

      Integer progress = ReplicaController.groupChangeProgress.get(name);
      if (progress == null || progress >= ReplicaController.OLD_ACTIVE_STOP) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("Old active name servers stopped. Paxos ID: " + oldPaxosID +
                  " Old Actives : " + oldActiveNameServers);
        }
        this.cancel();
        return;
      }


      int selectedOldActive = BestServerSelection.getSmallestLatencyNSNotFailed(oldActiveNameServers, oldActivesQueried);
      //selectNextActiveToQuery();

      if (selectedOldActive == -1) {
        ReplicaController.groupChangeProgress.remove(name);
        ReplicaController.groupChangeStartTimes.remove(name);
//      if (StartNameServer.debugMode) {
        GNS.getLogger().severe("ERROR: No more old active left to query. "
                + "Old Active name servers queried: " + oldActivesQueried + ". Old Actives not stopped. OldpaxosID " + oldPaxosID);
//      }
        this.cancel();
        return;
      } else {
        oldActivesQueried.add(selectedOldActive);
      }

      if (StartNameServer.debugMode) {
        GNS.getLogger().info(" Old Active Name Server Selected to Query: " + selectedOldActive);
      }

      OldActiveSetStopPacket packet = new OldActiveSetStopPacket(name, NameServer.nodeID, selectedOldActive, oldPaxosID,
              PacketType.OLD_ACTIVE_STOP);
      if (StartNameServer.debugMode) {
        GNS.getLogger().info(" Old active stop Sent Packet: " + packet);
      }
      try {
        NameServer.tcpTransport.sendToID(selectedOldActive, packet.toJSONObject());
      } catch (IOException e) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("IO Exception in sending OldActiveSetSTOPPacket: " + e.getMessage());
        }
        e.printStackTrace();
      } catch (JSONException e) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("JSON Exception in sending OldActiveSetSTOPPacket: " + e.getMessage());
        }
        e.printStackTrace();
      }
      Long groupChangeStartTime = ReplicaController.groupChangeStartTimes.get(packet.getName());
      if (groupChangeStartTime != null) {
        long groupChangeDuration = System.currentTimeMillis()  - groupChangeStartTime;
        if (StartNameServer.experimentMode) GNS.getLogger().severe("\tOldActiveStopPropose\t" + packet.getName() + "\t" + groupChangeDuration+ "\t");
      }

    } catch (Exception e) {
      GNS.getLogger().severe("Exception in Stop Active Set Task. " + e.getMessage());
      e.printStackTrace();
    }
  }

}
