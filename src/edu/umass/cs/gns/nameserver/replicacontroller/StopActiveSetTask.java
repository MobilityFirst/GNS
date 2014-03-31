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
 * On a change in the set of active replicas for a name, this class informs the old set of active replicas to
 * stop functioning. After a timeout, it checks if the old replicas have confirmed that they have stopped functioning.
 * If so, this task is cancelled, or else, this task sends another replica a message.
 * <p>
 * In paxos terms, it asks one of the old active replicas to propose a STOP request to the paxos
 * group between old set of active replicas. Once the STOP request is committed by paxos, all replicas would delete
 * paxos state, and update database to indicate they are no longer active replicas.
 * <p>
 * Note: this class is executed using a timer object and not an executor service.
 *
 * @see edu.umass.cs.gns.nameserver.replicacontroller.StartActiveSetTask
 * @see edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController
 * @see edu.umass.cs.gns.nameserver.ListenerReplicationPaxos
 * @author abhigyan
 * @deprecated
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

      Integer progress = GroupChangeProgress.groupChangeProgress.get(name);
      if (progress == null || progress >= GroupChangeProgress.OLD_ACTIVE_STOP) {
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
        GroupChangeProgress.groupChangeProgress.remove(name);
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

      OldActiveSetStopPacket packet = new OldActiveSetStopPacket(name, 0, NameServer.getNodeID(), selectedOldActive, oldPaxosID,
              PacketType.OLD_ACTIVE_STOP);
      if (StartNameServer.debugMode) {
        GNS.getLogger().info(" Old active stop Sent Packet: " + packet);
      }
      try {
        NameServer.getTcpTransport().sendToID(selectedOldActive, packet.toJSONObject());
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
