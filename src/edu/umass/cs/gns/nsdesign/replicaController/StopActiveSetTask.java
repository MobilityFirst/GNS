package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.packet.BasicPacket;
import edu.umass.cs.gns.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.packet.Packet.PacketType;
import org.json.JSONException;

import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;


/**
 * On a change in the set of active replicas for a name or when a record is to be removed, this class informs the old
 * set of active replicas to stop functioning. After a timeout, it checks if the old replicas have confirmed that
 * they have stopped functioning. If so, this task is cancelled, or else, this task sends another replica a message.
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
 */
public class StopActiveSetTask extends TimerTask {


  private String name;
  private Set<Integer> oldActiveNameServers;
  private Set<Integer> oldActivesQueried;
  private String oldPaxosID;
  private int requestID;
  private PacketType packetType;
  private ReplicaController rc;

  /**
   * Constructor object
   *
   * @param name
   * @param oldActiveNameServers
   */
  public StopActiveSetTask(String name, Set<Integer> oldActiveNameServers, String oldPaxosID, PacketType packetType,
                           BasicPacket clientPacket, ReplicaController rc) {
    this.name = name;
    this.oldActiveNameServers = oldActiveNameServers;
    this.oldActivesQueried = new HashSet<Integer>();
    this.oldPaxosID = oldPaxosID;
    this.requestID = rc.getOngoingStopActiveRequests().put(clientPacket);
    this.packetType = packetType;
    this.rc = rc;
  }

  @Override
  public void run() {
    try {
      GNSMessagingTask gnsMessagingTask = getMessagingTask();
      if (gnsMessagingTask == null) {
        throw new CancelExecutorTaskException();
      } else {
        GNSMessagingTask.send(gnsMessagingTask, rc.getNioServer());
      }
    } catch (Exception e) {
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw new RuntimeException();
      }
      GNS.getLogger().severe("Exception in Stop Active Set Task. " + e.getMessage());
      e.printStackTrace();
    }
  }

  private GNSMessagingTask getMessagingTask() {

    GNSMessagingTask msgTask = null;

    if (rc.getOngoingStopActiveRequests().get(requestID) == null) {
      GNS.getLogger().fine("Old active name servers stopped. Paxos ID: " + oldPaxosID + " Old Actives : "
              + oldActiveNameServers);
    } else {
      int selectedOldActive = rc.getGnsNodeConfig().getClosestNameServer(oldActiveNameServers, oldActivesQueried);
      if (selectedOldActive == -1) {
        rc.getOngoingStopActiveRequests().remove(this.requestID);
        GNS.getLogger().severe("ERROR: Old Actives not stopped and no more old active left to query. "
                + "Old Active name servers queried: " + oldActivesQueried + ". OldpaxosID " + oldPaxosID);
      } else {
        oldActivesQueried.add(selectedOldActive);

        GNS.getLogger().fine(" Old Active Name Server Selected to Query: " + selectedOldActive);

        OldActiveSetStopPacket packet = new OldActiveSetStopPacket(name, requestID, rc.getNodeID(),
                selectedOldActive, oldPaxosID, packetType);
        GNS.getLogger().fine(" Old active stop Sent Packet: " + packet);

        try {
          msgTask = new GNSMessagingTask(selectedOldActive, packet.toJSONObject());
        } catch (JSONException e) {
          GNS.getLogger().severe("JSON Exception in sending OldActiveSetSTOPPacket: " + e.getMessage());
          e.printStackTrace();
        }
      }
    }
    return msgTask;
  }

}
