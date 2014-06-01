package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.BasicPacket;
import edu.umass.cs.gns.nsdesign.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import org.json.JSONException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;


/**
 * On a change in the set of active replicas for a name or when a record is to be removed, this class informs the old
 * set of active replicas to stop functioning. After a timeout, it checks if the old replicas have confirmed that
 * they have stopped functioning. If so, this task is cancelled, or else, this task sends another replica a message.
 * <p>
 * It asks one of the old active replicas to propose a STOP request to the coordination
 * group between old set of active replicas. Once the STOP request is committed by coordination group, all replicas
 * would delete any coordination state, and update database to indicate they are no longer active replicas.
 * <p>
 *
 *
 * @see edu.umass.cs.gns.nsdesign.replicaController.StartActiveSetTask
 * @see edu.umass.cs.gns.nsdesign.replicaController.ReplicaController
 * @author abhigyan
 */
public class StopActiveSetTask extends TimerTask {


  private String name;
  private Set<Integer> oldActiveNameServers;
  private Set<Integer> oldActivesQueried;
  private int oldVersion;
  private int requestID;
  private PacketType packetType;
  private ReplicaController rc;

  /**
   * Constructor object
   */
  public StopActiveSetTask(String name, Set<Integer> oldActiveNameServers, int oldVersion, PacketType packetType,
                           BasicPacket clientPacket, ReplicaController rc) {
    this.name = name;
    this.oldActiveNameServers = oldActiveNameServers;
    this.oldActivesQueried = new HashSet<Integer>();
    this.oldVersion = oldVersion;
    this.requestID = rc.getOngoingStopActiveRequests().put(clientPacket);
    this.packetType = packetType;
    this.rc = rc;
  }

  @Override
  public void run() {
    try {
      boolean cancelTask = checkCancelTask();
      if (cancelTask) {
        throw new CancelExecutorTaskException();
      }
    } catch (Exception e) {
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw new RuntimeException();
      }
      GNS.getLogger().severe("Exception in Stop Active Set Task. " + e.getMessage());
      e.printStackTrace();
    }
  }

  private boolean checkCancelTask() throws IOException {

    boolean cancelTask = true;
    if (rc.getOngoingStopActiveRequests().get(requestID) == null) {
      GNS.getLogger().fine("Old active name servers stopped. Version: " + oldVersion + " Old Actives : "
              + oldActiveNameServers);
    } else {
      int selectedOldActive = rc.getGnsNodeConfig().getClosestServer(oldActiveNameServers, oldActivesQueried);
      if (selectedOldActive == -1) {
        rc.getOngoingStopActiveRequests().remove(this.requestID);
        GNS.getLogger().severe("Exception ERROR: Old Actives not stopped and no more old active left to query. "
                + "Old Active name servers queried: " + oldActivesQueried + ". Old Version " + oldVersion + " Name: "
                + name);
      } else {
        oldActivesQueried.add(selectedOldActive);
        GNS.getLogger().fine(" Old Active Name Server Selected to Query: " + selectedOldActive);
        OldActiveSetStopPacket packet = new OldActiveSetStopPacket(name, requestID, rc.getNodeID(), selectedOldActive,
                (short)oldVersion, packetType);
        GNS.getLogger().fine(" Old active stop Sent Packet: " + packet);
        try {
          rc.getNioServer().sendToID(selectedOldActive, packet.toJSONObject());
          cancelTask = false;
        } catch (JSONException e) {
          GNS.getLogger().severe("JSON Exception in sending OldActiveSetSTOPPacket: " + e.getMessage());
          e.printStackTrace();
        }
      }
    }
    return cancelTask;
  }

}
