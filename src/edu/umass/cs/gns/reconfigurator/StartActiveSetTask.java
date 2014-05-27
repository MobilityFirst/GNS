package edu.umass.cs.gns.reconfigurator;

import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * @author V. Arun
 * Based on code created by abhigyan.
 * 
 * On a change in the set of active replicas for a name, this class informs the new set of active replicas.
 * It informs one of the new replicas and checks after a timeout value if it received a confirmation that
 * at least a majority of new replicas have been informed. Otherwise, it resends to another new replica.
 *
 * Note: this class is executed using a timer object and not an executor service.
 *
 */
public class StartActiveSetTask implements RCProtocolTask {

  /** Fields read from database */
  private static ArrayList<ColumnField> startupActiveSetFields = new ArrayList<ColumnField>();

  static {
    startupActiveSetFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
    startupActiveSetFields.add(ReplicaControllerRecord.ACTIVE_VERSION);
  }


  private String name;
  private Set<Integer> oldActiveNameServers;
  private Set<Integer> newActiveNameServers;
  private Set<Integer> newActivesQueried;
  private int newActiveVersion;
  private int oldActiveVersion;
  private String initialValue;
  private ReplicaController replicaController;
  private int requestID;

  /**
   * Constructor object
   */
  public StartActiveSetTask(String name,Set<Integer> oldActiveNameServers, Set<Integer> newActiveNameServers,
		  int newActiveVersion, int oldActiveVersion, String initialValue) {
    this.name = name;
    this.oldActiveNameServers = oldActiveNameServers;
    this.newActiveNameServers = newActiveNameServers;
    this.newActivesQueried = new HashSet<Integer>();
    this.newActiveVersion = newActiveVersion;
    this.oldActiveVersion = oldActiveVersion;
    this.initialValue = initialValue;
    this.requestID = replicaController.getOngoingStartActiveRequests().put(this);
  }
  public void setReplicaController(ReplicaController rc) {
	  this.replicaController = rc;
  }



  @Override
  public void run() {
    // note: if some problem happens copy previous code for this class kept in dead-code folder
    try {
      boolean terminateTask = false;
      try {
        ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
                replicaController.getDB(), name, startupActiveSetFields);
        if (replicaController.getOngoingStartActiveRequests().get(requestID) == null){
          // normally, this task will be the terminated via this branch of code..
          terminateTask = true;
          if (Config.debugMode) {
            GNS.getLogger().info("New active name servers running. Name = " + name + " All Actives: "
                    + newActiveNameServers + " Actives Queried: " + newActivesQueried);
          }
        } else if (rcRecord.getActiveVersion() != newActiveVersion) {
          // this should be a rare occurrence, because group changes for a name happen at long intervals.
          terminateTask = true;
          GNS.getLogger().info(" Actives got accepted and replaced by new actives. Quitting. ");
        } else {
          // send request to a new active replica
          int selectedActive = replicaController.getGnsNodeConfig().getClosestServer(newActiveNameServers,
                  newActivesQueried);
          if (selectedActive == -1) {
            terminateTask = true;
            GNS.getLogger().severe("ERROR: No more active left to query. Active name servers queried: "
                    + newActivesQueried + " Actives not started.");
          }
          else {
            newActivesQueried.add(selectedActive);
            NewActiveSetStartupPacket packet = new NewActiveSetStartupPacket(name, replicaController.getNodeID(),
                    selectedActive, newActiveNameServers, oldActiveNameServers,
                    (short)oldActiveVersion, (short)newActiveVersion, PacketType.NEW_ACTIVE_START, initialValue, false);
            packet.setUniqueID(requestID);
            replicaController.getNioServer().sendToID(selectedActive, packet.toJSONObject());
            if (Config.debugMode) {
              GNS.getLogger().fine(" NEW ACTIVE STARTUP PACKET SENT Name: "+ name + "\t" + packet.toString());
            }
          }
        }
      }catch (RecordNotFoundException e) {
        terminateTask = true;
        GNS.getLogger().severe(" Name record does not exist. Name = " + name + " NewActiveID " + newActiveVersion
                + "\t NS Queried" + newActivesQueried);
      }
      if (terminateTask) {
        replicaController.getOngoingStartActiveRequests().remove(requestID);
        throw new CancelExecutorTaskException();
      }
    } catch (Exception e) {
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw new RuntimeException(); // this is the only way to cancel executor task exception.
      }
      // this exception handling here because this time task is executed via an executor service,
      // which does not print an exception printed at all.
      GNS.getLogger().severe("Exception in Start Active Set Task. " + e.getMessage() + "\t" + e.getClass());
      e.printStackTrace();
    }
  }


}
