package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.GroupChangeCompletePacket;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.Config;

import java.util.TimerTask;


/**
 * This class checks if replica controllers have updated the database indicating that the group change for the name
 * is complete. If yes, the task is immediately cancelled, otherwise it again proposes a request to replica controllers
 * to mark that group change is complete.
 * <p>
 * We may need to request replica controllers again because a proposed update request may not get committed by
 * coordination.
 * Therefore, it is necessary to retry a request. How many times to retry?  Usually, requests need to be tried
 * on a coordinator failure, therefore we should retry longer than the failure detection interval.
 * <p>
 * User: abhigyan
 * Date: 11/14/13
 * Time: 9:24 AM
 */
public class WriteActiveNameServersRunningTask extends TimerTask {

  /** Name for which group change completed */
  private String name;

  /** Version number of the current active replica set.*/
  private int version;

  private ReplicaController replicaController;

  /** Number of times request has been re-proposed.*/
  private int numAttempts = 0;

  /** Max number of attempts.*/
  private int MAX_RETRY = 10;

  public WriteActiveNameServersRunningTask(String name, int version, ReplicaController replicaController) {
    this.name = name;
    this.version = version;
    int timeout = Config.failureDetectionTimeoutSec;
    if (timeout != -1) MAX_RETRY = (int)(timeout * 2.0 / ReplicaController.RC_TIMEOUT_MILLIS );
    this.replicaController = replicaController;
  }

  @Override
  public void run() {
    try{
      numAttempts++;
      ReplicaControllerRecord rcRecord;
      try {
        rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(replicaController.getDB(), name,
                ReplicaControllerRecord.ACTIVE_VERSION, ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
      } catch (RecordNotFoundException e) {
        GNS.getLogger().warning("RCRecord\t" + name + "\tdeleted. Task Cancelled");
        throw  new CancelExecutorTaskException();
      }

      try {
        if (rcRecord.isActiveRunning()) {
          // this is usually how this task will be terminated
          GNS.getLogger().info("Group change complete. Record updated. Name " + name + "\tVersion\t" + version);
          throw  new CancelExecutorTaskException();
        }
        if (rcRecord.getActiveVersion() != version) {
          // this should rarely happen, as group changes are infrequent.
          GNS.getLogger().warning("Group change to  " + version + " complete. and new group change started.");
          throw  new CancelExecutorTaskException();
        }
      } catch (FieldNotFoundException e) {
        e.printStackTrace();
        GNS.getLogger().severe("Field Not Found Exception " + e.getMessage() + "\tVersion\t" + version + "\tnumAttempts" +
                numAttempts);
        throw  new CancelExecutorTaskException();
      }
      if (numAttempts == MAX_RETRY) {
        // we give up on this write at this point.
        GNS.getLogger().severe(" ERROR: Max retries reached: Active name servers not written. ");
        throw  new CancelExecutorTaskException();
      }

      if (Config.debugMode) GNS.getLogger().info("PROPOSAL write active NS running. Version " + version +
              "\tnumAttempts\t" + numAttempts);

      // otherwise propose again.
      GroupChangeCompletePacket proposePacket = new GroupChangeCompletePacket(version, name);

      // write to replica controller record object using replica controller coordination that newActive is running
      replicaController.getNioServer().sendToID(replicaController.getNodeID(), proposePacket.toJSONObject());
    } catch (Exception e) {
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw  new RuntimeException(); // this is the only way to terminate this task as a ExecutorService is
        // running this task
      }
      // log error msg for other types of exceptions.
      GNS.getLogger().severe("Exception: exception in WriteActiveNameServersRunningTask. " + e.getMessage());
      e.printStackTrace();
      throw new RuntimeException(); // terminate this task.
    }
  }

}
