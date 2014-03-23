package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.packet.*;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * A replica controller executes these methods to remove a name from GNS. If replica controllers are replicated
 * and use coordination, then methods in this class will be executed after the coordination among them is complete.
 * <p>
 * Like an add request, remove request is first received by a replica controller, but also needs to be executed at
 * active replicas. A replica controller makes two database operations to remove a record. First operation updates
 * record to say the name is going to be removed. After this, ReplicaController waits from confirmation from active
 * replicas of that name that they have removed the record. Second operation then finally removes the record at replica
 * controllers.
 * <p>
 * We need two operations to remove a record because the set of active replicas for a name can change.
 * After first operation is over, we do not allow any changes to the set of active replicas for a name. For the
 * remove request to complete, the current set of active replicas after the first operation must remove the record
 * from database. Replica controllers finally remove the record after the current active replicas have removed the record.
 * <p>
 * Created by abhigyan on 2/27/14.
 */
public class Remove {

  private static ArrayList<ColumnField> removeRecordLNSFields = new ArrayList<ColumnField>();

  static {
    removeRecordLNSFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
  }

  private static ArrayList<ColumnField> applyMarkedForRemovalFields = new ArrayList<ColumnField>();

  static {
    applyMarkedForRemovalFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
    applyMarkedForRemovalFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
    applyMarkedForRemovalFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
    applyMarkedForRemovalFields.add(ReplicaControllerRecord.ACTIVE_PAXOS_ID);
  }

  /**
   * Handle a client request to remove a record. If record does not exist, an error message is sent to client.
   * Otherwise, replica controller proceed to mark the record as removed, after coordinating with other replica
   * controllers.
   */
  public static GNSMessagingTask handleRemoveRecordLNS(RemoveRecordPacket removeRecord, ReplicaController rc)
          throws JSONException{
    GNSMessagingTask msgTask = null;
    try {
      ReplicaControllerRecord.getNameRecordPrimaryMultiField(rc.getDB(), removeRecord.getName(), removeRecordLNSFields);
      // record exists
      removeRecord.setNameServerID(rc.getNodeID());
      if (rc.getRcCoordinator() == null) {
        msgTask = executeMarkRecordForRemoval(removeRecord, rc);
      } else {
        rc.getRcCoordinator().handleRequest(removeRecord.toJSONObject());
      }

    } catch (RecordNotFoundException e) {
      // return failure, because record was not even found in deleted state
      ConfirmUpdateLNSPacket failPacket = new ConfirmUpdateLNSPacket(NSResponseCode.ERROR, removeRecord);
      GNS.getLogger().severe("Record not found. Sent failure confirmation to client. Name = " + removeRecord.getName());
      msgTask = new GNSMessagingTask(removeRecord.getLocalNameServerID(), failPacket.toJSONObject());
    }
    return  msgTask;
  }

  /**
   * Executes the first phase of remove operation, which updates record to say it is going to be removed.
   * This method also forwards request to current active replicas to remove the record.
   * The request is not forwarded in the special case that a group change for this name is in
   * progress at the same time. In this case, we wait for the group change to complete before proceeding to remove the
   * record at active replicas.
   *
   * @param removeRecord Packet sent by client
   * @param rc ReplicaController calling this method
   */
  public static GNSMessagingTask executeMarkRecordForRemoval(RemoveRecordPacket removeRecord, ReplicaController rc)
          throws JSONException{
    GNSMessagingTask msgTask = null;

    try {
      ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(rc.getDB(),
              removeRecord.getName(), applyMarkedForRemovalFields);

      rcRecord.setMarkedForRemoval();

      GNS.getLogger().info("PAXOS DECISION applied. Name Record marked for removal " + removeRecord);

      if (removeRecord.getNameServerID() == rc.getNodeID()) { // this node received packet from client,
                                                              // so it will inform actives
        if (rcRecord.isActiveRunning()) { // if active is running, stop current actives
          StopActiveSetTask stopActiveSetTask = new StopActiveSetTask(removeRecord.getName(),
                  rcRecord.getActiveNameservers(),rcRecord.getActivePaxosID(), Packet.PacketType.ACTIVE_REMOVE,
                  removeRecord, rc);
          rc.getScheduledThreadPoolExecutor().scheduleAtFixedRate(stopActiveSetTask, 0,
                  ReplicaController.RC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        }
        // the else of the above if means active name servers not running, i.e., group change is in progress.
        // We let new actives become functional and then we will stop the new actives to remove the record.
      } else {
        GNS.getLogger().info("SKIP: remove record request does not not contain " + rcRecord.getName());
      }

    } catch (RecordNotFoundException e) {
      ConfirmUpdateLNSPacket failPacket = new ConfirmUpdateLNSPacket(NSResponseCode.ERROR, removeRecord);

      GNS.getLogger().info("Record not found. Sent failure confirmation to client. Name = " + removeRecord.getName());
      msgTask = new GNSMessagingTask(removeRecord.getLocalNameServerID(), failPacket.toJSONObject());
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field Not Found Exception. " + e.getMessage());
      e.printStackTrace();
    }
    return msgTask;
  }

  /**
   * Actives have removed the record, so remove the requestID from the list of ongoing stop active requests.
   */
  public static GNSMessagingTask handleActiveRemoveRecord(OldActiveSetStopPacket activeStop, ReplicaController rc) throws JSONException{
    GNS.getLogger().fine("RC handling active remove record ... " + activeStop);
    GNSMessagingTask msgTask = null;
    // response received for active stop request, so remove from set, which will cancel the OldActiveSetStopPacket task
    RemoveRecordPacket removePacket = (RemoveRecordPacket) rc.getOngoingStopActiveRequests().remove(activeStop.getRequestID());
    GNS.getLogger().fine("RC remove packet fetched ... " + removePacket);
    if (removePacket != null) { // response has not been already received
        if (rc.getRcCoordinator() == null) {
          msgTask = executeRemoveRecord(removePacket, rc);
        } else {
          rc.getRcCoordinator().handleRequest(activeStop);
        }
    } else {
      GNS.getLogger().info("Duplicate or delayed response for old active stop: " + activeStop);
    }
    return msgTask;
  }

  /**
   * Executes second phase of remove operation at replica controllers, which finally removes the record from database
   * and sends confirmation to local name server.
   * This method is executed after active replicas have stopped and removed the record.
   * @param removeRecordPacket Packet sent by client
   * @param rc ReplicaController calling this method
   */
  public static GNSMessagingTask executeRemoveRecord(RemoveRecordPacket removeRecordPacket, ReplicaController rc) throws JSONException{

    GNSMessagingTask msgTask = null;
    GNS.getLogger().fine("DECISION executing remove record at RC: " + removeRecordPacket);
    rc.getDB().removeNameRecord(removeRecordPacket.getName());

    if (removeRecordPacket.getNameServerID() == rc.getNodeID()) { // this will be true at the replica controller who
                                                                  // first received the client's request
      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(NSResponseCode.NO_ERROR, removeRecordPacket);
      msgTask = new GNSMessagingTask(removeRecordPacket.getLocalNameServerID(), confirmPacket.toJSONObject());
      GNS.getLogger().fine("Remove record response sent to LNS: " + removeRecordPacket.getName() + " lns " +
              removeRecordPacket.getLocalNameServerID());
    }
    return msgTask;
  }
}
