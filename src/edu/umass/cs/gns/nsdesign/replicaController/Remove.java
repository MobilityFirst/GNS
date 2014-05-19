/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved
 */
package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FailedUpdateException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.RemoveRecordPacket;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.util.NSResponseCode;
import org.json.JSONException;

import java.io.IOException;
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

  private static ArrayList<ColumnField> applyMarkedForRemovalFields = new ArrayList<ColumnField>();

  static {
    applyMarkedForRemovalFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
    applyMarkedForRemovalFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
    applyMarkedForRemovalFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
    applyMarkedForRemovalFields.add(ReplicaControllerRecord.ACTIVE_VERSION);
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
   * @param recovery  true if we are replaying logs during recovery
   */
  public static void executeMarkRecordForRemoval(RemoveRecordPacket removeRecord, ReplicaController rc,
          boolean recovery) throws JSONException, IOException {
    boolean sendError = false;
    NSResponseCode respCode = NSResponseCode.ERROR;
    try {
      ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(rc.getDB(),
              removeRecord.getName(), applyMarkedForRemovalFields);

      rcRecord.setMarkedForRemoval();
      if (!recovery) {
        if (rcRecord.isMarkedForRemoval()) {  // check if record marked as removed, it may not be if a group change for
          //  this name is in progress concurrently.
          if (Config.debugMode) GNS.getLogger().info("Name Record marked for removal " + removeRecord);

          if (removeRecord.getNameServerID() == rc.getNodeID()) { // this node received packet from client,
            // so it will inform actives
            assert rcRecord.isActiveRunning(); // active must be running
            StopActiveSetTask stopActiveSetTask = new StopActiveSetTask(removeRecord.getName(),
                    rcRecord.getActiveNameservers(), rcRecord.getActiveVersion(), Packet.PacketType.ACTIVE_REMOVE,
                    removeRecord, rc);
            rc.getScheduledThreadPoolExecutor().scheduleAtFixedRate(stopActiveSetTask, 0,
                    ReplicaController.RC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

          } else {
            if (Config.debugMode) GNS.getLogger().info("SKIP: remove record request does not not contain node ID " + rcRecord.getName());
          }
        } else {
          GNS.getLogger().info("Remove record not executed because group change for the name is in progress "
                  + removeRecord);
          sendError = true;
        }
      }
    } catch (FailedUpdateException e) {
      sendError = true;
      GNS.getLogger().info("Error during update. Sent failure confirmation to client. Name = " + removeRecord.getName());
    } catch (RecordNotFoundException e) {
      sendError = true;
      GNS.getLogger().info("Record not found. Sent failure confirmation to client. Name = " + removeRecord.getName());
    } catch (FieldNotFoundException e) {
      if (Config.debugMode) GNS.getLogger().severe("Field Not Found Exception. " + e.getMessage());
      e.printStackTrace();
    }
    if (removeRecord.getNameServerID() == rc.getNodeID() && sendError & !recovery) {
      ConfirmUpdatePacket failPacket = new ConfirmUpdatePacket(respCode, removeRecord);
      rc.getNioServer().sendToID(removeRecord.getLocalNameServerID(), failPacket.toJSONObject());
    }
  }

  /**
   * Actives have removed the record, so remove the requestID from the list of ongoing stop active requests.
   */
  public static void handleActiveRemoveRecord(OldActiveSetStopPacket activeStop, ReplicaController rc,
          boolean recovery) throws JSONException, IOException {
    if (!recovery) {
      if (Config.debugMode) GNS.getLogger().fine("RC handling active remove record ... " + activeStop);
      // response received for active stop request, so remove from set, which will cancel the OldActiveSetStopPacket task
      RemoveRecordPacket removePacket = (RemoveRecordPacket) rc.getOngoingStopActiveRequests().remove(activeStop.getRequestID());
      if (Config.debugMode) GNS.getLogger().fine("RC remove packet fetched ... " + removePacket);
      if (removePacket != null) { // response has not been already received
        removePacket.changePacketTypeToRcRemove();
        rc.getNioServer().sendToID(rc.getNodeID(), removePacket.toJSONObject());
      } else {
        GNS.getLogger().info("Duplicate or delayed response for old active stop: " + activeStop);
      }
    }
  }

  /**
   * Executes second phase of remove operation at replica controllers, which finally removes the record from database
   * and sends confirmation to local name server.
   * This method is executed after active replicas have stopped and removed the record.
   *
   * @param removeRecordPacket Packet sent by client
   * @param rc ReplicaController calling this method
   *
   */
  public static void executeRemoveRecord(RemoveRecordPacket removeRecordPacket, ReplicaController rc,
          boolean recovery) throws JSONException, FailedUpdateException, IOException {
    if (Config.debugMode) GNS.getLogger().fine("DECISION executing remove record at RC: " + removeRecordPacket);
    rc.getDB().removeNameRecord(removeRecordPacket.getName());

    if (removeRecordPacket.getNameServerID() == rc.getNodeID() && !recovery) { // this will be true at the replica controller who
      // first received the client's request
      ConfirmUpdatePacket confirmPacket = new ConfirmUpdatePacket(NSResponseCode.NO_ERROR, removeRecordPacket);
      rc.getNioServer().sendToID(removeRecordPacket.getLocalNameServerID(), confirmPacket.toJSONObject());
      if (Config.debugMode) GNS.getLogger().fine("Remove record response sent to LNS: " + removeRecordPacket.getName() + " lns "
              + removeRecordPacket.getLocalNameServerID());
    }
  }
}
