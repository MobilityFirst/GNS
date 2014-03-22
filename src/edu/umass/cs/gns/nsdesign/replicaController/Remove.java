package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.NSResponseCode;
import edu.umass.cs.gns.packet.RemoveRecordPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

/**
 * Includes code which a replica controller executes to remove a name from GNS. If replica controllers are replicated
 * and use coordination, then methods in this class will be executed after the coordination among them is complete.
 * <p>
 * This class contains static methods that will be called by ReplicaController.
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


  public static GNSMessagingTask handleRemoveRecordLNS(JSONObject json, ReplicaController replicaController) throws JSONException{

    RemoveRecordPacket removeRecord = new RemoveRecordPacket(json);

    ArrayList<ColumnField> readFields = new ArrayList<ColumnField>();
    readFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);

    try {
      ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
              replicaController.getReplicaControllerDB(), removeRecord.getName(), readFields);
      if (rcRecord.isRemoved()) { // if removed, send confirm to client
        ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(NSResponseCode.NO_ERROR, removeRecord);
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("Record already remove. Sent confirmation to client. Name = " + removeRecord.getName());
        }
        return new GNSMessagingTask(removeRecord.getLocalNameServerID(), confirmPacket.toJSONObject());
      }

      if (rcRecord.isMarkedForRemoval()) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("Already marked for removal. Name record will be deleted soon. So request is dropped.");
        }
        return null;
      }

    } catch (RecordNotFoundException e) {
      // return failure, because record was not even found in deleted state
      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(NSResponseCode.ERROR, removeRecord);

      if (StartNameServer.debugMode) {
        GNS.getLogger().info("Record not found. Sent failure confirmation to client. Name = " + removeRecord.getName());
      }
      GNS.getLogger().severe(" REMOVE RECORD ERROR!! Name: " + removeRecord.getName());
      return new GNSMessagingTask(removeRecord.getLocalNameServerID(), confirmPacket.toJSONObject());
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("ColumnField not found exception. " + e.getMessage());
      e.printStackTrace();
      return null;
    }

    if (replicaController.getRcCoordinator() == null) {
      return executeRemoveRecord(removeRecord, replicaController);
    } else {
      replicaController.getRcCoordinator().handleRequest(removeRecord.toJSONObject());
      return null;
    }

  }

  /**
   * Executes the first phase of remove operation, which updates record to say the name is going to be removed.
   * This method also forwards request to current active replicas to remove the record.
   * @param removeRecordPacket Packet sent by client
   * @param replicaController ReplicaController calling this method
   */
  public static GNSMessagingTask executeRemoveRecord(RemoveRecordPacket removeRecordPacket, ReplicaController replicaController) {
    GNS.getLogger().info("Processing REMOVE " + removeRecordPacket);
    return null;
  }


  public static GNSMessagingTask executeActiveReplicaStop
  /**
   * Executes second phase of remove operation at replica controllers, which finally removes the record from database
   * and sends confirmation to local name server.
   * @param removeRecordPacket Packet sent by client
   * @param replicaController ReplicaController calling this method
   */
  public static void executeRemoveActiveConfirm(RemoveRecordPacket removeRecordPacket, ReplicaController replicaController) {

  }
}
