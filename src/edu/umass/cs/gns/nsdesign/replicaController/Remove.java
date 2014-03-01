package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.packet.RemoveRecordPacket;

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

  /**
   * Executes the first phase of remove operation, which updates record to say the name is going to be removed.
   * This method also forwards request to current active replicas to remove the record.
   * @param removeRecordPacket Packet sent by client
   * @param replicaController ReplicaController calling this method
   */
  public static void executeRemoveRecord(RemoveRecordPacket removeRecordPacket, ReplicaController replicaController) {
    GNS.getLogger().info("Processing REMOVE " + removeRecordPacket);
  }

  /**
   * Executes second phase of remove operation at replica controllers, which finally removes the record from database
   * and sends confirmation to local name server.
   * @param removeRecordPacket Packet sent by client
   * @param replicaController ReplicaController calling this method
   */
  public static void executeRemoveActiveConfirm(RemoveRecordPacket removeRecordPacket, ReplicaController replicaController) {

  }
}
