package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.packet.AddRecordPacket;

/**
 * Includes code which a replica controller executes to add a name to GNS. If name servers are replicated,
 * then methods in this class will be executed after  the coordination among replica controllers at name servers
 * is complete.
 * <p>
 * This class contains static methods that will be called by ReplicaController.
 * <p>
 * An add request is first received by a replica controller, but it also needs to be executed at active replicas.
 * Initially, active replicas are co-located with replica controllers therefore replica controller forwards an add
 * request to the ActiveReplica at the same node.
 * <p>
 *
 * Created by abhigyan on 2/27/14.
 */
public class Add {

  /**
   * Adds a name in the database at this replica controller and forwards the request to the active replica at the same node.
   *
   * @param addRecordPacket Packet sent by client.
   * @param replicaController ReplicaController object calling this method.
   */
  public static void executeAddRecord(AddRecordPacket addRecordPacket, ReplicaController replicaController) {

  }

  /**
   * Method is called after ActiveReplica confirms it has added the record. This methods sends confirmation
   * to local name server.
   */
  public static void executeAddActiveConfirm(AddRecordPacket addRecordPacket, ReplicaController replicaController) {

  }
}
