package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.packet.AddRecordPacket;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.NSResponseCode;
import edu.umass.cs.gns.packet.Packet;
import org.json.JSONException;

/*** DONT not use any class in package edu.umass.cs.gns.nsdesign ***/

/**
 * Includes code which a replica controller executes to add a name to GNS. If name servers are replicated,
 * then methods in this class will be executed after  the coordination among replica controllers at name servers
 * is complete.
 * <p/>
 * This class contains static methods that will be called by ReplicaController.
 * <p/>
 * An add request is first received by a replica controller, but it also needs to be executed at active replicas.
 * Initially, active replicas are co-located with replica controllers therefore replica controller forwards an add
 * request to the GnsReconfigurable at the same node.
 * <p/>
 * <p/>
 * Created by abhigyan on 2/27/14.
 */
public class Add {

  /**
   * Adds a name in the database at this replica controller and forwards the request to the active replica at
   * the same node. If name exists in database, it will send error message to the client.
   *
   * @param addRecordPacket   Packet sent by client.
   * @param replicaController ReplicaController object calling this method.
   */
  public static GNSMessagingTask executeAddRecord(AddRecordPacket addRecordPacket, ReplicaController replicaController)
          throws JSONException {

    GNSMessagingTask gnsMessagingTask;
    GNS.getLogger().info("Executing ADD at replica controller " + addRecordPacket + " Local name server ID = " + addRecordPacket.getLocalNameServerID());
    ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(replicaController.getDB(),
            addRecordPacket.getName(), true);
    // change packet type
    try {
      ReplicaControllerRecord.addNameRecordPrimary(replicaController.getDB(), rcRecord);
      addRecordPacket.setType(Packet.PacketType.ACTIVE_ADD);
      gnsMessagingTask = new GNSMessagingTask(replicaController.getNodeID(), addRecordPacket.toJSONObject());

    } catch (RecordExistsException e) {
      // send error to client
      ConfirmUpdateLNSPacket confirmPkt = new ConfirmUpdateLNSPacket(NSResponseCode.ERROR, addRecordPacket);
      gnsMessagingTask = new GNSMessagingTask(addRecordPacket.getLocalNameServerID(), confirmPkt.toJSONObject());
    }
    return gnsMessagingTask;
  }

  /**
   * Method is called after GnsReconfigurable confirms it has added the record. This methods sends confirmation
   * to local name server that record is added.
   */
  public static GNSMessagingTask executeAddActiveConfirm(AddRecordPacket addRecordPacket,
                                                         ReplicaController replicaController)
          throws JSONException {

    GNS.getLogger().info("Add complete informing client. " + addRecordPacket + " Local name server ID = " + addRecordPacket.getLocalNameServerID());
    ConfirmUpdateLNSPacket confirmPkt = new ConfirmUpdateLNSPacket(NSResponseCode.NO_ERROR, addRecordPacket);
    return new GNSMessagingTask(addRecordPacket.getLocalNameServerID(), confirmPkt.toJSONObject());
  }
}
