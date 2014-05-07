package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.exceptions.FailedUpdateException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.util.NSResponseCode;
import org.json.JSONException;

import java.io.IOException;

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
  public static void executeAddRecord(AddRecordPacket addRecordPacket, ReplicaController replicaController,
                                                  boolean recovery)
          throws JSONException, FailedUpdateException, IOException {

    if (Config.debugMode) GNS.getLogger().fine("Executing ADD at replica controller " + addRecordPacket +
            " Local name server ID = " + addRecordPacket.getLocalNameServerID());
    if (recovery) ReplicaControllerRecord.removeNameRecordPrimary(replicaController.getDB(), addRecordPacket.getName());
    ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(replicaController.getDB(), addRecordPacket.getName(),
            true);
    // change packet type
    try {
      ReplicaControllerRecord.addNameRecordPrimary(replicaController.getDB(), rcRecord);
      addRecordPacket.setType(Packet.PacketType.ACTIVE_ADD);
      if (!recovery) {
        replicaController.getNioServer().sendToID(replicaController.getNodeID(), addRecordPacket.toJSONObject());
      }

      if (addRecordPacket.getNameServerID() == replicaController.getNodeID()) {
        ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(NSResponseCode.NO_ERROR, addRecordPacket);
        if (Config.debugMode) GNS.getLogger().fine("Add complete informing client. " + addRecordPacket + " Local name server ID = " +
                addRecordPacket.getLocalNameServerID() + "Response code: " + confirmPkt);
        if (!recovery) {
          replicaController.getNioServer().sendToID(addRecordPacket.getLocalNameServerID(), confirmPkt.toJSONObject());
        }
      }
    } catch (FailedUpdateException e) {
      // send error to client
      ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(NSResponseCode.ERROR, addRecordPacket);
      if (!recovery) {
        replicaController.getNioServer().sendToID(addRecordPacket.getLocalNameServerID(), confirmPkt.toJSONObject());
      }
    } catch (RecordExistsException e) {
      if (addRecordPacket.getNameServerID() == replicaController.getNodeID()) {
        // send error to client
        ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(NSResponseCode.ERROR, addRecordPacket);
        if (Config.debugMode)
          GNS.getLogger().fine("Record exists. sending failure: name = " + addRecordPacket.getName() + " Local name server ID = " +
                  addRecordPacket.getLocalNameServerID() + "Response code: " + confirmPkt);
        if (!recovery) {
          replicaController.getNioServer().sendToID(addRecordPacket.getLocalNameServerID(), confirmPkt.toJSONObject());
        }
      }
    }
  }

  /**
   * Method is called after GnsReconfigurable confirms it has added the record. This methods sends confirmation
   * to local name server that record is added.
   */
  public static void executeAddActiveConfirm(AddRecordPacket addRecordPacket,
                                                         ReplicaController replicaController)
          throws JSONException {
    // no action needed.

  }
}
