package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.replicationframework.BeehiveReplication;
import edu.umass.cs.gns.nsdesign.replicationframework.RandomReplication;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;
import edu.umass.cs.gns.util.NSResponseCode;
import org.json.JSONException;

import java.io.IOException;
import java.util.Set;

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
          throws JSONException, FailedDBOperationException, IOException {

    if (Config.debuggingEnabled) GNS.getLogger().fine("Executing ADD at replica controller " + addRecordPacket +
            " Local name server address = " + addRecordPacket.getLnsAddress());
    if (recovery) ReplicaControllerRecord.removeNameRecordPrimary(replicaController.getDB(), addRecordPacket.getName());

    ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(replicaController.getDB(), addRecordPacket.getName(), true);

    // if clauses used only during experiments
    if (addRecordPacket.getActiveNameServers() != null){
      rcRecord = new ReplicaControllerRecord(replicaController.getDB(), addRecordPacket.getName(),
              addRecordPacket.getActiveNameServers(), true);
    } else if (Config.replicationFrameworkType.equals(ReplicationFrameworkType.BEEHIVE) ||
            Config.replicationFrameworkType.equals(ReplicationFrameworkType.STATIC) ) {
      rcRecord = new ReplicaControllerRecord(replicaController.getDB(), addRecordPacket.getName(),
              getInitialReplicasForOtherReplicationSchemes(addRecordPacket.getName(), replicaController), true);
    }

    try {
      ReplicaControllerRecord.addNameRecordPrimary(replicaController.getDB(), rcRecord);

      //FUCK THIS SHIT!!!
      if (addRecordPacket.getNameServerID().equals(replicaController.getNodeID())) {
        if (!recovery) {
          // change packet type and inform all active replicas.
          addRecordPacket.setType(Packet.PacketType.ACTIVE_ADD);
          addRecordPacket.setActiveNameServers(rcRecord.getActiveNameservers());
          if (Config.debuggingEnabled) GNS.getLogger().fine("Name: " + rcRecord.getName() + " Initial active replicas: " + rcRecord.getActiveNameservers());
          for (NodeId<String> nodeID: rcRecord.getActiveNameservers()) {
            replicaController.getNioServer().sendToID(nodeID, addRecordPacket.toJSONObject());
          }
        }

        ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(NSResponseCode.NO_ERROR, addRecordPacket);
        if (Config.debuggingEnabled) GNS.getLogger().fine("Add complete informing client. " + addRecordPacket
                + " Local name server address = " + addRecordPacket.getLnsAddress() + "Response code: " + confirmPkt);
        if (!recovery) {
          replicaController.getNioServer().sendToAddress(addRecordPacket.getLnsAddress(), confirmPkt.toJSONObject());
        }
      }
//    } catch (FailedDBOperationException e) {
//      // send error to client
//      ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(NSResponseCode.ERROR, addRecordPacket);
//      if (!recovery && addRecordPacket.getNameServerID().equals(replicaController.getNodeID())) {
//        replicaController.getNioServer().sendToID(addRecordPacket.getLocalNameServerID(), confirmPkt.toJSONObject());
//      }
    } catch (RecordExistsException e) {
      if (addRecordPacket.getNameServerID().equals(replicaController.getNodeID())) {
        // send error to client
        ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(NSResponseCode.ERROR, addRecordPacket);
        if (Config.debuggingEnabled)
          GNS.getLogger().fine("Record exists. sending failure: name = " + addRecordPacket.getName() + " Local name server address = " +
                  addRecordPacket.getLnsAddress() + "Response code: " + confirmPkt);
        if (!recovery) {
          replicaController.getNioServer().sendToAddress(addRecordPacket.getLnsAddress(), confirmPkt.toJSONObject());
        }
      }
    } catch (FieldNotFoundException e) {
      e.printStackTrace();
    }
  }

  private static Set<NodeId<String>> getInitialReplicasForOtherReplicationSchemes(String name, ReplicaController rc) {

    if (Config.replicationFrameworkType.equals(ReplicationFrameworkType.STATIC)) {
      GNS.getLogger().fine("Using static replication: " + name);
      // select replicas randomly
      try {
        return new RandomReplication().newActiveReplica(rc, new ReplicaControllerRecord(rc.getDB(), name), GNS.numPrimaryReplicas, 0).getReplicas();
      } catch (FieldNotFoundException e) {
        e.printStackTrace();
      }
    }

    if (Config.replicationFrameworkType.equals(ReplicationFrameworkType.BEEHIVE)) {
      GNS.getLogger().fine("Using beehive replication: " + name);
      // Decide number of replicas according to beehive's algorithm. and choose them randomly.
      try {
        int numReplicas = Math.min(Config.minReplica, BeehiveReplication.numActiveNameServers(name));
        return new RandomReplication().newActiveReplica(rc, new ReplicaControllerRecord(rc.getDB(), name), numReplicas, 0).getReplicas();
      } catch (FieldNotFoundException e) {
        e.printStackTrace();
      }
    }
    return null;
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
