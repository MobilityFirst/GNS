package edu.umass.cs.gns.nsdesign.activeReplica;

import edu.umass.cs.gns.packet.RemoveRecordPacket;

/**
 * This class removes the record from all active replicas in the process of removing a record from GNS.
 * One of the active replica starts the process of removing a record, after it receives a message from replica
 * controller. That active replica is responsible for confirming to the replica controller after record is
 * removed.
 * <p>
 * Created by abhigyan on 2/27/14.
 */
public class Remove {

  public static void executeRemoveLocal(RemoveRecordPacket packet, ActiveReplica replica) {

  }
}
