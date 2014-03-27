package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.recordmap.NameRecord;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.packet.OldActiveSetStopPacket;
import org.json.JSONException;

import java.util.ArrayList;

/**
 * This class removes the record from all active replicas in the process of removing a record from GNS.
 * One of the active replica starts the process of removing a record, after it receives a message from replica
 * controller. That active replica is responsible for confirming to the replica controller after record is
 * removed.
 * <p>
 * We do not actually remove the record from database, but update some fields in the name record to indicate that
 * that this node is no longer a valid active replica. This update operation is exactly the same operation that is
 * done when stopping the old set of active replicas on a group change.
 * <p>
 * The remove message from replica controller includes a version number of active replica. The remove
 * operation is done only if the name record in database has same version number of active replicas as the incoming
 * message. (This version number is currently embedded in the field ACTIVE_PAXOS_ID of name record.)
 * A mismatch between the two version numbers should not occur usually, but can happen if the active replica
 * receives duplicate copies of remove record message, or this replica never received the initial message indicating
 * that it is an active replica.
 * <p>
 * Future work:
 * (1) Implement a daemon thread that removes name records for which this node is no longer an active replica.
 * (2) Replace the ACTIVE_PAXOS_ID and OLD_ACTIVE_PAXOS_ID fields with their version numbers.
 * <p>
 * Created by abhigyan on 2/27/14.
 */
public class Remove {

  private static  ArrayList<ColumnField> activePaxosStopFields = new ArrayList<ColumnField>();

  static {
    activePaxosStopFields.add(NameRecord.ACTIVE_PAXOS_ID);
    activePaxosStopFields.add(NameRecord.VALUES_MAP);
  }

  private static ArrayList<ColumnField> oldActiveStopFields = new ArrayList<ColumnField>();

  static {
    oldActiveStopFields.add(NameRecord.ACTIVE_PAXOS_ID);
    oldActiveStopFields.add(NameRecord.OLD_ACTIVE_PAXOS_ID);
    oldActiveStopFields.add(NameRecord.ACTIVE_NAMESERVERS);
  }

  /**
   * Handles a request from replica controller to remove the record. It reads the current name record from database
   * to check if version number of active replica in the database matches that in the incoming packet, in which case
   * it proceeds with the remove request. In case this remove request has already been executed, a response is
   * immediately sent back to the replica controller.
   * @param oldActiveStopPacket
   * @param replica
   * @return
   * @throws JSONException
   */
  public static GNSMessagingTask handleActiveRemovePacket(OldActiveSetStopPacket oldActiveStopPacket,
                                                          GnsReconfigurable replica) throws JSONException {

    GNSMessagingTask msgTask = null;

    GNS.getLogger().info("Received Old Active Stop Packet: " + oldActiveStopPacket);
    String paxosID = oldActiveStopPacket.getPaxosIDToBeStopped();
    // if this is current active:

    NameRecord nameRecord1;
    try {
      nameRecord1 = NameRecord.getNameRecordMultiField(replica.getDB(), oldActiveStopPacket.getName(), oldActiveStopFields);
      GNS.getLogger().info("NAME RECORD NOW: " + nameRecord1);
      int paxosStatus = nameRecord1.getPaxosStatus(paxosID);
      GNS.getLogger().info("PaxosIDtoBeStopped = " + paxosID + " PaxosStatus = " + paxosStatus);
      if (paxosStatus == 1) { // this paxos ID is current active
        // propose STOP command for this paxos instance
        // Change Packet Type in oldActiveStop: This will help paxos identify that
        // this is a stop packet. See: PaxosManager.isStopCommand()

        if (replica.getActiveCoordinator() == null) {
          msgTask = handleActivePaxosStop(oldActiveStopPacket, replica);
        } else {
          replica.getActiveCoordinator().handleRequest(oldActiveStopPacket);
          GNS.getLogger().info("PAXOS PROPOSE: STOP Current Active Set. Paxos ID = " + paxosID);
        }

      } else if (paxosStatus == 2) { // this is the old paxos ID
        // send confirmation to primary that this paxos ID is stopped.
        msgTask = getActiveRemovedConfirmationMsg(oldActiveStopPacket, replica);
      } else {
        // if new active start packet comes before old active stop is committed, this situation might arise.
        GNS.getLogger().info("PAXOS ID Neither current nor old. Ignore msg = " + paxosID);
      }
    } catch (RecordNotFoundException e) {
      GNS.getLogger().info("Record not found exception. Name = " + oldActiveStopPacket.getName());
      // we should send error message to replica-controller in this case. but we haven't defined it.
    } catch (FieldNotFoundException e) {
      GNS.getLogger().info("FieldNotFoundException: " + e.getMessage());
      e.printStackTrace();
    }
    return msgTask;
  }

  /**
   * Updates the database to indicate that this node is no longer an active replica, which effectively removes the
   * record from this active replica.
   */
  public static GNSMessagingTask handleActivePaxosStop(OldActiveSetStopPacket oldActiveStopPacket, GnsReconfigurable replica) {
    GNSMessagingTask msgTask = null;
    GNS.getLogger().info("Active removed: Name = " + oldActiveStopPacket.getName() + "\t" + oldActiveStopPacket);
    String paxosID = oldActiveStopPacket.getPaxosIDToBeStopped();

    NameRecord nameRecord;
    try {
      nameRecord = NameRecord.getNameRecordMultiField(replica.getDB(), oldActiveStopPacket.getName(), activePaxosStopFields);
      nameRecord.handleCurrentActiveStop(paxosID);
      msgTask = getActiveRemovedConfirmationMsg(oldActiveStopPacket, replica);
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Record not found exception. Message = " + e.getMessage());
    } catch (FieldNotFoundException e) {
      GNS.getLogger().info("FieldNotFoundException. " + e.getMessage());
      e.printStackTrace();
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return msgTask;
  }

  /**
   * After actives have removed the record, this method creates the confirmation message that we will send to
   * replica controller.
   */
  private static GNSMessagingTask getActiveRemovedConfirmationMsg(OldActiveSetStopPacket oldActiveStopPacket,
                                                                  GnsReconfigurable replica) throws JSONException {
    GNSMessagingTask msgTask = null;
    // confirm to primary name server that this set of actives has stopped
    if (oldActiveStopPacket.getActiveReceiver() == replica.getNodeID()) {
      // the active node who received this node, sends confirmation to primary
      // confirm to primary
      oldActiveStopPacket.changePacketTypeToActiveRemoved();
      msgTask = new GNSMessagingTask(oldActiveStopPacket.getPrimarySender(), oldActiveStopPacket.toJSONObject());
      GNS.getLogger().info("Active removed: Name Record updated. Sent confirmation to replica controller. Packet = " +
              oldActiveStopPacket);
    } else {
      // other nodes do nothing.
      GNS.getLogger().info("Active removed: Name Record updated. OldPaxosID = "
              + oldActiveStopPacket.getPaxosIDToBeStopped());
    }
    return msgTask;
  }

}
