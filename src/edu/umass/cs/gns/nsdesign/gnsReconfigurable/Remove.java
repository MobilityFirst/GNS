/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved
 */
package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import org.json.JSONException;

import java.io.IOException;
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
 * message. (This version number is currently embedded in the field ACTIVE_VERSION of name record.)
 * A mismatch between the two version numbers should not occur usually, but can happen if the active replica
 * receives duplicate copies of remove record message, or this replica never received the initial message indicating
 * that it is an active replica.
 * <p>
 * Future work:
 * (1) Implement a daemon thread that removes name records for which this node is no longer an active replica.
 * (2) Replace the ACTIVE_VERSION and OLD_ACTIVE_VERSION fields with their version numbers.
 * <p>
 * Created by abhigyan on 2/27/14.
 */
public class Remove {


  private static  ArrayList<ColumnField> activeStopFields = new ArrayList<ColumnField>();

  static {
    activeStopFields.add(NameRecord.ACTIVE_VERSION);
    activeStopFields.add(NameRecord.VALUES_MAP);
  }


  private static ArrayList<ColumnField> versionFields = new ArrayList<ColumnField>();

  static {
    versionFields.add(NameRecord.ACTIVE_VERSION);
    versionFields.add(NameRecord.OLD_ACTIVE_VERSION);
  }

  /**
   * Handles a request from replica controller to remove the record. It reads the current name record from database
   * to check if version number of active replica in the database matches that in the incoming packet, in which case
   * it proceeds with the remove request. In case this remove request has already been executed, a response is
   * immediately sent back to the replica controller.
   *
   * TODO update this doc
   * Updates the database to indicate that this node is no longer an active replica, which effectively removes the
   * record from this active replica.
   */
  public static void executeActiveRemove(OldActiveSetStopPacket oldActiveStopPacket, GnsReconfigurable gnsApp,
                                                     boolean noCoordinationState, boolean recovery) throws IOException, FailedDBOperationException {
    if (Config.debuggingEnabled) GNS.getLogger().fine("Executing remove: " + oldActiveStopPacket);
//    GNSMessagingTask msgTask = null;
    if (noCoordinationState == false) { // normal case:
      try {
        NameRecord nameRecord = NameRecord.getNameRecordMultiField(gnsApp.getDB(), oldActiveStopPacket.getName(),
                activeStopFields);
        nameRecord.handleCurrentActiveStop();
        sendActiveRemovedConfirmationMsg(oldActiveStopPacket, gnsApp, recovery);
      } catch (JSONException e) {
        e.printStackTrace();
      } catch (FieldNotFoundException e) {
        e.printStackTrace();
      } catch (RecordNotFoundException e) {
        e.printStackTrace();
      }
//      catch (FailedDBOperationException e) {
//        GNS.getLogger().severe("Failed update exception: " + e.getMessage());
//        e.printStackTrace();
//      }
    } else {  // exceptional case: either request is retransmitted by RC. or this replica never received any state for
    // this name.
      try {
        int version = oldActiveStopPacket.getVersion();
        NameRecord nameRecord1 = NameRecord.getNameRecordMultiField(gnsApp.getDB(), oldActiveStopPacket.getName(),
                versionFields);
        int versionStatus = nameRecord1.getVersionStatus(version);
        GNS.getLogger().severe("Version to Be Stopped = " + version + " VersionStatus = " + versionStatus + " name = "
                + oldActiveStopPacket.getName());
        // todo make enum out of constants: 1, 2, 3
        if (versionStatus == 1) { // this is the current active version
          GNS.getLogger().severe("Case cannot happen because coordinator state would have exist = " +
                  oldActiveStopPacket);
        } else if (versionStatus == 2) { // this is the old version
          // send confirmation to primary that this version is stopped.
          sendActiveRemovedConfirmationMsg(oldActiveStopPacket, gnsApp, recovery);
        } else {
          // if new active start packet comes before old active stop is committed, this situation might arise.
          GNS.getLogger().severe("Version Neither current nor old. Ignore msg = " + version);
        }
      } catch (RecordNotFoundException e) {
        GNS.getLogger().info("Record not found exception. Name = " + oldActiveStopPacket.getName());
        // we should send error message to replica-controller in this case. but we haven't defined it.
      } catch (FieldNotFoundException e) {
        GNS.getLogger().severe("FieldNotFoundException: " + e.getMessage());
        e.printStackTrace();
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * After actives have removed the record, this method creates the confirmation message that we will send to
   * replica controller.
   */
  private static void sendActiveRemovedConfirmationMsg(OldActiveSetStopPacket oldActiveStopPacket,
                                                                  GnsReconfigurable gnsApp, boolean recovery)
          throws JSONException, IOException {
    // confirm to primary name server that this set of actives has stopped
    if (oldActiveStopPacket.getActiveReceiver() == gnsApp.getNodeID()) {
      // the active node who received this node, sends confirmation to primary
      // confirm to primary
      oldActiveStopPacket.changePacketTypeToActiveRemoved();
      if (!recovery) {
        gnsApp.getNioServer().sendToID(oldActiveStopPacket.getPrimarySender(), oldActiveStopPacket.toJSONObject());
      }
      GNS.getLogger().fine("Active removed: Name Record updated. Sent confirmation to replica controller. Packet = " +
              oldActiveStopPacket);
    } else {
      // other nodes do nothing.
      GNS.getLogger().fine("Active removed: Name Record updated. OldVersion = " + oldActiveStopPacket.getVersion());
    }
  }

}
