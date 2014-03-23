package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.packet.AddRecordPacket;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.NSResponseCode;
import edu.umass.cs.gns.packet.UpdateAddressPacket;
import edu.umass.cs.gns.util.BestServerSelection;
import org.json.JSONException;

import java.io.IOException;
import java.util.Set;

/**
 * Handles the upsert case of UpdateAddressPacket. If the record exists in the replica controller database, upsert is
 * converted to an update operation and forwarded to an active replica (on a possibly remote node) to execute the update.
 * Otherwise, upsert is convert to an add operation and is executed by the replica controller module.
 *
 *
 * Created by abhigyan on 3/21/14.
 */
public class Upsert {


  public static GNSMessagingTask handleUpsert(UpdateAddressPacket updatePacket, ReplicaController replicaController) throws JSONException, IOException {
    // this must be primary
    //
    GNS.getLogger().fine("<>>>>>>>>>>>>>>>>>> Handling upsert case ....... ");
    GNSMessagingTask msgTask = null;
    ReplicaControllerRecord nameRecordPrimary;
    try {
      nameRecordPrimary = ReplicaControllerRecord.getNameRecordPrimaryMultiField(replicaController.getDB(), updatePacket.getName(),
              ReplicaControllerRecord.MARKED_FOR_REMOVAL, ReplicaControllerRecord.ACTIVE_NAMESERVERS);
      if (nameRecordPrimary.isMarkedForRemoval()) {
        ConfirmUpdateLNSPacket failConfirmPacket = ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NSResponseCode.ERROR);
        msgTask = new GNSMessagingTask(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(" UPSERT-FAILED because name record deleted already\t" + updatePacket.getName()
                  + "\t" + replicaController.getNodeID() + "\t" + updatePacket.getLocalNameServerId());// + "\t" + updatePacket.getSequenceNumber());
        }
      } else {

        // record does not exist, so we can do an ADD
        int activeID = -1;
        Set<Integer> activeNS;
        activeNS = nameRecordPrimary.getActiveNameservers();
        if (activeNS != null) {
          activeID = BestServerSelection.getSmallestLatencyNS(activeNS, null);
        }

        if (activeID != -1) {
          // forward update to active NS
          // updated to use a less kludgey operation - Westy
          if (updatePacket.getOperation().isUpsert()) {
            updatePacket.setOperation(updatePacket.getOperation().getNonUpsertEquivalent());
          }
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("UPSERT forwarded as UPDATE to active: " + activeID);
          }
          msgTask = new GNSMessagingTask(activeID, updatePacket.toJSONObject());
          // could not find activeNS for this name
        } else {
          // send error to LNS
          ConfirmUpdateLNSPacket failConfirmPacket =
                  ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NSResponseCode.ERROR);
          msgTask = new GNSMessagingTask(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine(" UPSERT-FAILED\t" + updatePacket.getName() + "\t" + replicaController.getNodeID() +
                    "\t" + updatePacket.getLocalNameServerId());
          }
        }
      }
    } catch (RecordNotFoundException e) {
      // do an INSERT (AKA ADD) operation
      AddRecordPacket addRecordPacket = new AddRecordPacket(updatePacket.getRequestID(), updatePacket.getName(),
              updatePacket.getRecordKey(), updatePacket.getUpdateValue(), updatePacket.getLocalNameServerId(),
              updatePacket.getTTL()); //  getTTL() is used only with upsert.
      addRecordPacket.setLNSRequestID(updatePacket.getLNSRequestID());
      replicaController.handleIncomingPacket(addRecordPacket.toJSONObject());

      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" NS processing UPSERT changed to ADD: " + addRecordPacket.getName());
      }
    } catch (FieldNotFoundException e) {
      GNS.getLogger().fine("Field not found exception. " + e.getMessage());
      e.printStackTrace();
    }
    return msgTask;
  }

}
