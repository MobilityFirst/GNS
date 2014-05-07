package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
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

  public static void handleUpsert(UpdatePacket updatePacket, ReplicaController replicaController) throws JSONException, IOException {
    if (Config.debugMode) {
      GNS.getLogger().fine("Handling upsert case ....... ");
    }
    ReplicaControllerRecord nameRecordPrimary;
    try {
      nameRecordPrimary = ReplicaControllerRecord.getNameRecordPrimaryMultiField(replicaController.getDB(), updatePacket.getName(),
              ReplicaControllerRecord.MARKED_FOR_REMOVAL, ReplicaControllerRecord.ACTIVE_NAMESERVERS);
      if (nameRecordPrimary.isMarkedForRemoval()) {
        ConfirmUpdatePacket failConfirmPacket = ConfirmUpdatePacket.createFailPacket(updatePacket, NSResponseCode.ERROR);
        replicaController.getNioServer().sendToID(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());
        if (Config.debugMode) {
          GNS.getLogger().fine(" UPSERT-FAILED because name record deleted already\t" + updatePacket.getName()
                  + "\t" + replicaController.getNodeID() + "\t" + updatePacket.getLocalNameServerId());
        }
      } else {
        int activeID = -1;
        Set<Integer> activeNS;
        activeNS = nameRecordPrimary.getActiveNameservers();
        if (activeNS != null) {
          activeID = replicaController.getGnsNodeConfig().getClosestServer(activeNS, null);
        }

        if (activeID != -1) {
          // forward update to active NS
          // updated to use a less kludgey operation - Westy
          if (updatePacket.getOperation().isUpsert()) {
            updatePacket.setOperation(updatePacket.getOperation().getNonUpsertEquivalent());
          }
          if (Config.debugMode) {
            GNS.getLogger().fine("UPSERT forwarded as UPDATE to active: " + activeID);
          }
          replicaController.getNioServer().sendToID(activeID, updatePacket.toJSONObject());
          // could not find activeNS for this name
        } else {
          // send error to LNS
          ConfirmUpdatePacket failConfirmPacket =
                  ConfirmUpdatePacket.createFailPacket(updatePacket, NSResponseCode.ERROR);
          replicaController.getNioServer().sendToID(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());
          if (Config.debugMode) {
            GNS.getLogger().fine(" UPSERT-FAILED\t" + updatePacket.getName() + "\t" + replicaController.getNodeID() +
                    "\t" + updatePacket.getLocalNameServerId());
          }
        }
      }
    } catch (RecordNotFoundException e) {
      // record does not exist, so we can do an ADD
      // do an INSERT (AKA ADD) operation
      AddRecordPacket addRecordPacket = new AddRecordPacket(updatePacket.getSourceId(),
              updatePacket.getRequestID(), updatePacket.getName(),
              updatePacket.getRecordKey(), updatePacket.getUpdateValue(), updatePacket.getLocalNameServerId(),
              updatePacket.getTTL()); //  getTTL() is used only with upsert.
      addRecordPacket.setLNSRequestID(updatePacket.getLNSRequestID());
      replicaController.getNioServer().sendToID(replicaController.getNodeID(), addRecordPacket.toJSONObject());

      if (Config.debugMode) {
        GNS.getLogger().fine(" NS processing UPSERT changed to ADD: " + addRecordPacket.getName());
      }
    } catch (FieldNotFoundException e) {
      GNS.getLogger().fine("Field not found exception. " + e.getMessage());
      e.printStackTrace();
    }
//    return msgTask;
  }

}
