package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.clientsupport.NSAuthentication;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.util.NSResponseCode;
import org.json.JSONException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

/**
 *
 * Contains code for executing an address update locally at each active replica. If name servers are replicated,
 * then methods in this class will be executed after the coordination among active replicas at name servers
 * is complete.
 *
 * Created by abhigyan on 2/27/14.
 */
public class GnsReconUpdate {

  public static void executeUpdateLocal(UpdatePacket updatePacket, GnsReconfigurable replica,
          boolean noCoordinatonState, boolean recovery)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException, JSONException, IOException, FailedDBOperationException {
    if (Config.debuggingEnabled) {
      GNS.getLogger().fine(" Processing UPDATE: " + updatePacket);
    }

    if (noCoordinatonState) {
      ConfirmUpdatePacket failConfirmPacket = ConfirmUpdatePacket.createFailPacket(updatePacket, NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER);
      if (!recovery) {
        replica.getNioServer().sendToID(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());
      }
      return;
    }

    // First we do signature and ACL checks
    String guid = updatePacket.getName();
    String field = updatePacket.getRecordKey() != null ? updatePacket.getRecordKey().getName() : null;
    String writer = updatePacket.getAccessor();
    String signature = updatePacket.getSignature();
    String message = updatePacket.getMessage();
    NSResponseCode errorCode = NSResponseCode.NO_ERROR;
    // FIXME : handle ACL checks for full JSON user updates
    if (writer != null && field != null) { // writer will be null for internal system reads
      errorCode = NSAuthentication.signatureAndACLCheck(guid, field, writer, signature, message, MetaDataTypeName.WRITE_WHITELIST, replica);
    }
    // return an error packet if one of the checks doesn't pass
    if (errorCode.isAnError()) {
      ConfirmUpdatePacket failConfirmPacket = ConfirmUpdatePacket.createFailPacket(updatePacket, errorCode);
      if (!recovery) {
        replica.getNioServer().sendToID(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());

      }
      return;
    }

    NameRecord nameRecord;

    if (updatePacket.getOperation().isAbleToSkipRead()) { // some operations don't require a read first
      nameRecord = new NameRecord(replica.getDB(), guid);
    } else {
      try {
        if (field == null) {
          nameRecord = NameRecord.getNameRecord(replica.getDB(), guid);
        } else {
          nameRecord = NameRecord.getNameRecordMultiField(replica.getDB(), guid, null, ColumnFieldType.LIST_STRING, field);
        }
      } catch (RecordNotFoundException e) {
        GNS.getLogger().severe(" Error: name record not found before update. Return. Name = " + guid);
        e.printStackTrace();
        ConfirmUpdatePacket failConfirmPacket = ConfirmUpdatePacket.createFailPacket(updatePacket, errorCode);
        if (!recovery) {
          replica.getNioServer().sendToID(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());

        }
        return;
      }
    }

    // Apply update
    if (Config.debuggingEnabled) {
      if (field != null) {
      GNS.getLogger().fine("****** field=" + field + " operation= " + updatePacket.getOperation().toString() +
              " value= " + updatePacket.getUpdateValue() +
              " name Record=" + nameRecord.toString());
      }
    }
    boolean result;
    try {
      result = nameRecord.updateNameRecord(field,
              updatePacket.getUpdateValue(), updatePacket.getOldValue(), updatePacket.getArgument(),
              updatePacket.getUserJSON(),
              updatePacket.getOperation());
      if (Config.debuggingEnabled) {
        GNS.getLogger().fine("Update operation result = " + result + "\t"
                + updatePacket.getUpdateValue());
      }

      if (!result) { // update failed
        if (Config.debuggingEnabled) {
          GNS.getLogger().info("Update operation failed " + updatePacket);
        }
        if (updatePacket.getNameServerId() == replica.getNodeID()) { //if this node proposed this update
          // send error message to client
          ConfirmUpdatePacket failPacket = new ConfirmUpdatePacket(Packet.PacketType.CONFIRM_UPDATE,
                  updatePacket.getSourceId(),
                  updatePacket.getRequestID(), updatePacket.getLNSRequestID(), NSResponseCode.ERROR);
          if (Config.debuggingEnabled) {
            GNS.getLogger().info("Error msg sent to client for failed update " + updatePacket);
          }
          if (!recovery) {
            replica.getNioServer().sendToID(updatePacket.getLocalNameServerId(), failPacket.toJSONObject());
          }
        }

      } else {
        if (Config.debuggingEnabled) {
          GNS.getLogger().fine("Update applied" + updatePacket);
        }

        // Abhigyan: commented this because we are using lns votes for this calculation.
        // this should be uncommented once active replica starts to send read/write statistics for name.
//        nameRecord.incrementUpdateRequest();
        if (updatePacket.getNameServerId() == replica.getNodeID()) {
          ConfirmUpdatePacket confirmPacket = new ConfirmUpdatePacket(Packet.PacketType.CONFIRM_UPDATE,
                  updatePacket.getSourceId(),
                  updatePacket.getRequestID(), updatePacket.getLNSRequestID(), NSResponseCode.NO_ERROR);
          if (Config.debuggingEnabled) {
            GNS.getLogger().fine("NS Sent confirmation to LNS. Sent packet: " + confirmPacket.toJSONObject());
          }
          if (!recovery) {
            replica.getNioServer().sendToID(updatePacket.getLocalNameServerId(), confirmPacket.toJSONObject());
          }
        }
      }
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. Exception = " + e.getMessage());
      e.printStackTrace();
    }
  }

}
