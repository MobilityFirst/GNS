package edu.umass.cs.gns.nsdesign.activeReplica;

import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.recordmap.NameRecord;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.NSResponseCode;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.UpdateAddressPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

/*** DONT not use any class in package edu.umass.cs.gns.nsdesign ***/
/**
 *
 * Contains code for executing an address update locally at each active replica. If name servers are replicated,
 * then methods in this class will be executed after  the coordination among active replicas at name servers
 * is complete.
 *
 * Created by abhigyan on 2/27/14.
 */
public class Update {

  public static GNSMessagingTask handleUpdate(JSONObject json, ActiveReplica replica)
          throws JSONException, InvalidKeySpecException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {

    UpdateAddressPacket updateAddressPacket = new UpdateAddressPacket(json);
    updateAddressPacket.setNameServerId(replica.getNodeID());
    if (replica.getActiveCoordinator() == null) {
      return Update.executeUpdateLocal(updateAddressPacket, replica);
    } else {
      replica.getActiveCoordinator().handleRequest(updateAddressPacket.toJSONObject());
      return null;
    }

  }


  public static GNSMessagingTask executeUpdateLocal(UpdateAddressPacket updatePacket, ActiveReplica replica)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException, JSONException {
    GNS.getLogger().info(" Processing UPDATE: " + updatePacket);

//    UpdateAddressPacket updatePacket = new UpdateAddressPacket(incomingJSON);

    // First we do signature and ACL checks
    String guid = updatePacket.getName();
    String field = updatePacket.getRecordKey().getName();
    String writer = updatePacket.getAccessor();
    String signature = updatePacket.getSignature();
    String message = updatePacket.getMessage();
    NSResponseCode errorCode = NSResponseCode.NO_ERROR;
    if (writer != null) { // writer will be null for internal system reads
      errorCode = Lookup.signatureAndACLCheck(guid, field, writer, signature, message, MetaDataTypeName.WRITE_WHITELIST, replica);
    }
    // return an error packet if one of the checks doesn't pass
    if (errorCode.isAnError()) {
      ConfirmUpdateLNSPacket failConfirmPacket = ConfirmUpdateLNSPacket.createFailPacket(updatePacket, errorCode);
//      NameServer.returnToSender(failConfirmPacket.toJSONObject(), updatePacket.getLocalNameServerId());
      return new GNSMessagingTask(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());
    }


    NameRecord nameRecord;

    if (updatePacket.getOperation().equals(UpdateOperation.REPLACE_ALL)) { // we don't need to read for replace-all
      nameRecord = new NameRecord(replica.getDB(), updatePacket.getName());
    } else {
      try {
        nameRecord = NameRecord.getNameRecordMultiField(replica.getDB(), updatePacket.getName(), null, updatePacket.getRecordKey().getName());
      } catch (RecordNotFoundException e) {
        GNS.getLogger().severe(" Error: name record not found before update. Return. Name = " + updatePacket.getName());
        e.printStackTrace();
        ConfirmUpdateLNSPacket failConfirmPacket = ConfirmUpdateLNSPacket.createFailPacket(updatePacket, errorCode);
        return new GNSMessagingTask(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());
      }
    }

    // Apply update
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("NAME RECORD is: " + nameRecord.toString());
    }
    boolean result;
    try {
      result = nameRecord.updateKey(updatePacket.getRecordKey().getName(), updatePacket.getUpdateValue(),
              updatePacket.getOldValue(), updatePacket.getOperation());

      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Update operation result = " + result + "\t"
                + updatePacket.getUpdateValue());
      }

      if (!result) { // update failed
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Update operation failed " + updatePacket);
        }
        if (updatePacket.getNameServerId() == replica.getNodeID()) { //if this node proposed this update
          // send error message to client
          ConfirmUpdateLNSPacket failPacket = new ConfirmUpdateLNSPacket(Packet.PacketType.CONFIRM_UPDATE_LNS,
                  updatePacket.getRequestID(), updatePacket.getLNSRequestID(), NSResponseCode.ERROR);

          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("Error msg sent to client for failed update " + updatePacket);
          }
          return new GNSMessagingTask(updatePacket.getLocalNameServerId(), failPacket.toJSONObject());
        }
        return null;
      }

      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Update applied" + updatePacket);
      }

      // Abhigyan: commented this because we are using lns votes for this calculation.
      // this should be uncommented once active replica starts to send read/write statistics for name.
//        nameRecord.incrementUpdateRequest();
      if (updatePacket.getNameServerId() == replica.getNodeID()) {
        ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(Packet.PacketType.CONFIRM_UPDATE_LNS,
                updatePacket.getRequestID(), updatePacket.getLNSRequestID(), NSResponseCode.NO_ERROR);
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("NS Sent confirmation to LNS. Sent packet: " + confirmPacket.toJSONObject());
        }

        return new GNSMessagingTask(updatePacket.getLocalNameServerId(), confirmPacket.toJSONObject());

      }
      return null;
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. Exception = " + e.getMessage());
      e.printStackTrace();
      return  null;
    }
  }


}
