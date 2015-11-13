/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp;

import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.InternalField;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.clientSupport.NSAuthentication;
import edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.gnsserver.gnsApp.packet.UpdatePacket;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.Util;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;
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
 */
public class AppUpdate {

  /**
   * Executes the local update in response to an UpdatePacket.
   *
   * @param updatePacket
   * @param app
   * @param doNotReplyToClient
   * @param activeCodeHandler
   * @throws NoSuchAlgorithmException
   * @throws InvalidKeySpecException
   * @throws InvalidKeyException
   * @throws SignatureException
   * @throws JSONException
   * @throws IOException
   * @throws FailedDBOperationException
   */
  public static void executeUpdateLocal(UpdatePacket<String> updatePacket,
          GnsApplicationInterface<String> app,
          boolean doNotReplyToClient, ActiveCodeHandler activeCodeHandler)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException, JSONException, IOException, FailedDBOperationException {
    Long receiptTime = System.currentTimeMillis();
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().info("Processing UPDATE with " + " "
              + "doNotReplyToClient= " + doNotReplyToClient
              //+ " packet: " + updatePacket.toString());
              + " packet: " + updatePacket.toReasonableString());
    }

    // First we do signature and ACL checks
    String guid = updatePacket.getName();
    String field = updatePacket.getKey() != null ? updatePacket.getKey() : null;
    String writer = updatePacket.getAccessor();
    String signature = updatePacket.getSignature();
    String message = updatePacket.getMessage();
    NSResponseCode errorCode = NSResponseCode.NO_ERROR;
    // FIXME : handle ACL checks for full JSON user updates
    if (writer != null && field != null) { // writer will be null for internal system reads
      errorCode = NSAuthentication.signatureAndACLCheck(guid, field, writer, signature, message, MetaDataTypeName.WRITE_WHITELIST,
              app, updatePacket.getCppAddress());
    }
    DelayProfiler.updateDelay("totalUpdateAuth", receiptTime);
    // return an error packet if one of the checks doesn't pass
    if (errorCode.isAnError()) {
      @SuppressWarnings("unchecked")
      ConfirmUpdatePacket<String> failConfirmPacket = ConfirmUpdatePacket.createFailPacket(updatePacket, errorCode);
      if (!doNotReplyToClient) {
        app.getClientCommandProcessor().injectPacketIntoCCPQueue(failConfirmPacket.toJSONObject());
        //replica.getMessenger().sendToAddress(updatePacket.getCppAddress(), failConfirmPacket.toJSONObject());

      }
      return;
    }

    NameRecord nameRecord;

    if (updatePacket.getOperation().isAbleToSkipRead()) { // some operations don't require a read first
      nameRecord = new NameRecord(app.getDB(), guid);
    } else {
      try {
        if (field == null) {
          nameRecord = NameRecord.getNameRecord(app.getDB(), guid);
        } else {
          nameRecord = NameRecord.getNameRecordMultiField(app.getDB(), guid, null, ColumnFieldType.LIST_STRING, field);
        }
      } catch (RecordNotFoundException e) {
        GNS.getLogger().severe(" Error: name record not found before update. Return. Name = " 
                //+ guid + " Packet = " + updatePacket.toString());
                + guid + " Packet = " + updatePacket.toReasonableString());
        e.printStackTrace();
        @SuppressWarnings("unchecked")
        ConfirmUpdatePacket<String> failConfirmPacket = ConfirmUpdatePacket.createFailPacket(updatePacket, NSResponseCode.ERROR);
        if (!doNotReplyToClient) {
          app.getClientCommandProcessor().injectPacketIntoCCPQueue(failConfirmPacket.toJSONObject());
          //app.getMessenger().sendToAddress(updatePacket.getCppAddress(), failConfirmPacket.toJSONObject());

        }
        return;
      }
    }

    // START ACTIVE CODE HANDLING
    ValuesMap newValue = null;
    // Only do this for user fields.
    if (field == null || !InternalField.isInternalField(field)) {
      NameRecord codeRecord = null;

      try {
        codeRecord = NameRecord.getNameRecordMultiField(app.getDB(), guid, null,
                ColumnFieldType.USER_JSON, ActiveCode.ON_WRITE);
//      codeRecord = NameRecord.getNameRecordMultiField(app.getDB(), guid, null,
//              ColumnFieldType.LIST_STRING, ActiveCode.ON_WRITE);
      } catch (RecordNotFoundException e) {
        //GNS.getLogger().severe("Active code read record not found: " + e.getMessage());
      }

      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("AC--->>> " + codeRecord.toString());
      }

      int hopLimit = 1;
      if (codeRecord != null && activeCodeHandler.hasCode(codeRecord, "write")) {
        try {
          String code64 = codeRecord.getValuesMap().getString(ActiveCode.ON_WRITE);
          ValuesMap packetValuesMap = updatePacket.getUserJSON();
//        ResultValue codeResult = codeRecord.getKeyAsArray(ActiveCode.ON_WRITE);
//        String code64 = codeResult.get(0).toString();
          //String code64 = NSFieldAccess.lookupListFieldOnThisServer(guid, ActiveCode.ON_WRITE, app).get(0).toString();
          if (AppReconfigurableNodeOptions.debuggingEnabled) {
            //GNS.getLogger().info("AC--->>> " + guid + " " + field + " " + packetValuesMap.toString());
            GNS.getLogger().info("AC--->>> " + guid + " " + field + " " + packetValuesMap.toReasonableString());
          }
          newValue = activeCodeHandler.runCode(code64, guid, field, "write", packetValuesMap, hopLimit);
        } catch (Exception e) {
          GNS.getLogger().info("Active code error: " + e.getMessage());
        }
      }
    }
    if (newValue == null) {
      newValue = updatePacket.getUserJSON();
    }

    // END ACTIVE CODE HANDLING
    // Apply update
    // FIXME: THIS CAUSES US TO HANG.
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      if (field != null) {
        GNS.getLogger().info("****** field= " + field + " operation= " + updatePacket.getOperation().toString()
                + " value= " + updatePacket.getUpdateValue().toString()
                //FIXME: THIS CAUSES US TO HANG!
                //+ " value= " + Util.ellipsize(updatePacket.getUpdateValue().toString(), 500)
                //+ " name Record=" + nameRecord.toString());
                + " name Record=" + nameRecord.toReasonableString());
      }
    }
    boolean result;
    try {
      result = nameRecord.updateNameRecord(field,
              updatePacket.getUpdateValue(), updatePacket.getOldValue(), updatePacket.getArgument(),
              newValue,
              //updatePacket.getUserJSON(),
              updatePacket.getOperation());
      // FIXME: THIS CAUSES US TO HANG!
//      if (AppReconfigurableNodeOptions.debuggingEnabled) {
//        GNS.getLogger().fine("Update operation result = " + result + "\t"
//                + updatePacket.getUpdateValue().toReasonableString());
//      }

      if (!result) { // update failed
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          //GNS.getLogger().info("Update operation failed " + updatePacket.toString());
          GNS.getLogger().info("Update operation failed " + updatePacket.toReasonableString());
        }
        if (updatePacket.getNameServerID().equals(app.getNodeID())) {
          // IF this node proposed this update send error message to client (CCP).
          ConfirmUpdatePacket<String> failPacket
                  = new ConfirmUpdatePacket<String>(Packet.PacketType.UPDATE_CONFIRM,
                          updatePacket.getSourceId(),
                          updatePacket.getRequestID(), updatePacket.getCCPRequestID(), NSResponseCode.ERROR);
          if (AppReconfigurableNodeOptions.debuggingEnabled) {
            //GNS.getLogger().info("Error msg sent to client for failed update " + updatePacket.toString());
            GNS.getLogger().info("Error msg sent to client for failed update " + updatePacket.toReasonableString());
          }
          if (!doNotReplyToClient) {
            app.getClientCommandProcessor().injectPacketIntoCCPQueue(failPacket.toJSONObject());
          }
        }

      } else {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          //GNS.getLogger().info("Update applied" + updatePacket.toString());
          GNS.getLogger().info("Update applied" + updatePacket.toReasonableString());
        }

        // If this node proposed this update send the confirmation back to the client (CCP).
        if (updatePacket.getNameServerID().equals(app.getNodeID())) {
          ConfirmUpdatePacket<String> confirmPacket = new ConfirmUpdatePacket<String>(Packet.PacketType.UPDATE_CONFIRM,
                  updatePacket.getSourceId(),
                  updatePacket.getRequestID(), updatePacket.getCCPRequestID(), NSResponseCode.NO_ERROR);

          if (!doNotReplyToClient) {
            app.getClientCommandProcessor().injectPacketIntoCCPQueue(confirmPacket.toJSONObject());
            //app.getMessenger().sendToAddress(updatePacket.getCppAddress(), confirmPacket.toJSONObject());
            if (AppReconfigurableNodeOptions.debuggingEnabled) {
              GNS.getLogger().info("NS Sent confirmation to CCP. Sent packet: " + confirmPacket.toJSONObject());
            }
          }
        }
      }
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. Exception = " + e.getMessage());
      e.printStackTrace();
    }
    DelayProfiler.updateDelay("totalUpdate", receiptTime);
  }
}
