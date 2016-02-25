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
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.InternalField;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.clientSupport.NSAuthentication;
import edu.umass.cs.gnsserver.gnsApp.clientSupport.NSUpdateSupport;
import edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.gnsserver.gnsApp.packet.UpdatePacket;
import edu.umass.cs.gnsserver.gnsApp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.ResultValue;
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
@Deprecated
public class AppUpdate {
    
  /**
   * Executes the local update in response to an UpdatePacket.
   *
   * @param updatePacket
   * @param app
   * @param doNotReplyToClient
   * @throws NoSuchAlgorithmException
   * @throws InvalidKeySpecException
   * @throws InvalidKeyException
   * @throws SignatureException
   * @throws JSONException
   * @throws IOException
   * @throws FailedDBOperationException
   */
  @Deprecated
  public static void executeUpdateLocalUpdatePacket(UpdatePacket<String> updatePacket,
          GnsApplicationInterface<String> app,
          boolean doNotReplyToClient)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException,
          SignatureException, JSONException, IOException, FailedDBOperationException {
    Long receiptTime = System.currentTimeMillis();
    try {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("Processing UPDATE packet with " + " "
                + "doNotReplyToClient= " + doNotReplyToClient
                + " packet: " + updatePacket.toString(true));
      }

      // First we do signature and ACL checks
      String guid = updatePacket.getName();
      String field = updatePacket.getKey() != null ? updatePacket.getKey() : null;
      String writer = updatePacket.getAccessor();
      String signature = updatePacket.getSignature();
      String message = updatePacket.getMessage();
      NSResponseCode result = NSUpdateSupport.executeUpdateLocal(guid, field, writer, signature, message,
              updatePacket.getOperation(),
              updatePacket.getUpdateValue(), updatePacket.getOldValue(),
              updatePacket.getArgument(),
              updatePacket.getUserJSON(),
              app, doNotReplyToClient);
      switch (result) {
        case ERROR:
          if (AppReconfigurableNodeOptions.debuggingEnabled) {
            GNS.getLogger().info("Update operation failed " + updatePacket.toString(true));
          }
          if (updatePacket.getNameServerID().equals(app.getNodeID())) {
            // If this node proposed this update send error message to client (CCP).
            ConfirmUpdatePacket<String> failPacket
                    = new ConfirmUpdatePacket<String>(Packet.PacketType.UPDATE_CONFIRM,
                            updatePacket.getSourceId(),
                            updatePacket.getRequestIDInteger(), updatePacket.getCCPRequestID(), NSResponseCode.ERROR);
            if (AppReconfigurableNodeOptions.debuggingEnabled) {
              GNS.getLogger().info("Error msg sent to client for failed update " + updatePacket.toString(true));
            }
            if (!doNotReplyToClient) {
              app.getClientCommandProcessor().injectPacketIntoCCPQueue(failPacket.toJSONObject());
            }
          }
          return;
        case NO_ERROR:
          if (AppReconfigurableNodeOptions.debuggingEnabled) {
            GNS.getLogger().info("Update applied" + updatePacket.toString(true));
          }

          // If this node proposed this update send the confirmation back to the client (CCP).
          if (updatePacket.getNameServerID().equals(app.getNodeID())) {
            ConfirmUpdatePacket<String> confirmPacket = new ConfirmUpdatePacket<String>(Packet.PacketType.UPDATE_CONFIRM,
                    updatePacket.getSourceId(),
                    updatePacket.getRequestIDInteger(), updatePacket.getCCPRequestID(), NSResponseCode.NO_ERROR);

            if (!doNotReplyToClient) {
              app.getClientCommandProcessor().injectPacketIntoCCPQueue(confirmPacket.toJSONObject());
              //app.getMessenger().sendToAddress(updatePacket.getCppAddress(), confirmPacket.toJSONObject());
              if (AppReconfigurableNodeOptions.debuggingEnabled) {
                GNS.getLogger().info("NS Sent confirmation to CCP. Sent packet: " + confirmPacket.toJSONObject());
              }
            }
          }
          return;
        // authentication errors
        default:
          @SuppressWarnings("unchecked") ConfirmUpdatePacket<String> failConfirmPacket = ConfirmUpdatePacket.createFailPacket(updatePacket, result);
          if (!doNotReplyToClient) {
            app.getClientCommandProcessor().injectPacketIntoCCPQueue(failConfirmPacket.toJSONObject());
          }
          return;
      }
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe(" Error: name record not found before update. Return. Name = "
              + updatePacket.getName() + " Packet = " + updatePacket.toString(true));
      e.printStackTrace();
      @SuppressWarnings("unchecked")
      ConfirmUpdatePacket<String> failConfirmPacket = ConfirmUpdatePacket.createFailPacket(updatePacket,
              NSResponseCode.ERROR);
      if (!doNotReplyToClient) {
        app.getClientCommandProcessor().injectPacketIntoCCPQueue(failConfirmPacket.toJSONObject());
      }
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. Exception = " + e.getMessage());
      e.printStackTrace();
    }
    DelayProfiler.updateDelay("totalUpdate", receiptTime);
  }
}
