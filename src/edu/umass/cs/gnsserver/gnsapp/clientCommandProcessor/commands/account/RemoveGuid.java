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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;

/**
 *
 * @author westy
 */
public class RemoveGuid extends AbstractCommand {

  /**
   * Creates a RemoveGuid instance.
   *
   * @param module
   */
  public RemoveGuid(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.RemoveGuid;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException,
          InternalRequestException {
    JSONObject json = commandPacket.getCommand();
    String guidToRemove = json.getString(GNSProtocol.GUID.toString());
    String accountGuid = json.optString(GNSProtocol.ACCOUNT_GUID.toString(), null);
    String signature = json.getString(GNSProtocol.SIGNATURE.toString());
    String message = json.getString(GNSProtocol.SIGNATUREFULLMESSAGE.toString());
    GuidInfo accountGuidInfo = null;
    GuidInfo guidInfoToRemove;
    
    // We don't use cache in this lookup because cache on a deletion is not updated. A GUID
    // might be deleted in the database but still be present in the cache. 
    if ((guidInfoToRemove = AccountAccess.lookupGuidInfo(header, guidToRemove, handler, true, false)) == null) 
    {
      // Removing a non-existant guid is no longer an error.
      return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
    }
    if (accountGuid != null) {
      if ((accountGuidInfo = AccountAccess.lookupGuidInfoAnywhere(header, accountGuid, handler)) == null) {
        return new CommandResponse(ResponseCode.BAD_GUID_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_GUID.toString() + " " + accountGuid);
      }
    }
    if (NSAccessSupport.verifySignature(accountGuidInfo != null ? accountGuidInfo.getPublicKey()
            : guidInfoToRemove.getPublicKey(), signature, message)) {
      AccountInfo accountInfo = null;
      if (accountGuid != null) {
        accountInfo = AccountAccess.lookupAccountInfoFromGuidAnywhere(header, accountGuid, handler);
        if (accountInfo == null) {
          return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_ACCOUNT.toString() + " " + accountGuid);
        }
      }
      return AccountAccess.removeGuid(header, commandPacket, guidInfoToRemove, accountInfo, handler);
    } else {
      return new CommandResponse(ResponseCode.SIGNATURE_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_SIGNATURE.toString());
    }
  }
}
