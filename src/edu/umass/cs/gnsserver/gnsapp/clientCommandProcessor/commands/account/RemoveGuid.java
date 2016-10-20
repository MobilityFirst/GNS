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


import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.ACCOUNT_GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_ACCOUNT;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_SIGNATURE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATURE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATUREFULLMESSAGE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.UNSPECIFIED_ERROR;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import org.json.JSONException;
import org.json.JSONObject;

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
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {
    String guidToRemove = json.getString(GUID);
    String accountGuid = json.optString(ACCOUNT_GUID, null);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    GuidInfo accountGuidInfo = null;
    GuidInfo guidInfoToRemove;
    if ((guidInfoToRemove = AccountAccess.lookupGuidInfoAnywhere(guidToRemove, handler)) == null) {
      return new CommandResponse(ResponseCode.BAD_GUID_ERROR, BAD_RESPONSE + " " + BAD_GUID + " " + guidToRemove);
    }
    if (accountGuid != null) {
      if ((accountGuidInfo = AccountAccess.lookupGuidInfoAnywhere(accountGuid, handler)) == null) {
        return new CommandResponse(ResponseCode.BAD_GUID_ERROR, BAD_RESPONSE + " " + BAD_GUID + " " + accountGuid);
      }
    }
    try {
      if (NSAccessSupport.verifySignature(accountGuidInfo != null ? accountGuidInfo.getPublicKey()
              : guidInfoToRemove.getPublicKey(), signature, message)) {
        AccountInfo accountInfo = null;
        if (accountGuid != null) {
          accountInfo = AccountAccess.lookupAccountInfoFromGuidAnywhere(accountGuid, handler);
          if (accountInfo == null) {
            return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR, BAD_RESPONSE + " " + BAD_ACCOUNT + " " + accountGuid);
          }
        }
        return AccountAccess.removeGuid(guidInfoToRemove, accountInfo, handler);
      } else {
        return new CommandResponse(ResponseCode.SIGNATURE_ERROR, BAD_RESPONSE + " " + BAD_SIGNATURE);
      }
    } catch (ClientException | IOException e) {
      return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR, BAD_RESPONSE + " " + UNSPECIFIED_ERROR + " " + e.getMessage());
    }
  }
}
