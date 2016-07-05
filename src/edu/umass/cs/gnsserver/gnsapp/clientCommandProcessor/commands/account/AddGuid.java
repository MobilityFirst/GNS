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

import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Command to add a guid.
 *
 * @author westy
 */
public class AddGuid extends BasicCommand {

  /**
   * Creates an AddGuid instance.
   *
   * @param module
   */
  public AddGuid(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.AddGuid;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, GUID, PUBLIC_KEY, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

//  @Override
//  public String getCommandName() {
//    return ADD_GUID;
//  }
  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {
    String name = json.getString(NAME);
    String accountGuid = json.getString(GUID);
    String publicKey = json.getString(PUBLIC_KEY);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);

    String newGuid = SharedGuidUtils.createGuidStringFromBase64PublicKey(publicKey);
//    byte[] publicKeyBytes = Base64.decode(publicKey);
//    String newGuid = SharedGuidUtils.createGuidStringFromPublicKey(publicKeyBytes);
    GuidInfo accountGuidInfo;
    if ((accountGuidInfo = AccountAccess.lookupGuidInfo(accountGuid, handler, true)) == null) {
      return new CommandResponse(GNSResponseCode.BAD_GUID_ERROR, BAD_RESPONSE + " " + BAD_GUID + " " + accountGuid);
    }
    if (NSAccessSupport.verifySignature(accountGuidInfo.getPublicKey(), signature, message)) {
      AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuid(accountGuid, handler, true);
      if (accountInfo == null) {
        return new CommandResponse(GNSResponseCode.BAD_ACCOUNT_ERROR, BAD_RESPONSE + " " + BAD_ACCOUNT + " " + accountGuid);
      }
      if (!accountInfo.isVerified()) {
        return new CommandResponse(GNSResponseCode.VERIFICATION_ERROR, BAD_RESPONSE + " " + VERIFICATION_ERROR + " Account not verified");
      } else if (accountInfo.getGuids().size() > GNSConfig.MAXGUIDS) {
        return new CommandResponse(GNSResponseCode.TOO_MANY_GUIDS_EXCEPTION, BAD_RESPONSE + " " + TOO_MANY_GUIDS);
      } else {
        CommandResponse result = AccountAccess.addGuid(accountInfo, accountGuidInfo, name, newGuid, publicKey, handler);
        if (result.getExceptionOrErrorCode().isOKResult()) {
          // Everything is hunkey dorey so return the new guid
          return new CommandResponse(GNSResponseCode.NO_ERROR, newGuid);
        } else {
          // Otherwise return the error response
          return result;
        }
      }
    } else {
      // Signature verification failed
      return new CommandResponse(GNSResponseCode.SIGNATURE_ERROR, BAD_RESPONSE + " " + BAD_SIGNATURE);
    }
    //}
  }

  @Override
  public String getCommandDescription() {
    return "Adds a GUID to the account associated with the GUID. Must be signed by the guid. "
            + "Returns " + BAD_GUID + " if the GUID has not been registered.";

  }
}
