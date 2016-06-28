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

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSResponseCode;
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
public class RemoveAccount extends BasicCommand {

  /**
   * Creates a RemoveAccount instance.
   *
   * @param module
   */
  public RemoveAccount(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.RemoveAccount;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, GUID, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

//  @Override
//  public String getCommandName() {
//    return REMOVE_ACCOUNT;
//  }
  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {
    String name = json.getString(NAME);
    String guid = json.getString(GUID);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    GuidInfo guidInfo;
    if ((guidInfo = AccountAccess.lookupGuidInfo(guid, handler, true)) == null) {
      return new CommandResponse(BAD_RESPONSE + " " + BAD_GUID + " " + guid,
              GNSResponseCode.BAD_GUID_ERROR);
    }
    try {
      if (NSAccessSupport.verifySignature(guidInfo.getPublicKey(), signature, message)) {
        AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromName(name, handler, true);
        if (accountInfo != null) {
          return AccountAccess.removeAccount(accountInfo, handler);
        } else {
          return new CommandResponse(BAD_RESPONSE + " " + BAD_ACCOUNT,
                  GNSResponseCode.BAD_ACCOUNT_ERROR);
        }
      } else {
        return new CommandResponse(BAD_RESPONSE + " " + BAD_SIGNATURE,
                GNSResponseCode.SIGNATURE_ERROR);
      }
    } catch (ClientException | IOException e) {
      return new CommandResponse(BAD_RESPONSE + " " + UNSPECIFIED_ERROR + " " + e.getMessage(),
              GNSResponseCode.UNSPECIFIED_ERROR);
    }
  }

  @Override
  public String getCommandDescription() {
    return "Removes the account GUID associated with the human readable name. Must be signed by the guid.";
  }
}
