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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccessSupport;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Command to add a guid.
 *
 * @author westy
 */
public class AddMultipleGuids extends GnsCommand {

  /**
   * Creates an AddGuid instance.
   *
   * @param module
   */
  public AddMultipleGuids(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAMES, ACCOUNT_GUID, PUBLIC_KEYS, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return ADD_GUID;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {

    String accountGuid = json.getString(ACCOUNT_GUID);
    //String names = json.getString(NAMES);
    JSONArray names = json.getJSONArray(NAMES);
    //String publicKeys = json.getString(PUBLIC_KEYS);
    JSONArray publicKeys = json.getJSONArray(PUBLIC_KEYS);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);

    GuidInfo accountGuidInfo;
    if ((accountGuidInfo = AccountAccess.lookupGuidInfo(accountGuid, handler)) == null) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_GUID + " " + accountGuid);
    }
    if (AccessSupport.verifySignature(accountGuidInfo.getPublicKey(), signature, message)) {
      AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuid(accountGuid, handler);
      if (accountInfo == null) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_ACCOUNT + " " + accountGuid);
      }
      if (!accountInfo.isVerified()) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + VERIFICATION_ERROR + " Account not verified");
      } else if (accountInfo.getGuids().size() > GNS.MAXGUIDS) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + TOO_MANY_GUIDS);
      } else {

        CommandResponse<String> result
                = AccountAccess.addMultipleGuids(JSONUtils.JSONArrayToArrayListString(names),
                        JSONUtils.JSONArrayToArrayListString(publicKeys),
                        accountInfo, accountGuidInfo, handler);
//         CommandResponse<String> result
//                = AccountAccess.addMultipleGuids(JSONUtils.JSONArrayToArrayListString(new JSONArray(names)),
//                        JSONUtils.JSONArrayToArrayListString(new JSONArray(publicKeys)),
//                        accountInfo, accountGuidInfo, handler);
        //CommandResponse<String> result = AccountAccess.addGuid(accountInfo, accountGuidInfo, name, 
        //newGuid, publicKey, handler);
        return result;
      }
    } else {
      return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_SIGNATURE);
    }
    //}
  }

  @Override
  public String getCommandDescription() {
    return "Adds guids to the account associated with the account guid. Must be signed by the account guid. "
            + "Returns " + BAD_GUID + " if the account guid has not been registered.";

  }
}
