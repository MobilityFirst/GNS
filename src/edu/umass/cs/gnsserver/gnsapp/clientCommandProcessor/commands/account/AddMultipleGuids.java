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
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnscommon.CommandType;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_ACCOUNT;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_SIGNATURE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GUIDCNT;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.NAMES;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.PUBLIC_KEYS;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATURE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATUREFULLMESSAGE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.TOO_MANY_GUIDS;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.UNSPECIFIED_ERROR;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.VERIFICATION_ERROR;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.JSONUtils;

import edu.umass.cs.utils.Config;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Command to add a multiple guids using batch support.
 *
 * @author westy
 */
public class AddMultipleGuids extends AbstractCommand {

  /**
   * Creates an AddGuid instance.
   *
   * @param module
   */
  public AddMultipleGuids(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.AddMultipleGuids;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {

    String guid = json.getString(GUID);
    String guidCntString = json.optString(GUIDCNT);
    JSONArray names = json.optJSONArray(NAMES);
    JSONArray publicKeys = json.optJSONArray(PUBLIC_KEYS);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);

    GuidInfo accountGuidInfo;
    if ((accountGuidInfo = AccountAccess.lookupGuidInfoAnywhere(guid, handler)) == null) {
      return new CommandResponse(GNSResponseCode.BAD_GUID_ERROR, BAD_RESPONSE + " " + BAD_GUID + " " + guid);
    }
    if (NSAccessSupport.verifySignature(accountGuidInfo.getPublicKey(), signature, message)) {
      AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuidAnywhere(guid, handler);
      if (accountInfo == null) {
        return new CommandResponse(GNSResponseCode.BAD_ACCOUNT_ERROR, BAD_RESPONSE + " " + BAD_ACCOUNT + " " + guid);
      }
      if (!accountInfo.isVerified()) {
        return new CommandResponse(GNSResponseCode.VERIFICATION_ERROR, BAD_RESPONSE + " " + VERIFICATION_ERROR + " Account not verified");
      } else if (accountInfo.getGuids().size() > Config.getGlobalInt(GNSConfig.GNSC.ACCOUNT_GUID_MAX_SUBGUIDS)) {
        return new CommandResponse(GNSResponseCode.TOO_MANY_GUIDS_EXCEPTION, BAD_RESPONSE + " " + TOO_MANY_GUIDS);
      } else if (names != null && publicKeys != null) {
        GNSConfig.getLogger().info("ADD SLOW" + names + " / " + publicKeys);
        return AccountAccess.addMultipleGuids(JSONUtils.JSONArrayToArrayListString(names),
                JSONUtils.JSONArrayToArrayListString(publicKeys),
                accountInfo, accountGuidInfo, handler);
      } else if (names != null) {
        //GNS.getLogger().info("ADD FASTER" + names + " / " + publicKeys);
        return AccountAccess.addMultipleGuidsFaster(JSONUtils.JSONArrayToArrayListString(names),
                accountInfo, accountGuidInfo, handler);
      } else if (guidCntString != null) {
        //GNS.getLogger().info("ADD RANDOM" + names + " / " + publicKeys);
        int guidCnt = Integer.parseInt(guidCntString);
        return AccountAccess.addMultipleGuidsFasterAllRandom(guidCnt, accountInfo, accountGuidInfo, handler);
      } else {
        return new CommandResponse(GNSResponseCode.UNSPECIFIED_ERROR, BAD_RESPONSE + " " + UNSPECIFIED_ERROR
                + " bad arguments: need " + NAMES + " or " + NAMES + " and " + PUBLIC_KEYS + " or " + GUIDCNT);
      }
    } else {
      return new CommandResponse(GNSResponseCode.SIGNATURE_ERROR, BAD_RESPONSE + " " + BAD_SIGNATURE);
    }
    //}
  }

}
