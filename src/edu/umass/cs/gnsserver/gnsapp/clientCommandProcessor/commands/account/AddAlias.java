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

import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_ACCOUNT;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.NAME;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATURE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATUREFULLMESSAGE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.TIMESTAMP;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.TOO_MANY_ALIASES;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.VERIFICATION_ERROR;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Date;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class AddAlias extends BasicCommand {

  /**
   * Creates an AddAlias instance.
   *
   * @param module
   */
  public AddAlias(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.AddAlias;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    // The guid of the account we are adding the alias to
    String guid = json.getString(GUID);
    // The HRN (alias) we are adding to this account guid
    String name = json.getString(NAME);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    Date timestamp = json.has(TIMESTAMP) ? Format.parseDateISO8601UTC(json.getString(TIMESTAMP)) : null; // can be null on older client
    // Fixme: Does this really need remote access?
    AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuidAnywhere(guid, handler);
    if (accountInfo == null) {
      return new CommandResponse(GNSResponseCode.BAD_ACCOUNT_ERROR, BAD_RESPONSE + " " + BAD_ACCOUNT + " " + guid);
    }
    if (!accountInfo.isVerified()) {
      return new CommandResponse(GNSResponseCode.VERIFICATION_ERROR, BAD_RESPONSE + " " + VERIFICATION_ERROR + " Account not verified");
    } else if (accountInfo.getAliases().size() > Config.getGlobalInt(GNSConfig.GNSC.ACCOUNT_GUID_MAX_ALIASES)) {
      return new CommandResponse(GNSResponseCode.TOO_MANY_ALIASES_EXCEPTION, BAD_RESPONSE + " " + TOO_MANY_ALIASES);
    } else {
      return AccountAccess.addAlias(accountInfo, name, guid, signature, message, timestamp, handler);
    }
  }
}
