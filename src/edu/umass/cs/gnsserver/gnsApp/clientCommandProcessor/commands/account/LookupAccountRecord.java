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

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class LookupAccountRecord extends GnsCommand {

  /**
   * Creates a LookupAccountRecord instance.
   * 
   * @param module
   */
  public LookupAccountRecord(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID};
  }

  @Override
  public String getCommandName() {
    return LOOKUP_ACCOUNT_RECORD;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
//    if (CommandDefs.handleAcccountCommandsAtNameServer) {
//      return LNSToNSCommandRequestHandler.sendCommandRequest(json);
//    } else {
      String guid = json.getString(GUID);
      AccountInfo acccountInfo;
      if ((acccountInfo = AccountAccess.lookupAccountInfoFromGuid(guid, handler)) == null) {
        return new CommandResponse<>(BAD_RESPONSE + " " + BAD_ACCOUNT + " " + guid);
      }
      if (acccountInfo != null) {
        try {
          return new CommandResponse<>(acccountInfo.toJSONObject().toString());
        } catch (JSONException e) {
          return new CommandResponse<>(BAD_RESPONSE + " " + JSON_PARSE_ERROR);
        }
      } else {
        return new CommandResponse<>(BAD_RESPONSE + " " + BAD_GUID + " " + guid);
      }
   // }
  }

  @Override
  public String getCommandDescription() {
    return "Returns the account info associated with the given GUID. "
            + "Returns " + BAD_GUID + " if the GUID has not been registered.";

  }
}
