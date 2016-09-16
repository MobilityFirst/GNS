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
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import edu.umass.cs.gnscommon.CommandType;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_ACCOUNT;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.JSON_PARSE_ERROR;
import edu.umass.cs.gnscommon.GNSResponseCode;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class LookupAccountRecord extends BasicCommand {

  /**
   * Creates a LookupAccountRecord instance.
   *
   * @param module
   */
  public LookupAccountRecord(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.LookupAccountRecord;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    String guid = json.getString(GUID);
    AccountInfo acccountInfo;
    if ((acccountInfo = AccountAccess.lookupAccountInfoFromGuidLocally(guid, handler)) == null) {
      return new CommandResponse(GNSResponseCode.BAD_ACCOUNT_ERROR, BAD_RESPONSE + " " + BAD_ACCOUNT + " " + guid);
    }
    if (acccountInfo != null) {
      try {
        // the true below omits the list of guids which might be too big to send back to the client
        return new CommandResponse(GNSResponseCode.NO_ERROR, acccountInfo.toJSONObject(true).toString());
        //return new CommandResponse(acccountInfo.toJSONObject().toString());
      } catch (JSONException e) {
        return new CommandResponse(GNSResponseCode.JSON_PARSE_ERROR, BAD_RESPONSE + " " + JSON_PARSE_ERROR);
      }
    } else {
      return new CommandResponse(GNSResponseCode.BAD_GUID_ERROR, BAD_RESPONSE + " " + BAD_GUID + " " + guid);
    }
  }

}
