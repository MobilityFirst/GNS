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
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class LookupGuid extends BasicCommand {

  /**
   * Creates a LookupGuid instance.
   *
   * @param module
   */
  public LookupGuid(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.LookupGuid;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME};
  }

//  @Override
//  public String getCommandName() {
//    return LOOKUP_GUID;
//  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    String name = json.getString(NAME);
    // look for an account guid
    String result = AccountAccess.lookupGuid(name, handler);
    if (result != null) {
      return new CommandResponse<String>(result);
    } else {
      return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_ACCOUNT);
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns the guid associated with for the human readable name. "
            + "Returns " + BAD_ACCOUNT + " if the GUID has not been registered.";
  }
}
