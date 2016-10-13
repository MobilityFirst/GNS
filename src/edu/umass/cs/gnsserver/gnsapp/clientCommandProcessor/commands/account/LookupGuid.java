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
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_ACCOUNT;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.NAME;
import edu.umass.cs.gnscommon.GNSResponseCode;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class LookupGuid extends AbstractCommand {

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
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    String name = json.getString(NAME);
    // look for an account guid
    String result = AccountAccess.lookupGuidLocally(name, handler);
    if (result != null) {
      return new CommandResponse(GNSResponseCode.NO_ERROR, result);
    } else {
      return new CommandResponse(GNSResponseCode.BAD_ACCOUNT_ERROR, BAD_RESPONSE + " " + BAD_ACCOUNT);
    }
  }

}
