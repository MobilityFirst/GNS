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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin;

import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;

import java.util.logging.Level;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class Dump extends BasicCommand {

  /**
   *
   * @param module
   */
  public Dump(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.Dump;
  }

  

  @Override
  @SuppressWarnings("unchecked")
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    if (module.isAdminMode()) {
      if (json.has(PASSKEY)) {
        //If the user cannot be authenticated, return an ACCESS_ERROR and abort.
        String passkey = json.getString(PASSKEY);
        if (!Admin.authenticate(passkey)) {
          GNSConfig.getLogger().log(Level.INFO, "A client failed to authenticate for " + getCommandType().toString() + " : " + json.toString());
          return new CommandResponse(GNSResponseCode.ACCESS_ERROR, BAD_RESPONSE + " " + ACCESS_DENIED
                  + " Failed to authenticate " + getCommandType().toString() + " with key : " + passkey);
        }
        return handler.getAdmintercessor().sendDump(handler);
      } else {
        return new CommandResponse(GNSResponseCode.OPERATION_NOT_SUPPORTED, BAD_RESPONSE + " " + OPERATION_NOT_SUPPORTED
                + " " + getCommandType().toString() + " requires " + PASSKEY);
      }
    }
    return new CommandResponse(GNSResponseCode.OPERATION_NOT_SUPPORTED, BAD_RESPONSE + " " + OPERATION_NOT_SUPPORTED
            + " Don't understand " + getCommandType().toString());
  }

  
}
