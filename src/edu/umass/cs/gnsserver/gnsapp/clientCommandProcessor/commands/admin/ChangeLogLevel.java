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
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class ChangeLogLevel extends BasicCommand {

  /**
   *
   * @param module
   */
  public ChangeLogLevel(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.ChangeLogLevel;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{LOG_LEVEL};
  }

//  @Override
//  public String getCommandName() {
//    return CHANGE_LOG_LEVEL;
//  }
  @Override
  @SuppressWarnings("unchecked")
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String levelString = json.getString(LOG_LEVEL);
    if (module.isAdminMode()) {
      try {
        Level level = Level.parse(levelString);
        if (handler.getAdmintercessor().sendChangeLogLevel(level, handler)) {
          return new CommandResponse<>(OK_RESPONSE, GNSResponseCode.NO_ERROR);
        } else {
          return new CommandResponse<>(BAD_RESPONSE, GNSResponseCode.UNSPECIFIED_ERROR);
        }
      } catch (IllegalArgumentException e) {
        return new CommandResponse<>(BAD_RESPONSE + " " + UNSPECIFIED_ERROR + " Bad level " + levelString,
                GNSResponseCode.UNSPECIFIED_ERROR);
      }
    }
    return new CommandResponse<>(BAD_RESPONSE + " " + OPERATION_NOT_SUPPORTED
            + " Don't understand " + getCommandType().toString(),
            GNSResponseCode.OPERATION_NOT_SUPPORTED);
  }

  @Override
  public String getCommandDescription() {
    return "Changes the log level.";
  }
}
