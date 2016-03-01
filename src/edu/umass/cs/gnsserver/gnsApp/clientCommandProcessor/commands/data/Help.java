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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.ClientRequestHandlerInterface;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class Help extends GnsCommand {

  /**
   *
   * @param module
   */
  public Help(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{};
  }

  @Override
  public String getCommandName() {
    return HELP;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) {
    if (json.has("tcp")) {
      return new CommandResponse<String>("Commands are sent as TCP packets." + NEWLINE + NEWLINE
              + "Note: We use the terms field and key interchangably below." + NEWLINE + NEWLINE
              + "Commands:" + NEWLINE
              + module.allCommandDescriptions(CommandModule.CommandDescriptionFormat.TCP));
    } else if (json.has("tcpwiki")) {
      return new CommandResponse<String>("Commands are sent as TCP packets." + NEWLINE + NEWLINE
              + "Note: We use the terms field and key interchangably below." + NEWLINE + NEWLINE
              + "Commands:" + NEWLINE
              + module.allCommandDescriptions(CommandModule.CommandDescriptionFormat.TCP_Wiki));
    } else {
      return new CommandResponse<String>("Commands are sent as HTTP GET queries." + NEWLINE + NEWLINE
              + "Note: We use the terms field and key interchangably below." + NEWLINE + NEWLINE
              + "Commands:" + NEWLINE
              + module.allCommandDescriptions(CommandModule.CommandDescriptionFormat.HTML));
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns this help message";
  }
}
