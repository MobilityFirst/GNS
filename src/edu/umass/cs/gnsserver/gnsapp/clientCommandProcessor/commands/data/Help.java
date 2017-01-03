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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data;


import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandDescriptionFormat;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class Help extends AbstractCommand {

  /**
   *
   * @param module
   */
  public Help(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */ 
  @Override
  public CommandType getCommandType() {
    return CommandType.Help;
  }

  

//  @Override
//  public String getCommandName() {
//    return HELP;
//  }
  @Override
  public CommandResponse execute(InternalRequestHeader header, JSONObject json, ClientRequestHandlerInterface handler) {
    if (json.has("tcp")) {
      return new CommandResponse(ResponseCode.NO_ERROR, "Commands are sent as TCP packets." + GNSProtocol.NEWLINE.toString() + GNSProtocol.NEWLINE.toString()
              + "Note: We use the terms field and key interchangably below." + GNSProtocol.NEWLINE.toString() + GNSProtocol.NEWLINE.toString()
              + "Commands:" + GNSProtocol.NEWLINE.toString()
              + module.allCommandDescriptions(CommandDescriptionFormat.TCP));
    } else if (json.has("tcpwiki")) {
      return new CommandResponse(ResponseCode.NO_ERROR, "Commands are sent as TCP packets." + GNSProtocol.NEWLINE.toString() + GNSProtocol.NEWLINE.toString()
              + "Note: We use the terms field and key interchangably below." + GNSProtocol.NEWLINE.toString() + GNSProtocol.NEWLINE.toString()
              + "Commands:" + GNSProtocol.NEWLINE.toString()
              + module.allCommandDescriptions(CommandDescriptionFormat.TCP_Wiki));
    } else {
      return new CommandResponse(ResponseCode.NO_ERROR, "Commands are sent as HTTP GET queries." + GNSProtocol.NEWLINE.toString() + GNSProtocol.NEWLINE.toString()
              + "Note: We use the terms field and key interchangably below." + GNSProtocol.NEWLINE.toString() + GNSProtocol.NEWLINE.toString()
              + "Commands:" + GNSProtocol.NEWLINE.toString()
              + module.allCommandDescriptions(CommandDescriptionFormat.HTML));
    }
  }

  
}
