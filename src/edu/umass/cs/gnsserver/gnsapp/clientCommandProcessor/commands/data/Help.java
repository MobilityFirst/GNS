
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data;


import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandDescriptionFormat;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import org.json.JSONObject;


public class Help extends AbstractCommand {


  public Help(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.Help;
  }

  

//  @Override
//  public String getCommandName() {
//    return HELP;
//  }
  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) {
    JSONObject json = commandPacket.getCommand();
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
