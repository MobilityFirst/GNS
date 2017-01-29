
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.secured;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Read;


public class ReadSecured extends Read {


  public ReadSecured(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.ReadSecured;
  }

}
