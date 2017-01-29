
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;


public class CreateEmpty extends Create {


  public CreateEmpty(CommandModule module) {
    super(module);
  }
  

  @Override
  public CommandType getCommandType() {
    return CommandType.CreateEmpty;
  }

}
