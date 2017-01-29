
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;


public class AddMultipleGuidsFast extends AddMultipleGuids {


  public AddMultipleGuidsFast(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.AddMultipleGuidsFast;
  }

}
