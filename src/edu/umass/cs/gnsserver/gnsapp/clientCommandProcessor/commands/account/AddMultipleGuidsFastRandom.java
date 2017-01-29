
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;


public class AddMultipleGuidsFastRandom extends AddMultipleGuids {


  public AddMultipleGuidsFastRandom(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.AddMultipleGuidsFastRandom;
  }

}
