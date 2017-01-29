
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;


public class SelectGroupSetupQueryWithGuid extends SelectGroupSetupQuery {


  public SelectGroupSetupQueryWithGuid(CommandModule module) {
    super(module);
  }
  

  @Override
  public CommandType getCommandType() {
    return CommandType.SelectGroupSetupQueryWithGuid;
  }

}
