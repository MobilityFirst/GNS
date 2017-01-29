
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;


public class SelectGroupSetupQueryWithGuidAndInterval extends SelectGroupSetupQuery {


  public SelectGroupSetupQueryWithGuidAndInterval(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.SelectGroupSetupQueryWithGuidAndInterval;
  }
 
}
