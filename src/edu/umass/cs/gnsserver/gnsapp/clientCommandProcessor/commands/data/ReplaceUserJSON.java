
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;


public class ReplaceUserJSON extends AbstractUpdate {


  public ReplaceUserJSON(CommandModule module) {
    super(module);
  }
  

  @Override
  public CommandType getCommandType() {
    return CommandType.ReplaceUserJSON;
  }


  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.USER_JSON_REPLACE;
  }
  
}
