
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;


public class Append extends AbstractUpdate {


  public Append(CommandModule module) {
    super(module);
  }
  

  @Override
  public CommandType getCommandType() {
    return CommandType.Append;
  }


  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_APPEND;
  }

}
