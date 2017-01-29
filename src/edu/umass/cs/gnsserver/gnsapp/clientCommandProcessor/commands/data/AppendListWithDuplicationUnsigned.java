
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnscommon.CommandType;


public class AppendListWithDuplicationUnsigned extends AbstractUpdateList {


  public AppendListWithDuplicationUnsigned(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.AppendListWithDuplicationUnsigned;
  }


  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_APPEND_WITH_DUPLICATION;
  }
 
}
