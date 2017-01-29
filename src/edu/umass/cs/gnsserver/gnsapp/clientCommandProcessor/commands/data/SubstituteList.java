
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;


public class SubstituteList extends AbstractUpdateList {


  public SubstituteList(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.SubstituteList;
  }


  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_SUBSTITUTE;
  }
  
}
