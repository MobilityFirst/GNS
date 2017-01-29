
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;


public class AclAddSelf extends AclAdd {


  public AclAddSelf(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.AclAddSelf;
  }
  
}
