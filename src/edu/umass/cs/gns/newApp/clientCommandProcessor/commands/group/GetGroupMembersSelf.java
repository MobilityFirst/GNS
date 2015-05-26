/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group;

import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Defs.*;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.CommandModule;

/**
 *
 * @author westy
 */
public class GetGroupMembersSelf extends GetGroupMembers {

  public GetGroupMembersSelf(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return GETGROUPMEMBERS;
  }

  @Override
  public String getCommandDescription() {
    return "Returns the members of the group formatted as a JSON Array.";
  }
}
