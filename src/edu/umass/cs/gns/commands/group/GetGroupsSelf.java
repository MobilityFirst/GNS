/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.group;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.commands.CommandModule;

/**
 * Command to return the groups that a GUID is a member of formatted as a JSON Array.
 * 
 * @author westy
 */
public class GetGroupsSelf extends GetGroups {

  public GetGroupsSelf(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return GETGROUPS;
  }

  @Override
  public String getCommandDescription() {
    return "Returns the groups that a guid is a member of formatted as a JSON Array.";
  }
}
