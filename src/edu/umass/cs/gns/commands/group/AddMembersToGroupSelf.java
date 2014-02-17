/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.group;

import edu.umass.cs.gns.commands.CommandModule;
import static edu.umass.cs.gns.clientprotocol.Defs.*;

/**
 *
 * @author westy
 */
public class AddMembersToGroupSelf extends AddMembersToGroup {

  public AddMembersToGroupSelf(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, MEMBERS, SIGNATURE, "message"};
  }

  @Override
  public String getCommandName() {
    return ADDTOGROUP;
  }

  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS for the given guid after authenticating that GUID making request has access authority."
            + " Values are always returned as a JSON list."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields as a JSON object.";
  }
}
