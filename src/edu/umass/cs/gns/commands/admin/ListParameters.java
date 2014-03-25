/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.SystemParameter;
import edu.umass.cs.gns.commands.data.CommandModule;
import edu.umass.cs.gns.commands.data.GnsCommand;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class ListParameters extends GnsCommand {

  public ListParameters(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{};
  }

  @Override
  public String getCommandName() {
    return LISTPARAMETERS;
  }

  @Override
  public String execute(JSONObject json) {
    if (module.isAdminMode()) {
      return SystemParameter.listParameters();
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + LISTPARAMETERS;
  }

  @Override
  public String getCommandDescription() {
    return "[ONLY IN ADMIN MODE] Lists all parameter values.";
  }
}
