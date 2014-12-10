/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import edu.umass.cs.gns.clientsupport.CommandResponse;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.SystemParameter;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
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
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) {
    if (module.isAdminMode()) {
      return new CommandResponse(SystemParameter.listParameters());
    }
    return new CommandResponse(BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + LISTPARAMETERS);
  }

  @Override
  public String getCommandDescription() {
    return "[ONLY IN ADMIN MODE] Lists all parameter values.";
  }
}
