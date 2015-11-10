/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.SystemParameter;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class ListParameters extends GnsCommand {

  /**
   *
   * @param module
   */
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
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) {
    if (module.isAdminMode()) {
      return new CommandResponse<String>(SystemParameter.listParameters());
    }
    return new CommandResponse<String>(BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + LISTPARAMETERS);
  }

  @Override
  public String getCommandDescription() {
    return "[ONLY IN ADMIN MODE] Lists all parameter values.";
  }
}
