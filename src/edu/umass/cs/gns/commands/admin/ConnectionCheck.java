/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Defs.*;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class ConnectionCheck extends GnsCommand {

  public ConnectionCheck(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{};
  }

  @Override
  public String getCommandName() {
    return CONNECTIONCHECK;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    return new CommandResponse(OKRESPONSE);
  }

  @Override
  public String getCommandDescription() {
    return "Checks connectivity.";
  }
}
