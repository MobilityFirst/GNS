/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.admin;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class PingTable extends GnsCommand {

  /**
   *
   * @param module
   */
  public PingTable(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{N};
  }

  @Override
  public String getCommandName() {
    return PINGTABLE;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException, NumberFormatException {
    String nodeString = json.getString(N);
    if (module.isAdminMode()) {
      return new CommandResponse<String>(handler.getAdmintercessor().sendPingTable(nodeString, handler));
    }
    return new CommandResponse<String>(BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + getCommandName());
  }

  @Override
  public String getCommandDescription() {
    return "[ONLY IN ADMIN MODE] Returns a table of ping values for the given node.";
  }
}
