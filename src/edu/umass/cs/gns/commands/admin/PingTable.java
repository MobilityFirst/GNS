/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import edu.umass.cs.gns.clientsupport.Admintercessor;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.commands.data.CommandModule;
import edu.umass.cs.gns.commands.data.GnsCommand;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class PingTable extends GnsCommand {

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
  public String execute(JSONObject json) throws JSONException, NumberFormatException {
    String nodeString = json.getString(N);
    if (module.isAdminMode()) {
      return Admintercessor.sendPingTable(nodeString);
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + getCommandName();
  }

  @Override
  public String getCommandDescription() {
    return "[ONLY IN ADMIN MODE] Returns a table of ping values for the given node.";
  }
}
