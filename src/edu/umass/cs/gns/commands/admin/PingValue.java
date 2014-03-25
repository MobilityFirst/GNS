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
public class PingValue extends GnsCommand {

  public PingValue(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{N,N2};
  }

  @Override
  public String getCommandName() {
    return PINGVALUE;
  }

  @Override
  public String execute(JSONObject json) throws JSONException, NumberFormatException {
    String node1String = json.getString(N);
    String node2String = json.getString(N2);
    if (module.isAdminMode()) {
      return Admintercessor.sendPingValue(node1String, node2String);
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + getCommandName();
  }

  @Override
  public String getCommandDescription() {
    return "[ONLY IN ADMIN MODE] Returns the contents of the local name server cache.";
  }
}
