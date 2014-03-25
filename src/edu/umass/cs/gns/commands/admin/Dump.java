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
public class Dump extends GnsCommand {

  public Dump(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{};
  }

  @Override
  public String getCommandName() {
    return DUMP;
  }

  @Override
  public String execute(JSONObject json) throws JSONException {
    if (module.isAdminMode()) {
      return Admintercessor.sendDump();
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + getCommandName();
  }

  @Override
  public String getCommandDescription() {
    return "[ONLY IN ADMIN MODE] Returns the contents of the GNS.";
  }
}
