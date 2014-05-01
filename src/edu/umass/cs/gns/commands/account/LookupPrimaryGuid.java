/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.account;

import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.CommandRequestHandler;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.commands.CommandDefs;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class LookupPrimaryGuid extends GnsCommand {

  public LookupPrimaryGuid(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID};
  }

  @Override
  public String getCommandName() {
    return LOOKUPPRIMARYGUID;
  }

  @Override
  public String execute(JSONObject json) throws JSONException {
    if (CommandDefs.handleAcccountCommandsAtNameServer) {
      return CommandRequestHandler.sendCommandRequest(json);
    } else {
      String guid = json.getString(GUID);
      String result = AccountAccess.lookupPrimaryGuid(guid);
      if (result != null) {
        return result;
      } else {
        return BADRESPONSE + " " + BADGUID;
      }
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns the account guid associated the guid. "
            + "Returns " + BADGUID + " if the GUID does not exist.";
  }
}
