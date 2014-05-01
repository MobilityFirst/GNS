/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.commands.account;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.nsdesign.clientsupport.NSAccountAccess;
import edu.umass.cs.gns.nsdesign.commands.NSCommand;
import edu.umass.cs.gns.nsdesign.commands.NSCommandModule;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class LookupGuid extends NSCommand {

  public LookupGuid(NSCommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME};
  }

  @Override
  public String getCommandName() {
    return LOOKUPGUID;
  }

  @Override
  public String execute(JSONObject json, GnsReconfigurable activeReplica) throws JSONException {
    String name = json.getString(NAME);
    // look for an account guid
    String result = NSAccountAccess.lookupGuid(name, activeReplica);
    if (result != null) {
      return result;
    } else {
      return BADRESPONSE + " " + BADACCOUNT;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns the guid associated with for the human readable name. "
            + "Returns " + BADACCOUNT + " if the GUID has not been registered.";
  }
}
