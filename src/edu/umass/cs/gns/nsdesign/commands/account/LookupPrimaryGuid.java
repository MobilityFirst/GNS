/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.commands.account;

import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.nsdesign.clientsupport.NSAccountAccess;
import edu.umass.cs.gns.nsdesign.commands.NSCommand;
import edu.umass.cs.gns.nsdesign.commands.NSCommandModule;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import org.json.JSONException;
import org.json.JSONObject;

import static edu.umass.cs.gns.clientsupport.Defs.*;

/**
 *
 * @author westy
 */
@Deprecated
public class LookupPrimaryGuid extends NSCommand {

  public LookupPrimaryGuid(NSCommandModule module) {
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
  public String execute(JSONObject json, GnsReconfigurableInterface activeReplica) throws JSONException, FailedDBOperationException {
    String guid = json.getString(GUID);
    String result = NSAccountAccess.lookupPrimaryGuid(guid, activeReplica);
    if (result != null) {
      return result;
    } else {
      return BADRESPONSE + " " + BADGUID;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns the account guid associated the guid. "
            + "Returns " + BADGUID + " if the GUID does not exist.";
  }
}
