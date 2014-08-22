/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.commands.account;

import static edu.umass.cs.gns.clientsupport.Defs.*;

import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.nsdesign.clientsupport.NSAccountAccess;
import edu.umass.cs.gns.nsdesign.commands.NSCommand;
import edu.umass.cs.gns.nsdesign.commands.NSCommandModule;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
@Deprecated
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
  public String execute(JSONObject json, GnsReconfigurableInterface activeReplica, InetSocketAddress lnsAddress) throws JSONException, FailedDBOperationException {
    String name = json.getString(NAME);
    String result = NSAccountAccess.lookupGuid(name, activeReplica);
    if (result != null) {
      return result;
    } else {
      return BADRESPONSE + " " + BADACCOUNT;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns the guid associated with the human readable name. "
            + "Returns " + BADACCOUNT + " if the GUID has not been registered.";
  }
}
