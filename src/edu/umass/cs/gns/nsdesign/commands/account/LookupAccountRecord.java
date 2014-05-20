/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.commands.account;

import edu.umass.cs.gns.clientsupport.AccountInfo;
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
public class LookupAccountRecord extends NSCommand {

  public LookupAccountRecord(NSCommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID};
  }

  @Override
  public String getCommandName() {
    return LOOKUPACCOUNTRECORD;
  }

  @Override
  public String execute(JSONObject json, GnsReconfigurableInterface activeReplica) throws JSONException {
    String guid = json.getString(GUID);
    AccountInfo acccountInfo;
    if ((acccountInfo = NSAccountAccess.lookupAccountInfoFromGuid(guid, activeReplica)) == null) {
      return BADRESPONSE + " " + BADACCOUNT + " " + guid;
    }
    if (acccountInfo != null) {
      try {
        return acccountInfo.toJSONObject().toString();
      } catch (JSONException e) {
        return BADRESPONSE + " " + JSONPARSEERROR;
      }
    } else {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns the account info associated with the given GUID. "
            + "Returns " + BADGUID + " if the GUID has not been registered.";


  }
}
