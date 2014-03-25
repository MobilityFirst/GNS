/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.account;

import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.AccountInfo;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class LookupAccountRecord extends GnsCommand {

  public LookupAccountRecord(CommandModule module) {
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
  public String execute(JSONObject json) throws JSONException {
    String guid = json.getString(GUID);
    AccountInfo acccountInfo;
    if ((acccountInfo = AccountAccess.lookupAccountInfoFromGuid(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
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
