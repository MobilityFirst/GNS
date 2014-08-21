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
import edu.umass.cs.gns.clientsupport.CommandResponse;
import edu.umass.cs.gns.clientsupport.LNSToNSCommandRequestHandler;
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
  public CommandResponse execute(JSONObject json) throws JSONException {
//    if (CommandDefs.handleAcccountCommandsAtNameServer) {
//      return LNSToNSCommandRequestHandler.sendCommandRequest(json);
//    } else {
      String guid = json.getString(GUID);
      AccountInfo acccountInfo;
      if ((acccountInfo = AccountAccess.lookupAccountInfoFromGuid(guid)) == null) {
        return new CommandResponse(BADRESPONSE + " " + BADACCOUNT + " " + guid);
      }
      if (acccountInfo != null) {
        try {
          return new CommandResponse(acccountInfo.toJSONObject().toString());
        } catch (JSONException e) {
          return new CommandResponse(BADRESPONSE + " " + JSONPARSEERROR);
        }
      } else {
        return new CommandResponse(BADRESPONSE + " " + BADGUID + " " + guid);
      }
   // }
  }

  @Override
  public String getCommandDescription() {
    return "Returns the account info associated with the given GUID. "
            + "Returns " + BADGUID + " if the GUID has not been registered.";

  }
}
