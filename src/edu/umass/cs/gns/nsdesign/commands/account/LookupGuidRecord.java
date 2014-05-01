/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.commands.account;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.GuidInfo;
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
public class LookupGuidRecord extends NSCommand {

  public LookupGuidRecord(NSCommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID};
  }

  @Override
  public String getCommandName() {
    return LOOKUPGUIDRECORD;
  }

  @Override
  public String execute(JSONObject json, GnsReconfigurable activeReplica) throws JSONException {
    String guid = json.getString(GUID);
    GuidInfo guidInfo;
    if ((guidInfo = NSAccountAccess.lookupGuidInfo(guid, activeReplica)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (guidInfo != null) {
      try {
        return guidInfo.toJSONObject().toString();
      } catch (JSONException e) {
        return BADRESPONSE + " " + JSONPARSEERROR;
      }
    } else {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns human readable name and public key associated with the given GUID. "
            + "Returns " + BADGUID + " if the GUID has not been registered.";


  }
}
