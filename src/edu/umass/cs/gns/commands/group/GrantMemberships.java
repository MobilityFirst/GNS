/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.group;

import edu.umass.cs.gns.client.GuidInfo;
import edu.umass.cs.gns.clientprotocol.AccessSupport;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import static edu.umass.cs.gns.clientprotocol.Defs.*;
import edu.umass.cs.gns.nameserver.ResultValue;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class GrantMemberships extends GnsCommand {

  public GrantMemberships(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, MEMBERS, SIGNATURE, "message"};
  }

  @Override
  public String getCommandName() {
    return GRANTMEMBERSHIP;
  }

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String members = json.getString(MEMBERS);
    // signature and message can be empty for unsigned cases
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString("message", null);
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (!AccessSupport.verifySignature(guidInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
      // no need to verify ACL because only the GUID can access this
    } else if (groupAccess.grantMembership(guid, new ResultValue(members))) {
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + GENERICEERROR;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS for the given guid after authenticating that GUID making request has access authority."
            + " Values are always returned as a JSON list."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields as a JSON object.";
  }
}
