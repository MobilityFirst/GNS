/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.group;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.GroupAccess;
import edu.umass.cs.gns.commands.data.CommandModule;
import edu.umass.cs.gns.commands.data.GnsCommand;
import edu.umass.cs.gns.nameserver.ResultValue;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class GrantMembership extends GnsCommand {

  public GrantMembership(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, MEMBER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return GRANTMEMBERSHIP;
  }

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String member = json.getString(MEMBER);
    // signature and message can be empty for unsigned cases
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    if (GroupAccess.grantMembership(guid, new ResultValue(new ArrayList(Arrays.asList(member))), guid, signature, message)) {
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + GENERICEERROR;
    }
//    GuidInfo guidInfo;
//    if ((guidInfo = AccountAccess.lookupGuidInfo(guid)) == null) {
//      return BADRESPONSE + " " + BADGUID + " " + guid;
//    }
//    if (!AccessSupport.verifySignature(guidInfo, signature, message)) {
//      return BADRESPONSE + " " + BADSIGNATURE;
//      // no need to verify ACL because only the GUID can access this
//    } else if (GroupAccess.grantMembership(guid, new ResultValue(new ArrayList(Arrays.asList(member))))) {
//      return OKRESPONSE;
//    } else {
//      return BADRESPONSE + " " + GENERICEERROR;
//    }
  }

  @Override
  public String getCommandDescription() {
    return "Approves membership of member in the group. Guid needs to sign the command.";
  }
}
