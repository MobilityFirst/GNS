/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import edu.umass.cs.gns.clientsupport.AccessSupport;
import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.CommandResponse;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
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
public class AddTag extends GnsCommand {

  public AddTag(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, NAME, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return ADDTAG;
  }

  @Override
  public CommandResponse execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String tag = json.getString(NAME);
    // signature and message can be empty for unsigned cases
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    GuidInfo guidInfo;
    if ((guidInfo = AccountAccess.lookupGuidInfo(guid)) == null) {
      return new CommandResponse(BADRESPONSE + " " + BADGUID + " " + guid);
    }
    return AccountAccess.addTag(guidInfo, tag, guid, signature, message);
//    GuidInfo guidInfo;
//    if ((guidInfo = AccountAccess.lookupGuidInfo(guid)) == null) {
//      return new CommandResponse(BADRESPONSE + " " + BADGUID + " " + guid);
//    }
//    if (AccessSupport.verifySignature(guidInfo, signature, message)) {
//      return AccountAccess.addTag(guidInfo, tag);
//    } else {
//      return new CommandResponse(BADRESPONSE + " " + BADSIGNATURE);
//    }
  }

  @Override
  public String getCommandDescription() {
    return "Adds a tag to the guid. Must be signed by the guid. "
            + "Returns " + BADGUID + " if the GUID has not been registered.";
  }
}
