/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.account;

import edu.umass.cs.gns.client.AccountInfo;
import edu.umass.cs.gns.client.GuidInfo;
import edu.umass.cs.gns.clientprotocol.AccessSupport;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import static edu.umass.cs.gns.clientprotocol.Defs.*;
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
public class SetPassword extends GnsCommand {

  public SetPassword(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, NAME, SIGNATURE, "message"};
  }

  @Override
  public String getCommandName() {
    return SETPASSWORD;
  }

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String password = json.getString(PASSWORD);
    String signature = json.getString(SIGNATURE);
    String message = json.getString("message");
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (AccessSupport.verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      return accountAccess.setPassword(accountInfo, password);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Sets the password. Must be signed by the guid. Returns " + BADGUID + " if the GUID has not been registered.";




  }
}
