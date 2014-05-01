/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.commands.account;

import edu.umass.cs.gns.clientsupport.AccessSupport;
import edu.umass.cs.gns.clientsupport.AccountInfo;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.nsdesign.clientsupport.NSAccountAccess;
import edu.umass.cs.gns.nsdesign.commands.NSCommand;
import edu.umass.cs.gns.nsdesign.commands.NSCommandModule;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
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
public class SetPassword extends NSCommand {

  public SetPassword(NSCommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, NAME, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return SETPASSWORD;
  }

  @Override
  public String execute(JSONObject json, GnsReconfigurable activeReplica) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String password = json.getString(PASSWORD);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    GuidInfo guidInfo;
    if ((guidInfo = NSAccountAccess.lookupGuidInfo(guid, activeReplica)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (AccessSupport.verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = NSAccountAccess.lookupAccountInfoFromGuid(guid, activeReplica);
      return NSAccountAccess.setPassword(accountInfo, password, activeReplica);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Sets the password. Must be signed by the guid. Returns " + BADGUID + " if the GUID has not been registered.";




  }
}
