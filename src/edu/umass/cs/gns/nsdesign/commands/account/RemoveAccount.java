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
public class RemoveAccount extends NSCommand {

  public RemoveAccount(NSCommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, GUID, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return REMOVEACCOUNT;
  }

  @Override
  public String execute(JSONObject json, GnsReconfigurable activeReplica) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String name = json.getString(NAME);
    String guid = json.getString(GUID);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    GuidInfo guidInfo;
    if ((guidInfo = NSAccountAccess.lookupGuidInfo(guid, activeReplica)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (AccessSupport.verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = NSAccountAccess.lookupAccountInfoFromName(name, activeReplica);
      if (accountInfo != null) {
        return NSAccountAccess.removeAccount(accountInfo, activeReplica);
      } else {
        return BADRESPONSE + " " + BADACCOUNT;
      }
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  @Override
  public String getCommandDescription() {
    return " Removes the account GUID associated with the human readable name. Must be signed by the guid.";
  }
}
