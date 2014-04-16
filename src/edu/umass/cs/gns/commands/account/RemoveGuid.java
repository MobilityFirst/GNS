/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.account;

import edu.umass.cs.gns.clientsupport.AccessSupport;
import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.AccountInfo;
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
public class RemoveGuid extends GnsCommand {

  public RemoveGuid(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, GUID2, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return REMOVEGUID;
  }

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String accountGuid = json.getString(GUID);
    String guidToRemove = json.getString(GUID2);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    GuidInfo accountGuidInfo, guidInfoToRemove;
    if ((guidInfoToRemove = AccountAccess.lookupGuidInfo(guidToRemove)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guidToRemove;
    }
    if ((accountGuidInfo = AccountAccess.lookupGuidInfo(accountGuid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + accountGuid;
    }
    if (AccessSupport.verifySignature(accountGuidInfo, signature, message)) {
      AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuid(accountGuid);
      if (accountInfo == null)  {
        return BADRESPONSE + " " + BADACCOUNT + " " + accountGuid;
      }
      return AccountAccess.removeGuid(accountInfo, guidInfoToRemove);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Removes the second GUID from the account associated with the first GUID. "
            + "Must be signed by the guid. "
            + "Returns " + BADGUID + " if the GUID has not been registered.";

  }
}
