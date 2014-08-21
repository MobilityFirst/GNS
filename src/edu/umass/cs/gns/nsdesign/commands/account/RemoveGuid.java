/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.commands.account;

import edu.umass.cs.gns.clientsupport.AccountInfo;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.nsdesign.clientsupport.NSAccessSupport;
import edu.umass.cs.gns.nsdesign.clientsupport.NSAccountAccess;
import edu.umass.cs.gns.nsdesign.commands.NSCommand;
import edu.umass.cs.gns.nsdesign.commands.NSCommandModule;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
@Deprecated
public class RemoveGuid extends NSCommand {

  public RemoveGuid(NSCommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, ACCOUNT_GUID, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return REMOVEGUID;
  }

  @Override
  public String execute(JSONObject json, GnsReconfigurableInterface activeReplica) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, FailedDBOperationException {
    String guidToRemove = json.getString(GUID);
    String accountGuid = json.optString(ACCOUNT_GUID, null);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    GuidInfo accountGuidInfo = null;
    GuidInfo guidInfoToRemove;
    if ((guidInfoToRemove = NSAccountAccess.lookupGuidInfo(guidToRemove, activeReplica)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guidToRemove;
    }
    if (accountGuid != null) {
      if ((accountGuidInfo = NSAccountAccess.lookupGuidInfo(accountGuid, activeReplica)) == null) {
        return BADRESPONSE + " " + BADGUID + " " + accountGuid;
      }
    }
    if (NSAccessSupport.verifySignature(accountGuidInfo != null ? accountGuidInfo : guidInfoToRemove, signature, message)) {
      AccountInfo accountInfo = null;
      if (accountGuid != null) {
        accountInfo = NSAccountAccess.lookupAccountInfoFromGuid(accountGuid, activeReplica);
        if (accountInfo == null) {
          return BADRESPONSE + " " + BADACCOUNT + " " + accountGuid;
        }
      }
      return NSAccountAccess.removeGuid(guidInfoToRemove, accountInfo, activeReplica);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Removes the GUID from the account associated with the ACCOUNT_GUID. "
            + "Must be signed by the account guid or the guid if account guid is not provided. "
            + "Returns " + BADGUID + " if the GUID has not been registered.";

  }
}
