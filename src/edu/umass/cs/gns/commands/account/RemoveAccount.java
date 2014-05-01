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
import edu.umass.cs.gns.clientsupport.CommandRequestHandler;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.commands.CommandDefs;
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
public class RemoveAccount extends GnsCommand {

  public RemoveAccount(CommandModule module) {
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
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    if (CommandDefs.handleAcccountCommandsAtNameServer) {
      return CommandRequestHandler.sendCommandRequest(json);
    } else {
      String name = json.getString(NAME);
      String guid = json.getString(GUID);
      String signature = json.getString(SIGNATURE);
      String message = json.getString(SIGNATUREFULLMESSAGE);
      GuidInfo guidInfo;
      if ((guidInfo = AccountAccess.lookupGuidInfo(guid)) == null) {
        return BADRESPONSE + " " + BADGUID + " " + guid;
      }
      if (AccessSupport.verifySignature(guidInfo, signature, message)) {
        AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromName(name);
        if (accountInfo != null) {
          return AccountAccess.removeAccount(accountInfo);
        } else {
          return BADRESPONSE + " " + BADACCOUNT;
        }
      } else {
        return BADRESPONSE + " " + BADSIGNATURE;
      }
    }
  }

  @Override
  public String getCommandDescription() {
    return " Removes the account GUID associated with the human readable name. Must be signed by the guid.";
  }
}
