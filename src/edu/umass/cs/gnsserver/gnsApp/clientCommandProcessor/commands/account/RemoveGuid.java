/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccessSupport;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import java.io.UnsupportedEncodingException;
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

  /**
   * Creates a RemoveGuid instance.
   * 
   * @param module
   */
  public RemoveGuid(CommandModule module) {
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
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {
//    if (CommandDefs.handleAcccountCommandsAtNameServer) {
//      return LNSToNSCommandRequestHandler.sendCommandRequest(json);
//    } else {
      String guidToRemove = json.getString(GUID);
      String accountGuid = json.optString(ACCOUNT_GUID, null);
      String signature = json.getString(SIGNATURE);
      String message = json.getString(SIGNATUREFULLMESSAGE);
      GuidInfo accountGuidInfo = null;
      GuidInfo guidInfoToRemove;
      if ((guidInfoToRemove = AccountAccess.lookupGuidInfo(guidToRemove, handler)) == null) {
        return new CommandResponse<String>(BADRESPONSE + " " + BADGUID + " " + guidToRemove);
      }
      if (accountGuid != null) {
        if ((accountGuidInfo = AccountAccess.lookupGuidInfo(accountGuid, handler)) == null) {
          return new CommandResponse<String>(BADRESPONSE + " " + BADGUID + " " + accountGuid);
        }
      }
      if (AccessSupport.verifySignature(accountGuidInfo != null ? accountGuidInfo.getPublicKey() 
              : guidInfoToRemove.getPublicKey(), signature, message)) {
        AccountInfo accountInfo = null;
        if (accountGuid != null) {
          accountInfo = AccountAccess.lookupAccountInfoFromGuid(accountGuid, handler);
          if (accountInfo == null) {
            return new CommandResponse<String>(BADRESPONSE + " " + BADACCOUNT + " " + accountGuid);
          }
        }
        return AccountAccess.removeGuid(guidInfoToRemove, accountInfo, handler);
      } else {
        return new CommandResponse<String>(BADRESPONSE + " " + BADSIGNATURE);
      }
    //}
  }

  @Override
  public String getCommandDescription() {
    return "Removes the GUID from the account associated with the ACCOUNT_GUID. "
            + "Must be signed by the account guid or the guid if account guid is not provided. "
            + "Returns " + BADGUID + " if the GUID has not been registered.";

  }
}
