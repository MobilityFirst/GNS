/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.account;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.AccessSupport;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.ClientUtils;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.utils.Base64;
import edu.umass.cs.gns.utils.JSONUtils;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Command to add a guid.
 *
 * @author westy
 */
public class AddMultipleGuids extends GnsCommand {

  /**
   * Creates an AddGuid instance.
   *
   * @param module
   */
  public AddMultipleGuids(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAMES, ACCOUNT_GUID, PUBLIC_KEYS, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return ADDGUID;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {

    String accountGuid = json.getString(ACCOUNT_GUID);
    //String names = json.getString(NAMES);
    JSONArray names = json.getJSONArray(NAMES);
    //String publicKeys = json.getString(PUBLIC_KEYS);
    JSONArray publicKeys = json.getJSONArray(PUBLIC_KEYS);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);

    GuidInfo accountGuidInfo;
    if ((accountGuidInfo = AccountAccess.lookupGuidInfo(accountGuid, handler)) == null) {
      return new CommandResponse<String>(BADRESPONSE + " " + BADGUID + " " + accountGuid);
    }
    if (AccessSupport.verifySignature(accountGuidInfo.getPublicKey(), signature, message)) {
      AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuid(accountGuid, handler);
      if (accountInfo == null) {
        return new CommandResponse<String>(BADRESPONSE + " " + BADACCOUNT + " " + accountGuid);
      }
      if (!accountInfo.isVerified()) {
        return new CommandResponse<String>(BADRESPONSE + " " + VERIFICATIONERROR + " Account not verified");
      } else if (accountInfo.getGuids().size() > GNS.MAXGUIDS) {
        return new CommandResponse<String>(BADRESPONSE + " " + TOMANYGUIDS);
      } else {

        CommandResponse<String> result
                = AccountAccess.addMultipleGuids(JSONUtils.JSONArrayToArrayListString(names),
                        JSONUtils.JSONArrayToArrayListString(publicKeys),
                        accountInfo, accountGuidInfo, handler);
//         CommandResponse<String> result
//                = AccountAccess.addMultipleGuids(JSONUtils.JSONArrayToArrayListString(new JSONArray(names)),
//                        JSONUtils.JSONArrayToArrayListString(new JSONArray(publicKeys)),
//                        accountInfo, accountGuidInfo, handler);
        //CommandResponse<String> result = AccountAccess.addGuid(accountInfo, accountGuidInfo, name, 
        //newGuid, publicKey, handler);
        return result;
      }
    } else {
      return new CommandResponse<String>(BADRESPONSE + " " + BADSIGNATURE);
    }
    //}
  }

  @Override
  public String getCommandDescription() {
    return "Adds guids to the account associated with the account guid. Must be signed by the account guid. "
            + "Returns " + BADGUID + " if the account guid has not been registered.";

  }
}
