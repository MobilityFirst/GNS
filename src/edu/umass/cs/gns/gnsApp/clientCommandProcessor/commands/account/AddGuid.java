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
import edu.umass.cs.gns.httpserver.Defs;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.Base64;
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
public class AddGuid extends GnsCommand {

  public AddGuid(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, ACCOUNT_GUID, PUBLICKEY, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return ADDGUID;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {
      String name = json.getString(NAME);
      String accountGuid = json.getString(ACCOUNT_GUID);
      String publicKey = json.getString(PUBLICKEY);
      String signature = json.getString(SIGNATURE);
      String message = json.getString(SIGNATUREFULLMESSAGE);
      
      byte[] publicKeyBytes = Base64.decode(publicKey);
      String newGuid = ClientUtils.createGuidStringFromPublicKey(publicKeyBytes);
      
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
          CommandResponse<String> result = AccountAccess.addGuid(accountInfo, accountGuidInfo, name, newGuid, publicKey, handler);
          if (result.getReturnValue().equals(OKRESPONSE)) {
//            // set up the default read access
//           FieldMetaData.add(MetaDataTypeName.READ_WHITELIST, newGuid, ALLFIELDS, EVERYONE, handler);
//            // give account guid read and write access to all fields in the new guid
//            FieldMetaData.add(MetaDataTypeName.READ_WHITELIST, newGuid, ALLFIELDS, accountGuidInfo.getPublicKey(), handler);
//            //FieldMetaData.add(MetaDataTypeName.READ_WHITELIST_GUID, newGuid, ALLFIELDS, accountGuid, handler);
//            FieldMetaData.add(MetaDataTypeName.WRITE_WHITELIST, newGuid, ALLFIELDS, accountGuidInfo.getPublicKey(), handler);
//            //FieldMetaData.add(MetaDataTypeName.WRITE_WHITELIST_GUID, newGuid, ALLFIELDS, accountGuid, handler);            
            return new CommandResponse<String>(newGuid);
          } else {
            return result;
          }
        }
      } else {
        return new CommandResponse<String>(BADRESPONSE + " " + BADSIGNATURE);
      }
    //}
  }

  @Override
  public String getCommandDescription() {
    return "Adds a GUID to the account associated with the GUID. Must be signed by the guid. "
            + "Returns " + BADGUID + " if the GUID has not been registered.";

  }
}
