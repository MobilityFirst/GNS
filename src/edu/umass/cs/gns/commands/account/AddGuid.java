/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.account;

import edu.umass.cs.gns.client.AccountInfo;
import edu.umass.cs.gns.client.FieldMetaData;
import edu.umass.cs.gns.client.GuidInfo;
import edu.umass.cs.gns.clientprotocol.AccessSupport;
import edu.umass.cs.gns.clientprotocol.ClientUtils;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import static edu.umass.cs.gns.clientprotocol.Defs.*;
import edu.umass.cs.gns.httpserver.Defs;
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
    return new String[]{NAME, GUID, PUBLICKEY, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return ADDGUID;
  }

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String name = json.getString(NAME);
    String accountGuid = json.getString(GUID);
    String publicKey = json.getString(PUBLICKEY);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    String newGuid = ClientUtils.createGuidFromPublicKey(publicKey);
    GuidInfo accountGuidInfo;
    if ((accountGuidInfo = accountAccess.lookupGuidInfo(accountGuid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + accountGuid;
    }
    if (AccessSupport.verifySignature(accountGuidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(accountGuid);
      if (!accountInfo.isVerified()) {
        return BADRESPONSE + " " + VERIFICATIONERROR + "Account not verified";
      } else if (accountInfo.getGuids().size() > Defs.MAXGUIDS) {
        return BADRESPONSE + " " + TOMANYGUIDS;
      } else {
        String result = accountAccess.addGuid(accountInfo, name, newGuid, publicKey);
        if (OKRESPONSE.equals(result)) {
          // set up the default read access
          fieldMetaData.add(FieldMetaData.MetaDataTypeName.READ_WHITELIST, newGuid, ALLFIELDS, EVERYONE);
          // give account guid read and write access to all fields in the new guid
          fieldMetaData.add(FieldMetaData.MetaDataTypeName.READ_WHITELIST, newGuid, ALLFIELDS, accountGuid);
          fieldMetaData.add(FieldMetaData.MetaDataTypeName.WRITE_WHITELIST, newGuid, ALLFIELDS, accountGuid);
          return newGuid;
        } else {
          return result;
        }
      }
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Adds a GUID to the account associated with the GUID. Must be signed by the guid. "
            + "Returns " + BADGUID + " if the GUID has not been registered.";


  }
}
