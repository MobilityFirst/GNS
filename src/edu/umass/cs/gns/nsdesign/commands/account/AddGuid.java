/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.commands.account;

import edu.umass.cs.gns.clientsupport.AccountInfo;
import edu.umass.cs.gns.clientsupport.ClientUtils;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.localnameserver.httpserver.Defs;
import edu.umass.cs.gns.nsdesign.clientsupport.NSAccessSupport;
import edu.umass.cs.gns.nsdesign.clientsupport.NSAccountAccess;
import edu.umass.cs.gns.nsdesign.clientsupport.NSFieldMetaData;
import edu.umass.cs.gns.nsdesign.commands.NSCommand;
import edu.umass.cs.gns.nsdesign.commands.NSCommandModule;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import org.json.JSONException;
import org.json.JSONObject;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.util.Base64;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;

/**
 *
 * @author westy
 */
@Deprecated
public class AddGuid extends NSCommand {

  public AddGuid(NSCommandModule module) {
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
  public String execute(JSONObject json, GnsReconfigurableInterface activeReplica, InetSocketAddress lnsAddress) 
          throws InvalidKeyException, InvalidKeySpecException, JSONException, NoSuchAlgorithmException, 
          SignatureException, FailedDBOperationException, UnsupportedEncodingException {
    String name = json.getString(NAME);
    String accountGuid = json.getString(ACCOUNT_GUID);
    String publicKey = json.getString(PUBLICKEY);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    
    byte[] publicKeyBytes = Base64.decode(publicKey);
    String newGuid = ClientUtils.createGuidFromPublicKey(publicKeyBytes);
    
    GuidInfo accountGuidInfo;
    if ((accountGuidInfo = NSAccountAccess.lookupGuidInfo(accountGuid, activeReplica, lnsAddress)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + accountGuid;
    }
    if (NSAccessSupport.verifySignature(accountGuidInfo, signature, message)) {
      AccountInfo accountInfo = NSAccountAccess.lookupAccountInfoFromGuid(accountGuid, activeReplica, lnsAddress);
      if (accountInfo == null) {
        return BADRESPONSE + " " + BADACCOUNT + " " + accountGuid;
      }
      if (!accountInfo.isVerified()) {
        return BADRESPONSE + " " + VERIFICATIONERROR + " Account not verified";
      } else if (accountInfo.getGuids().size() > Defs.MAXGUIDS) {
        return BADRESPONSE + " " + TOMANYGUIDS;
      } else {
        String result = NSAccountAccess.addGuid(accountInfo, name, newGuid, publicKey, activeReplica, lnsAddress);
        if (OKRESPONSE.equals(result)) {
          // set up the default read access
          NSFieldMetaData.add(MetaDataTypeName.READ_WHITELIST, newGuid, ALLFIELDS, EVERYONE, activeReplica, lnsAddress);
          // give account guid read and write access to all fields in the new guid
          NSFieldMetaData.add(MetaDataTypeName.READ_WHITELIST, newGuid, ALLFIELDS, accountGuid, activeReplica, lnsAddress);
          NSFieldMetaData.add(MetaDataTypeName.WRITE_WHITELIST, newGuid, ALLFIELDS, accountGuid, activeReplica, lnsAddress);
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
