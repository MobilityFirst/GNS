/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.acl;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.FieldMetaData;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Defs.*;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.util.NSResponseCode;
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
public class AclAdd extends GnsCommand {

  public AclAdd(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, ACCESSER, WRITER, ACLTYPE, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return ACLADD;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    // The guid that wants to access this field
    String accesser = json.getString(ACCESSER);
    // allows someone other than guid to change the acl, defaults to guid
    String writer = json.optString(WRITER, guid);
    String accessType = json.getString(ACLTYPE);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    MetaDataTypeName access;
    if ((access = MetaDataTypeName.valueOf(accessType)) == null) {
      return new CommandResponse(BADRESPONSE + " " + BADACLTYPE + "Should be one of " + MetaDataTypeName.values().toString());
    }
    String accessorPublicKey;
    if (EVERYONE.equals(accesser)) {
      accessorPublicKey = EVERYONE;
    } else {
      GuidInfo accessorGuidInfo;
      if ((accessorGuidInfo = AccountAccess.lookupGuidInfo(accesser, handler)) == null) {
        return new CommandResponse(BADRESPONSE + " " + BADGUID + " " + accesser);
      } else {
        accessorPublicKey = accessorGuidInfo.getPublicKey();
      }
    }
    NSResponseCode responseCode;
    if (!(responseCode = FieldMetaData.add(access, guid, field, accessorPublicKey, writer, signature, message, handler)).isAnError()) {
      return new CommandResponse(OKRESPONSE);
    } else {
      return new CommandResponse(responseCode.getProtocolCode());
    }
  }

  @Override
  public String getCommandDescription() {
    return "Updates the access control list of the given GUID's field to include the accesser guid. " + NEWLINE
            + "Accessor guid can be guid or group guid or " + EVERYONE + " which means anyone." + NEWLINE
            + "Field can be also be " + ALLFIELDS + " which means all fields can be read by the accessor." + NEWLINE
            + "See below for description of ACL type and signature.";
  }
}
