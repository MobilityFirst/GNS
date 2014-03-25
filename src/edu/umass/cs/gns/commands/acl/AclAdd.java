/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.acl;

import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.FieldMetaData;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.clientsupport.AccessSupport;
import edu.umass.cs.gns.commands.data.CommandModule;
import edu.umass.cs.gns.commands.data.GnsCommand;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.packet.NSResponseCode;
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
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    String accesser = json.getString(ACCESSER); // who is getting access
    // allows someone other than guid to change the acl, defaults to guid
    String writer = json.optString(WRITER, guid);
    String accessType = json.getString(ACLTYPE);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    MetaDataTypeName access;
    if ((access = MetaDataTypeName.valueOf(accessType)) == null) {
      return BADRESPONSE + " " + BADACLTYPE + "Should be one of " + MetaDataTypeName.values().toString();
    }
    NSResponseCode responseCode;
    if (!(responseCode = FieldMetaData.add(access, guid, field, accesser, writer, signature, message)).isAnError()) {
      return OKRESPONSE;
    } else {
      return responseCode.getProtocolCode();
    }
//    GuidInfo guidInfo;
//    if ((guidInfo = AccountAccess.lookupGuidInfo(guid)) == null) {
//      return BADRESPONSE + " " + BADGUID + " " + guid;
//    }
//    if (AccessSupport.verifySignature(guidInfo, signature, message)) {
//      FieldMetaData.add(access, guidInfo, field, accesser);
//      return OKRESPONSE;
//    } else {
//      return BADRESPONSE + " " + BADSIGNATURE;
//    }
  }

  @Override
  public String getCommandDescription() {
    return "Updates the access control list of the given GUID's field to include the accesser guid. " + NEWLINE
            + "Accessor guid can be guid or group guid or " + EVERYONE + " which means anyone." + NEWLINE
            + "Field can be also be " + ALLFIELDS + " which means all fields can be read by the accessor." + NEWLINE
            + "See below for description of ACL type and signature.";
  }
}
