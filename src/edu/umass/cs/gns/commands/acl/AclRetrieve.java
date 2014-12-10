/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.acl;

import edu.umass.cs.gns.clientsupport.CommandResponse;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.FieldMetaData;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class AclRetrieve extends GnsCommand {

  public AclRetrieve(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, ACLTYPE, READER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return ACLRETRIEVE;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    String accessType = json.getString(ACLTYPE);
    // allows someone other than guid to change the acl, defaults to guid
    String reader = json.optString(READER, guid);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    MetaDataTypeName access;
    if ((access = MetaDataTypeName.valueOf(accessType)) == null) {
      return new CommandResponse(BADRESPONSE + " " + BADACLTYPE + "Should be one of " + MetaDataTypeName.values().toString());
    }
    return new CommandResponse(new JSONArray(FieldMetaData.lookup(access, guid, field, reader, signature, message)).toString());
//    GuidInfo guidInfo;
//    if ((guidInfo = AccountAccess.lookupGuidInfo(guid)) == null) {
//      return new CommandResponse(BADRESPONSE + " " + BADGUID + " " + guid);
//    }
//    if (AccessSupport.verifySignature(guidInfo, signature, message)) {
//      Set<String> values = FieldMetaData.lookup(access, guidInfo, field);
//      return new JSONArray(values).toString();
//    } else {
//      return new CommandResponse(BADRESPONSE + " " + BADSIGNATURE);
//    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns the access control list for a guids's field. See below for description of ACL type and signature.";

  }
}
