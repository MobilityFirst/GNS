/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.acl;

import edu.umass.cs.gns.client.FieldMetaData;
import edu.umass.cs.gns.client.GuidInfo;
import edu.umass.cs.gns.clientprotocol.AccessSupport;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import static edu.umass.cs.gns.clientprotocol.Defs.*;
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
    return new String[]{GUID, FIELD, ACCESSER, ACLTYPE, SIGNATURE, "message"};
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
    // accesser might be same as guid
    String accesser = json.optString(ACCESSER, guid);
    String accessType = json.getString(ACLTYPE);
    // signature and message can be empty for unsigned cases
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString("message", null);
    FieldMetaData.MetaDataTypeName access;
    if ((access = FieldMetaData.MetaDataTypeName.valueOf(accessType)) == null) {
      return BADRESPONSE + " " + BADACLTYPE + "Should be one of " + FieldMetaData.MetaDataTypeName.values().toString();
    }
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (AccessSupport.verifySignature(guidInfo, signature, message)) {
      fieldMetaData.add(access, guidInfo, field, accesser);
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS for the given guid after authenticating that GUID making request has access authority."
            + " Values are always returned as a JSON list."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields as a JSON object.";
  }
}
