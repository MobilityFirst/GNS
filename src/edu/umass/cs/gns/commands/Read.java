/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands;

import edu.umass.cs.gns.client.FieldAccess;
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
public class Read extends GnsCommand {
  
  public Read(CommandModule module) {
    super(module);
  }
  
  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, READER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }
  
  @Override
  public String getCommandName() {
    return READ;
  }
  
  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    // the opt hair below is for the subclasses... cute, huh?
    // reader might be same as guid
    String reader = json.optString(READER, guid);
    // signature and message can be empty for unsigned cases
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
//    GuidInfo guidInfo, readerGuidInfo;
//    if ((guidInfo = AccountAccess.lookupGuidInfo(guid)) == null) {
//      return BADRESPONSE + " " + BADGUID + " " + guid;
//    }
//    
//    if (reader.equals(guid)) {
//      readerGuidInfo = guidInfo;
//    } else if ((readerGuidInfo = AccountAccess.lookupGuidInfo(reader)) == null) {
//      return BADRESPONSE + " " + BADREADERGUID + " " + reader;
//    }
//
//    // unsigned case, must be world readable
//    if (signature == null) {
//      if (!AccessSupport.fieldReadableByEveryone(guidInfo.getGuid(), field)) {
//        return BADRESPONSE + " " + ACCESSDENIED;
//      }
//      // signed case, check signature and access
//    } else if (signature != null) {
//      if (!AccessSupport.verifySignature(readerGuidInfo, signature, message)) {
//        return BADRESPONSE + " " + BADSIGNATURE;
//      } else if (!AccessSupport.verifyAccess(MetaDataTypeName.READ_WHITELIST, guidInfo, field, readerGuidInfo)) {
//        return BADRESPONSE + " " + ACCESSDENIED;
//      }
//    }

    // all checks passed, get the value to return
    if (getCommandName().equals(READONE)) {
      if (ALLFIELDS.equals(field)) {
        return FieldAccess.lookupOneMultipleValues(guid, ALLFIELDS, reader, signature, message);
      } else {
        return FieldAccess.lookupOne(guid, field, reader, signature, message);
      }
    } else {
      if (ALLFIELDS.equals(field)) {
        return FieldAccess.lookupMultipleValues(guid, ALLFIELDS, reader, signature, message);
      } else {
        return FieldAccess.lookup(guid, field, reader, signature, message);
      }
    }
  }
  
  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS for the given guid after authenticating that GUID making request has access authority."
            + " Values are always returned as a JSON list."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields as a JSON object.";
  }
}
