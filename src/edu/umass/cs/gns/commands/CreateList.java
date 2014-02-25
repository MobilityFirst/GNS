/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands;

import edu.umass.cs.gns.client.AccountAccess;
import edu.umass.cs.gns.client.FieldAccess;
import edu.umass.cs.gns.client.MetaDataTypeName;
import edu.umass.cs.gns.client.GuidInfo;
import edu.umass.cs.gns.clientprotocol.AccessSupport;
import static edu.umass.cs.gns.clientprotocol.Defs.*;
import edu.umass.cs.gns.nameserver.ResultValue;
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
public class CreateList extends GnsCommand {

  public CreateList(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, VALUE, WRITER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return CREATELIST;
  }

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    String value = json.getString(VALUE);
    // the opt hair below is for the subclasses... cute, huh?
    // writer might be same as guid
    String writer = json.optString(WRITER, guid);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
//    GuidInfo guidInfo, writerGuidInfo;
//    if ((guidInfo = AccountAccess.lookupGuidInfo(guid)) == null) {
//      return BADRESPONSE + " " + BADGUID + " " + guid;
//    }
//    if (writer.equals(guid)) {
//      writerGuidInfo = guidInfo;
//    } else if ((writerGuidInfo = AccountAccess.lookupGuidInfo(writer)) == null) {
//      return BADRESPONSE + " " + BADWRITERGUID + " " + writer;
//    }
//    try {
//      if (!AccessSupport.verifySignature(writerGuidInfo, signature, message)) {
//        return BADRESPONSE + " " + BADSIGNATURE;
//      } else if (!AccessSupport.verifyAccess(MetaDataTypeName.WRITE_WHITELIST, guidInfo, field, writerGuidInfo)) {
//        return BADRESPONSE + " " + ACCESSDENIED;
//      } else {
    if (FieldAccess.create(guid, field, new ResultValue(value), writer, signature, message)) {
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + DUPLICATEFIELD;
    }

//    } catch (JSONException e) {
//      return BADRESPONSE + " " + JSONPARSEERROR;
//    }
  }

  @Override
  public String getCommandDescription() {
    return "Adds a key value pair to the GNS for the given GUID. Value is a list of items formated as a JSON list.";
  }
}
