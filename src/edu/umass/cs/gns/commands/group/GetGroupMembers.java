/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.group;

import edu.umass.cs.gns.client.FieldMetaData;
import edu.umass.cs.gns.client.GuidInfo;
import edu.umass.cs.gns.clientprotocol.AccessSupport;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import static edu.umass.cs.gns.clientprotocol.Defs.*;
import edu.umass.cs.gns.nameserver.ResultValue;
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
public class GetGroupMembers extends GnsCommand {

  public GetGroupMembers(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, READER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return GETGROUPMEMBERS;
  }

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    // reader might be same as guid
    String reader = json.optString(READER, guid);
    // signature and message can be empty for unsigned cases
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    GuidInfo guidInfo, readInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (reader.equals(guid)) {
      readInfo = guidInfo;
    } else if ((readInfo = accountAccess.lookupGuidInfo(reader)) == null) {
      return BADRESPONSE + " " + BADREADERGUID + " " + reader;
    }
    if (!AccessSupport.verifySignature(readInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!AccessSupport.verifyAccess(FieldMetaData.MetaDataTypeName.READ_WHITELIST, guidInfo, GROUP_ACL, readInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else {
      ResultValue values = groupAccess.lookup(guid);
      JSONArray list = new JSONArray(values);
      return list.toString();
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns the members of the group formatted as a JSON Array. Reader guid needs to have read access and sign the command.";
  }
}
