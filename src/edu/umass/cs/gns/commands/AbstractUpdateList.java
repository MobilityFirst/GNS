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
import edu.umass.cs.gns.client.UpdateOperation;
import edu.umass.cs.gns.clientprotocol.AccessSupport;
import static edu.umass.cs.gns.clientprotocol.Defs.*;
import edu.umass.cs.gns.util.JSONUtils;
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
public abstract class AbstractUpdateList extends GnsCommand {

  public AbstractUpdateList(CommandModule module) {
    super(module);
  }

  public abstract UpdateOperation getUpdateOperation();

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    String value = json.getString(VALUE);
    String oldValue = json.optString(OLDVALUE, null);
    String writer = json.optString(WRITER, guid);
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    GuidInfo guidInfo, writerGuidInfo;
    if ((guidInfo = AccountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (writer.equals(guid)) {
      writerGuidInfo = guidInfo;
    } else if ((writerGuidInfo = AccountAccess.lookupGuidInfo(writer)) == null) {
      return BADRESPONSE + " " + BADWRITERGUID + " " + writer;
    }
    if (signature == null) {
      if (!AccessSupport.fieldWriteableByEveryone(guidInfo.getGuid(), field)) {
        return BADRESPONSE + " " + ACCESSDENIED;
      }
    } else if (signature != null) {
      if (!AccessSupport.verifySignature(writerGuidInfo, signature, message)) {
        return BADRESPONSE + " " + BADSIGNATURE;
      } else if (!AccessSupport.verifyAccess(MetaDataTypeName.WRITE_WHITELIST, guidInfo, field, writerGuidInfo)) {
        return BADRESPONSE + " " + ACCESSDENIED;
      }
    }
    if (FieldAccess.update(guidInfo.getGuid(), field,
            JSONUtils.JSONArrayToResultValue(new JSONArray(value)),
            oldValue != null ? JSONUtils.JSONArrayToResultValue(new JSONArray(oldValue)) : null,
            getUpdateOperation())) {
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + BADFIELD + " " + field;
    }

  }
}
