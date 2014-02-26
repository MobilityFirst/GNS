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
import edu.umass.cs.gns.client.GuidInfo;
import edu.umass.cs.gns.client.MetaDataTypeName;
import edu.umass.cs.gns.client.UpdateOperation;
import edu.umass.cs.gns.clientprotocol.AccessSupport;
import static edu.umass.cs.gns.clientprotocol.Defs.*;
import edu.umass.cs.gns.nameserver.ResultValue;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public abstract class AbstractUpdate extends GnsCommand {

  public AbstractUpdate(CommandModule module) {
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
    // writer might be unspecified so we use the guid
    String writer = json.optString(WRITER, guid);
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    if (FieldAccess.update(guid, field,
            new ResultValue(Arrays.asList(value)),
            oldValue != null ? new ResultValue(Arrays.asList(oldValue)) : null,
            getUpdateOperation(), 
            writer, signature, message)) {
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + BADFIELD + " " + field;
    }
  }
}
