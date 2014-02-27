/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.FieldAccess;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.packet.NSResponseCode;
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
     NSResponseCode responseCode;
    if (!(responseCode = FieldAccess.update(guid, field,
            new ResultValue(Arrays.asList(value)),
            oldValue != null ? new ResultValue(Arrays.asList(oldValue)) : null,
            getUpdateOperation(), 
            writer, signature, message)).isAnError()) {
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + responseCode.getProtocolCode();
    }
  }
}
