/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.util.NSResponseCode;
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
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    String value = json.getString(VALUE);
    String oldValue = json.optString(OLDVALUE, null);
    int argument = json.optInt(ARGUMENT, -1);
    String writer = json.optString(WRITER, guid);
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    NSResponseCode responseCode;
    if (!(responseCode = FieldAccess.update(guid, field,
            JSONUtils.JSONArrayToResultValue(new JSONArray(value)),
            oldValue != null ? JSONUtils.JSONArrayToResultValue(new JSONArray(oldValue)) : null,
            argument,
            getUpdateOperation(),
            writer, signature, message, handler)).isAnError()) {
      return new CommandResponse(OKRESPONSE);
    } else {
      return new CommandResponse(BADRESPONSE + " " + responseCode.getProtocolCode());
    }

  }
}
