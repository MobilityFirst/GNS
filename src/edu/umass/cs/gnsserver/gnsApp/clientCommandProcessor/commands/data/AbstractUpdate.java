/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.gnsserver.utils.ValuesMap;
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

  /**
   *
   * @param module
   */
  public AbstractUpdate(CommandModule module) {
    super(module);
  }

  /**
   * Return the update operation.
   * 
   * @return an {@link UpdateOperation}
   */
  public abstract UpdateOperation getUpdateOperation();

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String field = json.optString(FIELD, null);
    String value = json.optString(VALUE, null);
    String oldValue = json.optString(OLDVALUE, null);
    int index = json.optInt(N, -1);
    JSONObject userJSON = json.has(USERJSON) ? new JSONObject(json.getString(USERJSON)) : null;
    // writer might be unspecified so we use the guid
    String writer = json.optString(WRITER, guid);
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    NSResponseCode responseCode;
    if (field == null) {
      // full JSON object update
      if (!(responseCode = handler.getIntercessor().sendUpdateUserJSON(guid, new ValuesMap(userJSON), 
              getUpdateOperation(), writer, signature, message, false)).isAnError()) {
         return new CommandResponse<String>(OKRESPONSE);
      } else {
        return new CommandResponse<String>(BADRESPONSE + " " + responseCode.getProtocolCode());
      }
    } else {
      // single field update 
      if (!(responseCode = FieldAccess.update(guid, field,
              // special case for the ops which do not need a value
              value != null ? new ResultValue(Arrays.asList(value)) : new ResultValue(),
              oldValue != null ? new ResultValue(Arrays.asList(oldValue)) : null,
              index,
              getUpdateOperation(),
              writer, signature, message, handler)).isAnError()) {
        return new CommandResponse<String>(OKRESPONSE);
      } else {
        return new CommandResponse<String>(BADRESPONSE + " " + responseCode.getProtocolCode());
      }
    }
  }
}
