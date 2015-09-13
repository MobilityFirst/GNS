/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.utils.ResultValue;
import edu.umass.cs.gns.gnsApp.NSResponseCode;
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
public class Create extends GnsCommand {

  /**
   *
   * @param module
   */
  public Create(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, VALUE, WRITER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return CREATE;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    // the opt hair below is for the subclasses... cute, huh?
    // value might be null
    String value = json.optString(VALUE, null);
    // writer might be same as guid
    String writer = json.optString(WRITER, guid);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    NSResponseCode responseCode;
    if (!(responseCode = FieldAccess.create(guid, field, (value == null ? new ResultValue() : new ResultValue(Arrays.asList(value))),
            writer, signature, message, handler)).isAnError()) {
      return new CommandResponse<String>(OKRESPONSE);
    } else {
      return new CommandResponse<String>(BADRESPONSE + " " + responseCode.getProtocolCode());
    }
  }

  @Override
  public String getCommandDescription() {
    return "Adds a key value pair to the GNS for the given GUID."
             + " Field must be writeable by the WRITER guid.";
  }
}
