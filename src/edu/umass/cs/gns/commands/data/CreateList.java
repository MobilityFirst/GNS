/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.commands.CommandModule;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Defs.*;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.NSResponseCode;
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
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    String value = json.getString(VALUE);
    // the opt hair below is for the subclasses... cute, huh?
    // writer might be same as guid
    String writer = json.optString(WRITER, guid);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    NSResponseCode responseCode;
    if (!(responseCode = FieldAccess.create(guid, field, new ResultValue(value), writer, signature, message, handler)).isAnError()) {
      return new CommandResponse(OKRESPONSE);
    } else {
      return new CommandResponse(BADRESPONSE + " " + responseCode.getProtocolCode());
    }
  }

  @Override
  public String getCommandDescription() {
    return "Adds a key value pair to the GNS for the given GUID. Value is a list of items formated as a JSON list."
             + " Field must be writeable by the WRITER guid.";
  }
}
