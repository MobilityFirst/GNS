/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data;

import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.NSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import edu.umass.cs.gnsserver.utils.ResultValue;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import java.text.ParseException;
import java.util.Date;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class CreateList extends BasicCommand {

  /**
   *
   * @param module
   */
  public CreateList(CommandModule module) {
    super(module);
  }
  
  @Override
  public CommandType getCommandType() {
    return CommandType.CreateList;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, VALUE, WRITER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return CREATE_LIST;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    String value = json.getString(VALUE);
    // the opt hair below is for the subclasses... cute, huh?
    // writer might be same as guid
    String writer = json.optString(WRITER, guid);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    Date timestamp;
    if (json.has(TIMESTAMP)) {
      timestamp = json.has(TIMESTAMP) ? Format.parseDateISO8601UTC(json.getString(TIMESTAMP)) : null; // can be null on older client
    } else {
      timestamp = null;
    }
    NSResponseCode responseCode;
    if (!(responseCode = FieldAccess.create(guid, field, new ResultValue(value), 
            writer, signature, message, timestamp, handler)).isAnError()) {
      return new CommandResponse<String>(OK_RESPONSE);
    } else {
      return new CommandResponse<String>(BAD_RESPONSE + " " + responseCode.getProtocolCode());
    }
  }

  @Override
  public String getCommandDescription() {
    return "Adds a key value pair to the GNS for the given GUID. Value is a list of items formated as a JSON list."
             + " Field must be writeable by the WRITER guid.";
  }
}
