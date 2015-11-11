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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
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
      return new CommandResponse<String>(OK_RESPONSE);
    } else {
      return new CommandResponse<String>(BAD_RESPONSE + " " + responseCode.getProtocolCode());
    }
  }

  @Override
  public String getCommandDescription() {
    return "Adds a key value pair to the GNS for the given GUID."
             + " Field must be writeable by the WRITER guid.";
  }
}
