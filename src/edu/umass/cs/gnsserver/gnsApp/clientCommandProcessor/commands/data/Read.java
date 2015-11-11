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
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.FieldAccess;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class Read extends GnsCommand {

  /**
   *
   * @param module
   */
  public Read(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, READER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return NEWREAD;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    // the opt hair below is for the subclasses... cute, huh?
    String field = json.optString(FIELD, null);
    ArrayList<String> fields = json.has(FIELDS) ? JSONUtils.JSONArrayToArrayListString(json.getJSONArray(FIELDS)): null;
    // reader might be same as guid
    String reader = json.optString(READER, guid);
    // signature and message can be empty for unsigned cases
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);

    if (ALL_FIELDS.equals(field)) {
      return FieldAccess.lookupMultipleValues(guid, reader, signature, message, handler);
    } else if (field != null) {
      return FieldAccess.lookup(guid, field, null, reader, signature, message, handler);
    } else { // multi-field lookup
      return FieldAccess.lookup(guid, null, fields, reader, signature, message, handler);
    } 
  }

  @Override
  public String getCommandDescription() {
    return "Returns a key value pair from the GNS for the given guid after authenticating that READER making request has access authority."
            + " Field can use dot notation to access subfields."
            + " Specify " + ALL_FIELDS + " as the <field> to return all fields as a JSON object.";
  }
}
