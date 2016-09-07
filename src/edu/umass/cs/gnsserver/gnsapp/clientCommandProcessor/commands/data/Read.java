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

import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class Read extends BasicCommand {

  /**
   * Necessary parameters
   */
  public static final String[] PARAMS = {GNSCommandProtocol.GUID, GNSCommandProtocol.FIELD, 
    GNSCommandProtocol.READER, GNSCommandProtocol.SIGNATURE, GNSCommandProtocol.SIGNATUREFULLMESSAGE};

  /**
   *
   * @param module
   */
  public Read(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.Read;
  }

  @Override
  public String[] getCommandParameters() {
    return PARAMS;
  }
  
  private static final boolean HACK_WESTY_SHOULD_FIX = true;

  @Override
  public CommandResponse execute(InternalRequestHeader internalHeader, JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException, UnsupportedEncodingException {
    String guid = json.getString(GNSCommandProtocol.GUID);

    // the opt hair below is for the subclasses... cute, huh?
    String field = json.optString(GNSCommandProtocol.FIELD, null);
    ArrayList<String> fields = json.has(GNSCommandProtocol.FIELDS)
            ? JSONUtils.JSONArrayToArrayListString(json.getJSONArray(GNSCommandProtocol.FIELDS)) : null;

    String reader = json.has(GNSCommandProtocol.READER) ? json.getString(GNSCommandProtocol.READER) : HACK_WESTY_SHOULD_FIX ? guid : null;
    // signature and message can be empty for unsigned cases
    String signature = json.optString(GNSCommandProtocol.SIGNATURE, null);
    String message = json.optString(GNSCommandProtocol.SIGNATUREFULLMESSAGE, null);
    Date timestamp;
    if (json.has(GNSCommandProtocol.TIMESTAMP)) {
      timestamp = json.has(GNSCommandProtocol.TIMESTAMP)
              ? Format.parseDateISO8601UTC(json.getString(GNSCommandProtocol.TIMESTAMP)) : null; // can be null on older client
    } else {
      timestamp = null;
    }

    if (GNSCommandProtocol.ALL_FIELDS.equals(field)) {
      return FieldAccess.lookupMultipleValues(internalHeader, guid, reader,
              signature, message, timestamp, handler);
    } else if (field != null) {
      return FieldAccess.lookupSingleField(internalHeader, guid, field, reader, signature,
              message, timestamp, handler);
    } else { // multi-field lookup
      return FieldAccess.lookupMultipleFields(internalHeader, guid, fields, reader, signature,
              message, timestamp, handler);
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns a key value pair from the GNS for the given guid after authenticating that READER making request has access authority."
            + " Field can use dot notation to access subfields."
            + " Specify " + GNSCommandProtocol.ALL_FIELDS + " as the <field> to return all fields as a JSON object.";
  }
}
