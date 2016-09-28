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
import static edu.umass.cs.gnscommon.GNSCommandProtocol.ALL_FIELDS;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.FIELD;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.FIELDS;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.READER;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATURE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATUREFULLMESSAGE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.TIMESTAMP;
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
  public static final String[] PARAMS = {GUID, FIELD, 
    READER, SIGNATURE, SIGNATUREFULLMESSAGE};

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
  public CommandResponse execute(InternalRequestHeader internalHeader, JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException, UnsupportedEncodingException {
    String guid = json.getString(GUID);

    // the opt hair below is for the subclasses... cute, huh?
    String field = json.optString(FIELD, null);
    ArrayList<String> fields = json.has(FIELDS)
            ? JSONUtils.JSONArrayToArrayListString(json.getJSONArray(FIELDS)) : null;

    // Reader can be one of three things:
    // 1) a guid - the guid attempting access
    // 2) the value GNSConfig.GNSC.INTERNAL_OP_SECRET - which means this is a request from another server
    // 3) null (or missing from the JSON) - this is an unsigned read 
    String reader = json.optString(READER, null);
    // signature and message can be empty for unsigned cases (reader should be null as well)
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    Date timestamp;
    if (json.has(TIMESTAMP)) {
      timestamp = json.has(TIMESTAMP)
              ? Format.parseDateISO8601UTC(json.getString(TIMESTAMP)) : null; // can be null on older client
    } else {
      timestamp = null;
    }

    if (ALL_FIELDS.equals(field)) {
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
 
}
