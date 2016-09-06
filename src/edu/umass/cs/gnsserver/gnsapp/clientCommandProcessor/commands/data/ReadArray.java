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
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * This is the main class for a whole set of commands that support reading of the old style data formatted as
 * JSONArrays. These are here for backward compatibility. The new format also supports JSONArrays as part of the
 * whole "guid data is a JSONObject" format.
 *
 * @author westy
 */
public class ReadArray extends BasicCommand {

  /**
   *
   * @param module
   */
  public ReadArray(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.ReadArray;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, READER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }
  
//  @Override
//  public String getCommandName() {
//    return READ_ARRAY;
//  }
  @Override
  public CommandResponse execute(InternalRequestHeader header, JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    // the opt hair below is for the subclasses... cute, huh?
    // reader might be same as guid
    String reader = json.optString(READER, guid);
    // signature and message can be empty for unsigned cases
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    Date timestamp;
    if (json.has(TIMESTAMP)) {
      timestamp = json.has(TIMESTAMP) ? Format.parseDateISO8601UTC(json.getString(TIMESTAMP)) : null; // can be null on older client
    } else {
      timestamp = null;
    }
    if (reader.equals(Config.getGlobalString(GNSConfig.GNSC.INTERNAL_OP_SECRET))) {
      reader = null;
    }

    if (getCommandType().equals(CommandType.ReadArrayOne)
            || getCommandType().equals(CommandType.ReadArrayOneUnsigned)
            //|| getCommandType().equals(CommandType.ReadArrayOneSelf)
            ) {
      if (ALL_FIELDS.equals(field)) {
        return FieldAccess.lookupOneMultipleValues(guid, reader, signature, message, timestamp, handler);
      } else {
        return FieldAccess.lookupOne(guid, field, reader, signature, message, timestamp, handler);
      }
    } else if (ALL_FIELDS.equals(field)) {
      return FieldAccess.lookupMultipleValues(header, guid, reader, signature, message, timestamp, handler);
    } else {
      return FieldAccess.lookupJSONArray(guid, field, reader, signature, message, timestamp, handler);
    }
  }

  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS for the given guid after authenticating that READER making request has access authority."
            + " Values are always returned as a JSON list."
            + " Specify " + ALL_FIELDS + " as the <field> to return all fields as a JSON object.";
  }
}
