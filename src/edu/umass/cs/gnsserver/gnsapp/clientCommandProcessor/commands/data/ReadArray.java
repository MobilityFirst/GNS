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
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gnscommon.GNSProtocol;

/**
 * This is the main class for a whole set of commands that support reading of the old style data formatted as
 * JSONArrays. These are here for backward compatibility. The new format also supports JSONArrays as part of the
 * whole "guid data is a JSONObject" format.
 *
 * @author westy
 */
  public class ReadArray extends AbstractCommand {

  /**
   *
   * @param module
   */
  public ReadArray(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.ReadArray;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    String guid = json.getString(GNSProtocol.GUID.toString());
    String field = json.getString(GNSProtocol.FIELD.toString());

    // Reader can be one of three things:
    // 1) a guid - the guid attempting access
    // 2) the value GNSConfig.GNSC.INTERNAL_OP_SECRET - which means this is a request from another server
    // 3) null (or missing from the JSON) - this is an unsigned read 
    String reader = json.optString(GNSProtocol.READER.toString(), null);
    // signature and message can be empty for unsigned cases
    String signature = json.optString(GNSProtocol.SIGNATURE.toString(), null);
    String message = json.optString(GNSProtocol.SIGNATUREFULLMESSAGE.toString(), null);
    Date timestamp;
    if (json.has(GNSProtocol.TIMESTAMP.toString())) {
      timestamp = json.has(GNSProtocol.TIMESTAMP.toString()) ? Format.parseDateISO8601UTC(json.getString(GNSProtocol.TIMESTAMP.toString())) : null; // can be null on older client
    } else {
      timestamp = null;
    }

    if (getCommandType().equals(CommandType.ReadArrayOne)
            || getCommandType().equals(CommandType.ReadArrayOneUnsigned)) {
      if (GNSProtocol.ENTIRE_RECORD.toString().equals(field)) {
        return FieldAccess.lookupOneMultipleValues(header, guid, reader, signature, message, timestamp, handler);
      } else {
        return FieldAccess.lookupOne(header, guid, field, reader, signature, message, timestamp, handler);
      }
    } else if (GNSProtocol.ENTIRE_RECORD.toString().equals(field)) {
      return FieldAccess.lookupMultipleValues(header, guid, reader, signature, message, timestamp, handler);
    } else {
      return FieldAccess.lookupJSONArray(header, guid, field, reader, signature, message, timestamp, handler);
    }
  }
}
