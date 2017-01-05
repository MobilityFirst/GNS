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
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;

/**
 *
 * @author westy
 */
public class Read extends AbstractCommand {

  /**
   *
   * @param module
   */
  public Read(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.Read;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader internalHeader, CommandPacket commandPacket,
          ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException, UnsupportedEncodingException {
    JSONObject json = commandPacket.getCommand();
    String guid = json.getString(GNSProtocol.GUID.toString());

    assert (internalHeader != null);

    // the opt hair below is for the subclasses... cute, huh?
    String field = json.optString(GNSProtocol.FIELD.toString(), null);
    ArrayList<String> fields = json.has(GNSProtocol.FIELDS.toString())
            ? JSONUtils.JSONArrayToArrayListString(json.getJSONArray(GNSProtocol.FIELDS.toString())) : null;

    // Reader can be one of three things:
    // 1) a guid - the guid attempting access
    // 2) the value GNSConfig.GNSC.INTERNAL_OP_SECRET - which means this is a request from another server
    // 3) null (or missing from the JSON) - this is an unsigned read 
    String reader = json.optString(GNSProtocol.READER.toString(), null);
    // signature and message can be empty for unsigned cases (reader should be null as well)
    String signature = json.optString(GNSProtocol.SIGNATURE.toString(), null);
    String message = json.optString(GNSProtocol.SIGNATUREFULLMESSAGE.toString(), null);
    Date timestamp;
    if (json.has(GNSProtocol.TIMESTAMP.toString())) {
      timestamp = json.has(GNSProtocol.TIMESTAMP.toString())
              ? Format.parseDateISO8601UTC(json.getString(GNSProtocol.TIMESTAMP.toString())) : null; // can be null on older client
    } else {
      timestamp = null;
    }

    if (GNSProtocol.ENTIRE_RECORD.toString().equals(field)) {
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
