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

import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.FIELD;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.N;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.OLD_VALUE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATURE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATUREFULLMESSAGE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.TIMESTAMP;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.USER_JSON;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.VALUE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.WRITER;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ResultValue;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public abstract class AbstractUpdate extends BasicCommand {

  /**
   *
   * @param module
   */
  public AbstractUpdate(CommandModule module) {
    super(module);
  }

  /**
   * Return the update operation.
   *
   * @return an {@link UpdateOperation}
   */
  public abstract UpdateOperation getUpdateOperation();

  @Override
  public CommandResponse execute(InternalRequestHeader header, JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    String guid = json.getString(GUID);
    String field = json.optString(FIELD, null);
    String value = json.optString(VALUE, null);
    String oldValue = json.optString(OLD_VALUE, null);
    int index = json.optInt(N, -1);
    JSONObject userJSON = json.has(USER_JSON) ? new JSONObject(json.getString(USER_JSON)) : null;
    // writer might be unspecified so we use the guid
    String writer = json.optString(WRITER, guid);
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    Date timestamp = json.has(TIMESTAMP) ? 
            Format.parseDateISO8601UTC(json.getString(TIMESTAMP)) : null; // can be null on older client

    GNSResponseCode responseCode;
    if (field == null) {
      responseCode = FieldAccess.updateUserJSON(header, guid, userJSON, 
              writer, signature, message, timestamp, handler);
      if (!responseCode.isExceptionOrError()) {
        return new CommandResponse(GNSResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
      } else {
        return new CommandResponse(responseCode, BAD_RESPONSE + " " + responseCode.getProtocolCode());
      }
    } else {
      // single field update
      if (!(responseCode = FieldAccess.update(header, guid, field,
              // special case for the ops which do not need a value
              value != null ? new ResultValue(Arrays.asList(value)) : new ResultValue(),
              oldValue != null ? new ResultValue(Arrays.asList(oldValue)) : null,
              index,
              getUpdateOperation(),
              writer, signature, message, timestamp,
              handler)).isExceptionOrError()) {
        return new CommandResponse(GNSResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
      } else {
        return new CommandResponse(responseCode, BAD_RESPONSE + " " + responseCode.getProtocolCode());
      }
    }
  }
}
