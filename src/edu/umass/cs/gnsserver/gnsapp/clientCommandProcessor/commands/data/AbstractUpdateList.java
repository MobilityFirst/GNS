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

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.utils.Config;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Date;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public abstract class AbstractUpdateList extends BasicCommand {

  /**
   *
   * @param module
   */
  public AbstractUpdateList(CommandModule module) {
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
    String field = json.getString(FIELD);
    String value = json.getString(VALUE);
    String oldValue = json.optString(OLD_VALUE, null);
    int argument = json.optInt(ARGUMENT, -1);
    String writer = json.optString(WRITER, guid);
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    Date timestamp;
    if (json.has(TIMESTAMP)) {
      timestamp = json.has(TIMESTAMP) ? Format.parseDateISO8601UTC(json.getString(TIMESTAMP)) : null; // can be null on older client
    } else {
      timestamp = null;
    }
    GNSResponseCode responseCode;
//    if (writer.equals(Config.getGlobalString(GNSConfig.GNSC.INTERNAL_OP_SECRET))) {
//      writer = null;
//    }

    if (!(responseCode = FieldAccess.update(header, guid, field,
            JSONUtils.JSONArrayToResultValue(new JSONArray(value)),
            oldValue != null ? JSONUtils.JSONArrayToResultValue(new JSONArray(oldValue)) : null,
            argument,
            getUpdateOperation(),
            writer, signature, message, timestamp, handler)).isExceptionOrError()) {
      return new CommandResponse(GNSResponseCode.NO_ERROR, OK_RESPONSE);
    } else {
      return new CommandResponse(responseCode, BAD_RESPONSE + " " + responseCode.getProtocolCode());
    }

  }
}
