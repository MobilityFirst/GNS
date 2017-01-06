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


import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientCommandProcessorConfig;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ResultValue;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public abstract class AbstractUpdate extends AbstractCommand {

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
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    JSONObject json = commandPacket.getCommand();
    String guid = json.getString(GNSProtocol.GUID.toString());
    String field = json.optString(GNSProtocol.FIELD.toString(), null);
    String value = json.optString(GNSProtocol.VALUE.toString(), null);
    String oldValue = json.optString(GNSProtocol.OLD_VALUE.toString(), null);
    int index = json.optInt(GNSProtocol.N.toString(), -1);
    JSONObject userJSON = json.has(GNSProtocol.USER_JSON.toString()) 
            ? new JSONObject(json.getString(GNSProtocol.USER_JSON.toString())) : null;
    // writer might be unspecified so we use the guid
    String writer = json.optString(GNSProtocol.WRITER.toString(), guid);
    String signature = json.optString(GNSProtocol.SIGNATURE.toString(), null);
    String message = json.optString(GNSProtocol.SIGNATUREFULLMESSAGE.toString(), null);
    Date timestamp = json.has(GNSProtocol.TIMESTAMP.toString())
            ? Format.parseDateISO8601UTC(json.getString(GNSProtocol.TIMESTAMP.toString())) : null; // can be null on older client

    if (json.has("originalBase64")) {
      ClientCommandProcessorConfig.getLogger().log(Level.WARNING, 
              "||||||||||||||||||||||||||| message:{0} original{1}", 
              new Object[]{message, new String(Base64.decode(json.getString("originalBase64")))});
    }
    ResponseCode responseCode;
    if (field == null) {
      responseCode = FieldAccess.updateUserJSON(header, commandPacket,
              guid, userJSON, writer, signature, message, timestamp, handler);
      if (!responseCode.isExceptionOrError()) {
        return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
      } else {
        return new CommandResponse(responseCode, GNSProtocol.BAD_RESPONSE.toString() + " " + responseCode.getProtocolCode());
      }
    } else // single field update
    {
      if (!(responseCode = FieldAccess.update(header, commandPacket,
              guid, field,
              // special case for the ops which do not need a value
              value != null ? new ResultValue(Arrays.asList(value)) : new ResultValue(),
              oldValue != null ? new ResultValue(Arrays.asList(oldValue)) : null,
              index,
              getUpdateOperation(),
              writer, signature, message, timestamp,
              handler)).isExceptionOrError()) {
        return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
      } else {
        return new CommandResponse(responseCode, GNSProtocol.BAD_RESPONSE.toString() + " " + responseCode.getProtocolCode());
      }
    }
  }
}
