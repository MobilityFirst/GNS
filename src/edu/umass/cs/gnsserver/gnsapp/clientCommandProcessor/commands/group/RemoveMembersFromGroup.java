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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GroupAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.GNSResponseCode;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class RemoveMembersFromGroup extends AbstractCommand {

  /**
   *
   * @param module
   */
  public RemoveMembersFromGroup(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.RemoveMembersFromGroup;
  }

  

//  @Override
//  public String getCommandName() {
//    return REMOVE_FROM_GROUP;
//  }
  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String members = json.getString(MEMBERS);
    // writer might be same as guid
    String writer = json.optString(WRITER, guid);
    // signature and message can be empty for unsigned cases
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    GNSResponseCode responseCode;
    try {
      if (!(responseCode = GroupAccess.removeFromGroup(guid, new ResultValue(members), writer, signature,
              message, handler)).isExceptionOrError()) {
        return new CommandResponse(GNSResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
      } else {
        return new CommandResponse(responseCode, BAD_RESPONSE + " " + responseCode.getProtocolCode());
      }
    } catch (ClientException | IOException e) {
      return new CommandResponse(GNSResponseCode.UNSPECIFIED_ERROR, BAD_RESPONSE + " " + UNSPECIFIED_ERROR + " " + e.getMessage());
    }
  }

  
}
