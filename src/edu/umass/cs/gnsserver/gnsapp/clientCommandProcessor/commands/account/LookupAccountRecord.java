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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.ResponseCode;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;

/**
 *
 * @author westy
 */
  public class LookupAccountRecord extends AbstractCommand {

  /**
   * Creates a LookupAccountRecord instance.
   *
   * @param module
   */
  public LookupAccountRecord(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.LookupAccountRecord;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, 
          ClientRequestHandlerInterface handler) throws JSONException {
    JSONObject json = commandPacket.getCommand();
    String guid = json.getString(GNSProtocol.GUID.toString());
    AccountInfo acccountInfo;
    if ((acccountInfo = AccountAccess.lookupAccountInfoFromGuidLocally(header, guid, handler)) == null) {
      return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_ACCOUNT.toString() + " " + guid);
    }
    assert (acccountInfo != null);
    {
      try {
        // The true below omits things we don't want to send back to the client.
        // Like the verification code or the list of guids which might be too big to send back to the client
        return new CommandResponse(ResponseCode.NO_ERROR, acccountInfo.toJSONObject(true).toString());
      } catch (JSONException e) {
        return new CommandResponse(ResponseCode.JSON_PARSE_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.JSON_PARSE_ERROR.toString());
      }
    }
  }

}
