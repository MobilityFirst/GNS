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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * An admin command that lets us delete single GUID/HRN records.
 *
 * @author westy
 */
public class DeleteRecord extends AbstractCommand {

  /**
   *
   * @param module
   */
  public DeleteRecord(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.DeleteRecord;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket,
          ClientRequestHandlerInterface handler) throws JSONException {
    JSONObject json = commandPacket.getCommand();
    String guid = json.getString(GNSProtocol.GUID.toString());
    ResponseCode deleteGuidResponseCode;
    try {
      deleteGuidResponseCode = handler.getInternalClient().deleteOrNotExists(guid, true);
    } catch (ClientException e) {
      return new CommandResponse(e.getCode(),
              GNSProtocol.BAD_RESPONSE.toString()
              + " Failed to delete " + guid + ": " + e.getMessage());
    }
    if (deleteGuidResponseCode.isOKResult()) {
      return new CommandResponse(ResponseCode.NO_ERROR,
              GNSProtocol.OK_RESPONSE.toString());
    } else {
      return new CommandResponse(deleteGuidResponseCode,
              GNSProtocol.BAD_RESPONSE.toString()
              + " Failed to delete " + guid);
    }
  }

}
