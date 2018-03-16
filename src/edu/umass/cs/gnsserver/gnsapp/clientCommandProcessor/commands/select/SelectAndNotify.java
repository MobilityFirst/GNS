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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import edu.umass.cs.gnsserver.utils.JSONUtils;
import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A command for sending notification to all GUIDs that satisfy 
 * a select query. 
 * The response to this command includes a JSONObject that contains 
 * various statistics, like total number of GUIDs that satisfied the query and thereby
 * were sent notifications etc. 
 *  
 * @author ayadav
 */
public class SelectAndNotify extends AbstractCommand {

  /**
   *
   * @param module
   */
  public SelectAndNotify(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.SelectAndNotify;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws JSONException, InternalRequestException {
    JSONObject json = commandPacket.getCommand();
    String reader = json.optString(GNSProtocol.GUID.toString(), null);
    String query = json.getString(GNSProtocol.QUERY.toString());
    
    String notificationStr = json.getString(GNSProtocol.SELECT_NOTIFICATION.toString());
    
    String signature = json.optString(GNSProtocol.SIGNATURE.toString(), null);
    String message = json.optString(GNSProtocol.SIGNATUREFULLMESSAGE.toString(), null);
    // Special case handling of the fields argument
    // Empty means this is an older style select which is converted to null
    // GNSProtocol.ENTIRE_RECORD means what you think and is converted to a unique value which
    // is the fields array is length one and has GNSProtocol.ENTIRE_RECORD string as the first element
    // otherwise it is a list of fields
    ArrayList<String> fields;
    if (!json.has(GNSProtocol.FIELDS.toString())) {
      fields = null;
    } else if (GNSProtocol.ENTIRE_RECORD.toString().equals(json.optString(GNSProtocol.FIELDS.toString()))) {
      fields = new ArrayList<>(Arrays.asList(GNSProtocol.ENTIRE_RECORD.toString()));
    } else {
      fields = JSONUtils.JSONArrayToArrayListString(json.getJSONArray(GNSProtocol.FIELDS.toString()));
    }
    return FieldAccess.selectAndNotify(header, commandPacket, reader, query, fields,
            signature, message, notificationStr, handler );
  }
}