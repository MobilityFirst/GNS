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
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.CommandType;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Initializes a new group guid to automatically update and maintain all records that satisfy the query.
 *
 * @author westy
 */
public class SelectGroupSetupQuery extends AbstractCommand {

  /**
   *
   * @param module
   */
  public SelectGroupSetupQuery(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.SelectGroupSetupQuery;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    String accountGuid = json.getString(GNSCommandProtocol.GUID);
    String query = json.getString(GNSCommandProtocol.QUERY);
    String publicKey = json.getString(GNSCommandProtocol.PUBLIC_KEY);
    int interval = json.optInt(GNSCommandProtocol.INTERVAL, -1);

    return FieldAccess.selectGroupSetupQuery(accountGuid, query, publicKey, interval, handler);
  }

  
}
