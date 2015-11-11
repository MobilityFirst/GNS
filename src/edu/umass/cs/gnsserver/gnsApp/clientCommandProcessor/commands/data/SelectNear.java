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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.FieldAccess;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A query that returns all guids that have a location field near the given location.
 * 
 * @author westy
 */
public class SelectNear extends GnsCommand {

  /**
   *
   * @param module
   */
  public SelectNear(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{FIELD, NEAR, MAX_DISTANCE};
  }

  @Override
  public String getCommandName() {
    return SELECT;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    String field = json.getString(FIELD);
    String value = json.getString(NEAR);
    String maxDistance = json.getString(MAX_DISTANCE);
    return FieldAccess.selectNear(field, value, maxDistance, handler);
  }

  @Override
  public String getCommandDescription() {
    return "Key must be a GeoSpatial field. Return all fields that are within max distance of value."
            + "Value is a point specified as a JSONArray string tuple: [LONG, LAT]. Max Distance is in meters.";
  }
}
