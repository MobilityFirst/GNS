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
 * A query that returns all guids that have a location field within the given area.
 * 
 * @author westy
 */
public class SelectWithin extends GnsCommand {

  /**
   *
   * @param module
   */
  public SelectWithin(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{FIELD, WITHIN};
  }

  @Override
  public String getCommandName() {
    return SELECT;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    String field = json.getString(FIELD);
    String within = json.getString(WITHIN);
    return FieldAccess.selectWithin(field, within, handler);
  }

  @Override
  public String getCommandDescription() {
    return "Key must be a GeoSpatial field. Returns all records whose fields are within value which is a bounding box specified. "
            + "Bounding box is a nested JSONArray string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]";
  }
}
