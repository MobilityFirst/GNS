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

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gnscommon.GnsProtocol.*;

/**
 * Initializes the given group guid to automatically update and maintain all records that satisfy the query.
 *
 * @author westy
 */
public class SelectGroupSetupQueryWithGuid extends SelectGroupSetupQuery {

  /**
   *
   * @param module
   */
  public SelectGroupSetupQueryWithGuid(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{QUERY, GUID};
  }

  @Override
  public String getCommandDescription() {
    return "Initializes the given group guid to automatically update and maintain all records that satisfy the query."
            + "For details see http://gns.name/wiki/index.php/Query_Syntax "
            + "Values are returned as a JSON array of JSON Objects.";
  }
}
