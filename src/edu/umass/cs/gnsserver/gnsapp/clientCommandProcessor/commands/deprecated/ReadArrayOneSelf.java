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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArray;

/**
 *
 * @author westy
 */
@Deprecated
public class ReadArrayOneSelf extends ReadArray {

  /**
   *
   * @param module
   */
  public ReadArrayOneSelf(CommandModule module) {
    super(module);
  }
  
  @Override
  public CommandType getCommandType() {
    return CommandType.Unknown;
    //return CommandType.ReadArrayOneSelf;
  }
  
//  @Override
//  public String getCommandName() {
//    return READ_ARRAY_ONE;
//  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS for the given guid after authenticating that GUID making request has access authority. "
            + "Treats the value of key value pair as a singleton item and returns that item."
            + " Specify " + ALL_FIELDS + " as the <field> to return all fields. ";
  }
}
