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
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AbstractUpdateList;

/**
 *
 * @author westy
 */
@Deprecated
public class SubstituteListSelf extends AbstractUpdateList {

  /**
   *
   * @param module
   */
  public SubstituteListSelf(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.Unknown;
    //return CommandType.SubstituteListSelf;
  }

  /**
   * Return the update operation.
   *
   * @return an {@link UpdateOperation}
   */
  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_SUBSTITUTE;
  }

//  @Override
//  public String getCommandName() {
//    return SUBSTITUTE_LIST;
//  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, VALUE, OLD_VALUE, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandDescription() {
    return "Replaces OLD_VALUE with newvalue in the key value pair for the given GUID. "
            + "Value is a list of items formated as a JSON list.";
  }
}
