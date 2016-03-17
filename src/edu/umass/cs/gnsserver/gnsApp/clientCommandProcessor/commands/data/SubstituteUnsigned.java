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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data;

import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;

/**
 *
 * @author westy
 */
public class SubstituteUnsigned extends AbstractUpdate {

  /**
   *
   * @param module
   */
  public SubstituteUnsigned(CommandModule module) {
    super(module);
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

  @Override
  public String getCommandName() {
    return SUBSTITUTE;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, VALUE, OLD_VALUE};
  }

  @Override
  public String getCommandDescription() {
    return "Replaces OLD_VALUE with newvalue in the key value pair for the given GUID. See below for more on the signature."
            + " Field must be world writeable as this command does not specify the writer and is not signed.";
  }
}
