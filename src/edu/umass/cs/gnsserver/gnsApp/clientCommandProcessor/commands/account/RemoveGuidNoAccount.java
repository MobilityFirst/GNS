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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account;

import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;

/**
 * Creates a RemoveGuidNoAccount instance.
 * 
 * @author westy
 */
public class RemoveGuidNoAccount extends RemoveGuid {

  /**
   *
   * @param module
   */
  public RemoveGuidNoAccount(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return REMOVE_GUID;
  }

  @Override
  public String getCommandDescription() {
    return "Removes the GUID. "
            + "Must be signed by the guid. "
            + "Returns " + BAD_GUID + " if the GUID has not been registered.";

  }
}
