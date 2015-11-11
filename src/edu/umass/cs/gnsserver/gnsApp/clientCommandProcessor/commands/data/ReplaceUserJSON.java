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
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import static edu.umass.cs.gnscommon.GnsProtocol.*;

/**
 *
 * @author westy
 */
public class ReplaceUserJSON extends AbstractUpdate {

  /**
   *
   * @param module
   */
  public ReplaceUserJSON(CommandModule module) {
    super(module);
  }

  /**
   * Return the update operation.
   * 
   * @return an {@link UpdateOperation}
   */
  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.USER_JSON_REPLACE;
  }

  @Override
  public String getCommandName() {
    return REPLACE_USER_JSON;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, USER_JSON, WRITER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandDescription() {
    return "Replaces existing fields in JSON record with the given JSONObject's fields. "
            + "Doesn't touch top-level fields that aren't in the given JSONObject.";
  }
}
