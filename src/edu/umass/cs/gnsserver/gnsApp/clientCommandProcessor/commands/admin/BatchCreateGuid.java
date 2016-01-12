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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.BatchTests;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A test routine that batch creates guids.
 * This one creates the public keys on the server so the resulting
 * guids must be accessed using the account guid.
 * See also account.AddMultipleGuids for the real version.
 * 
 * @author westy
 */
public class BatchCreateGuid extends GnsCommand {

  /**
   *
   * @param module
   */
  public BatchCreateGuid(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, GUIDCNT};
  }

  @Override
  public String getCommandName() {
    return BATCH_TEST;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    if (module.isAdminMode()) {
      String accountName = json.getString(NAME);
      String guidCntString = json.getString(GUIDCNT);
      int guidCnt = Integer.parseInt(guidCntString);
      return BatchTests.runBatchTest(accountName, guidCnt, handler);
       
    } else {
      return new CommandResponse<>(BAD_RESPONSE + " " + OPERATION_NOT_SUPPORTED + " Don't understand " + getCommandName());
    }

  }

  @Override
  public String getCommandDescription() {
    return "Creates N guids using batch create for the supplied account. The public key is used"
            + "if we need to create the account.";
  }
}
