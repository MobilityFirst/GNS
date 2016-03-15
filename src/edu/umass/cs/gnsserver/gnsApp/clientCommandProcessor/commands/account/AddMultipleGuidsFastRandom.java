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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account;

import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;

/**
 * A test routine that batch creates guids.
 * This one creates the public keys on the server so the resulting
 * guids must be accessed using the account guid.
 * See also account.AddMultipleGuids for the real version.
 * 
 * @author westy
 */
public class AddMultipleGuidsFastRandom extends AddMultipleGuids {

  /**
   *
   * @param module
   */
  public AddMultipleGuidsFastRandom(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUIDCNT, GUID, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandDescription() {
    return "Creates multiple guids for the account associated with the account guid. "
            + "Must be signed by the account guid. "
            + "The created guids can only be accessed using the account guid because the have"
            + "no private key info stored on the client."
            + "Returns " + BAD_GUID + " if the account guid has not been registered.";
  }
}
