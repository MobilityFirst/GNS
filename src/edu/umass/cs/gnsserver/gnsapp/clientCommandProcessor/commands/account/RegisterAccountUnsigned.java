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

import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;

/**
 * THIS EXISTS FOR BACKWARDS COMPATIBILITY.
 *
 * @author westy
 */
public class RegisterAccountUnsigned extends RegisterAccount {

  /**
   * Creates a RegisterAccount instance.
   *
   * @param module
   */
  public RegisterAccountUnsigned(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.RegisterAccountUnsigned;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, PUBLIC_KEY, PASSWORD};
  }

//  @Override
//  public String getCommandName() {
//    return REGISTER_ACCOUNT;
//  }

  @Override
  public String getCommandDescription() {
    return "Creates an account GUID associated with the human readable name and the supplied public key. "
            + "Must be sign dwith the public key. "
            + "Returns a guid.";

  }
}
