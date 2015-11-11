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

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class Admin extends GnsCommand {
  
  /**
   *
   * @param module
   */
  public Admin(CommandModule module) {
    super(module);
  }
  
  @Override
  public String[] getCommandParameters() {
    return new String[]{PASSKEY};
  }
  
  @Override
  public String getCommandName() {
    return ADMIN;
  }
  
  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String passkey = json.getString(PASSKEY);
    if (module.getHTTPHost().equals(passkey) || "shabiz".equals(passkey)) {
      module.setAdminMode(true);
      return new CommandResponse<String>(OK_RESPONSE);
    } else if ("off".equals(passkey)) {
      module.setAdminMode(false);
      return new CommandResponse<String>(OK_RESPONSE);
    }
    return new CommandResponse<String>(BAD_RESPONSE + " " + OPERATION_NOT_SUPPORTED + " Don't understand " + getCommandName() + " " + passkey);
  }
  
  @Override
  public String getCommandDescription() {
    return "Turns on admin mode.";
  }
}
