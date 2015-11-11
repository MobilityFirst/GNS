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
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.SystemParameter;
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
public class SetParameter extends GnsCommand {

  /**
   *
   * @param module
   */
  public SetParameter(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, VALUE};
  }

  @Override
  public String getCommandName() {
    return SET_PARAMETER;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String parameterString = json.getString(NAME);
    String value = json.getString(VALUE);
    if (module.isAdminMode()) {
      try {
        SystemParameter.valueOf(parameterString.toUpperCase()).setFieldValue(value);
        return new CommandResponse<String>(OK_RESPONSE);
      } catch (Exception e) {
        System.out.println("Problem setting parameter: " + e);
      }
    }
    return new CommandResponse<String>(BAD_RESPONSE + " " + OPERATION_NOT_SUPPORTED + " Don't understand " + SET_PARAMETER + " " + parameterString + " " + VALUE + " " + value);
  }

  @Override
  public String getCommandDescription() {
    return "[ONLY IN ADMIN MODE] Changes a parameter value.";
  }
}
