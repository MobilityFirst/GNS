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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin;

import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.SystemParameter;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSResponseCode;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class GetParameter extends AbstractCommand {

  /**
   *
   * @param module
   */
  public GetParameter(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.GetParameter;
  }

  

//  @Override
//  public String getCommandName() {
//    return GET_PARAMETER;
//  }
  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String parameterString = json.getString(FIELD);
      try {
        return new CommandResponse(GNSResponseCode.NO_ERROR, SystemParameter.valueOf(parameterString.toUpperCase()).getFieldValue().toString());
      } catch (Exception e) {
        System.out.println("Problem getting parameter: " + e);
      }
    return new CommandResponse(GNSResponseCode.QUERY_PROCESSING_ERROR, BAD_RESPONSE + " "
            + " Couldn't get parameter " + CommandType.GetParameter.toString() + " " + parameterString);
  }

  
}
