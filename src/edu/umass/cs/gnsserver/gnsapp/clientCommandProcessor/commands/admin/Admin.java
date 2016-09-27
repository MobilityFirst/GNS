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

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.ACCESS_DENIED;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.OK_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.PASSKEY;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.UNSPECIFIED_ERROR;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class no longer needs to be used as authentication is being done by Mutual Auth, and so anyone with access to this command could do anything anyways.
 * @author westy
 */
@Deprecated
public class Admin extends BasicCommand {

  private static ArrayList<String> adminAuthStrings;

  /**
   *
   * @param module
   */
  public Admin(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.Admin;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String passkey = json.getString(PASSKEY);
    try {
      GNSConfig.getLogger().log(Level.INFO, "Http host:port = {0}", handler.getHTTPServerHostPortString());
      //Compares the passkey directly against the list in the file specified by admin.auth.  We could instead use hashing here and store the hashes and salt in the file.
      if ("on".equals(passkey)) {
        module.setAdminMode(true);
        return new CommandResponse(GNSResponseCode.NO_ERROR, OK_RESPONSE);
      } else if ("off".equals(passkey)) {
        module.setAdminMode(false);
        return new CommandResponse(GNSResponseCode.NO_ERROR, OK_RESPONSE);
      }
      return new CommandResponse(GNSResponseCode.ACCESS_ERROR, BAD_RESPONSE + " " + ACCESS_DENIED
              + " Failed to authenticate " + getCommandType().toString() + " with key : " + passkey);
    } catch (UnknownHostException e) {
      return new CommandResponse(GNSResponseCode.UNSPECIFIED_ERROR, BAD_RESPONSE
              + " " + UNSPECIFIED_ERROR + " Unable to determine host address");
    }
  }

  
}
