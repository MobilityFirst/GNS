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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.activecode;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.ACACTION;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.ACGET;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.GUID;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.READER;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.SIGNATURE;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.SIGNATUREFULLMESSAGE;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;

/**
 * The command to retrieve the active code for the specified GUID and action.
 *
 */
public class Get extends GnsCommand {

  /**
   * Creates a Get instance.
   *
   * @param module
   */
  public Get(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, READER, ACACTION, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return ACGET;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json,
          ClientRequestHandlerInterface handler) throws InvalidKeyException,
          InvalidKeySpecException, JSONException, NoSuchAlgorithmException,
          SignatureException {
    String accountGuid = json.getString(GUID);
    String reader = json.getString(READER);
    String action = json.getString(ACACTION);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    
    return new CommandResponse<>(ActiveCode.getCode(accountGuid, action, reader, signature, message, handler));
    //return new CommandResponse<>(new JSONArray(ActiveCode.getCode(accountGuid, action, reader, signature, message, handler)).toString());
  }

  @Override
  public String getCommandDescription() {
    return "Returns the active code for the specified action,"
            + "ensuring the reader has permission";
  }

}
