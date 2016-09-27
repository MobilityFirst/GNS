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

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import edu.umass.cs.gnscommon.CommandType;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_SIGNATURE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.NAME;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.PASSWORD;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.PUBLIC_KEY;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATURE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATUREFULLMESSAGE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.UNSPECIFIED_ERROR;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
public class RegisterAccount extends BasicCommand {

  /**
   * Creates a RegisterAccount instance.
   *
   * @param module
   */
  public RegisterAccount(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.RegisterAccount;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {
    String name = json.getString(NAME);
    String publicKey = json.getString(PUBLIC_KEY);
    String password = json.getString(PASSWORD);
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);

    String guid = SharedGuidUtils.createGuidStringFromBase64PublicKey(publicKey);
    // FIXME: this lacking signature check is for temporary backward compatability... remove it.
    // See RegisterAccountUnsigned
    if (signature != null && message != null) {
      if (!NSAccessSupport.verifySignature(publicKey, signature, message)) {
        return new CommandResponse(GNSResponseCode.SIGNATURE_ERROR, BAD_RESPONSE + " " + BAD_SIGNATURE);
      }
    }
    try {
      CommandResponse result = AccountAccess.addAccountWithVerification(
              handler.getHTTPServerHostPortString(),
              name, guid, publicKey,
              password, handler);
      if (result.getExceptionOrErrorCode().isOKResult()) {
        // Everything is hunkey dorey so return the new guid
        return new CommandResponse(GNSResponseCode.NO_ERROR, guid);
      } else {
    	  assert(result.getExceptionOrErrorCode()!=null);
        // Otherwise return the error response.
        return result;
      }
    } catch (ClientException | IOException e) {
      return new CommandResponse(GNSResponseCode.UNSPECIFIED_ERROR, BAD_RESPONSE + " " + UNSPECIFIED_ERROR + " " + e.getMessage());
    }
  }

}
