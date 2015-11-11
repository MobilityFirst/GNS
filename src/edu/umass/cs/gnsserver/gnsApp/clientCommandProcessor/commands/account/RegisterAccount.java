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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccessSupport;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.ClientUtils;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnscommon.utils.Base64;
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
public class RegisterAccount extends GnsCommand {

  /**
   * Creates a RegisterAccount instance.
   * 
   * @param module
   */
  public RegisterAccount(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, PUBLIC_KEY, PASSWORD};
  }

  @Override
  public String getCommandName() {
    return REGISTER_ACCOUNT;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {
    String name = json.getString(NAME);
    String publicKey = json.getString(PUBLIC_KEY);
    String password = json.getString(PASSWORD);
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    byte[] publicKeyBytes = Base64.decode(publicKey);
    String guid = ClientUtils.createGuidStringFromPublicKey(publicKeyBytes);

    if (signature != null && message != null) { //FIXME: this is for temporary backward compatability... remove it. 
      if (!AccessSupport.verifySignature(publicKey, signature, message)) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_SIGNATURE);
//      } else {
//        GNS.getLogger().info("########SIGNATURE VERIFIED FOR CREATE " + name);
      }
    }
    CommandResponse<String> result = AccountAccess.addAccountWithVerification(module.getHTTPHost(), name, guid, publicKey,
            password, handler);
    if (result.getReturnValue().equals(OK_RESPONSE)) {
      // set up the default read access
      //FieldMetaData.add(MetaDataTypeName.READ_WHITELIST, guid, ALL_FIELDS, EVERYONE, handler);
      return new CommandResponse<String>(guid);
    } else {
      return result;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Creates an account GUID associated with the human readable name and the supplied public key. "
            + "Must be sign dwith the public key. "
            + "Returns a guid.";

  }
}
