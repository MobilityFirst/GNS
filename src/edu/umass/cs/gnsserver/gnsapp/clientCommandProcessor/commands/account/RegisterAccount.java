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

import edu.umass.cs.gnscommon.exceptions.client.OperationNotSupportedException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Set;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;
import redis.clients.jedis.Protocol;

import javax.xml.ws.Response;

/**
 *
 * @author westy
 */
public class RegisterAccount extends AbstractCommand {

  /**
   * Creates a RegisterAccount instance.
   *
   * @param module
   */
  public RegisterAccount(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.RegisterAccount;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException, JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException, InternalRequestException, OperationNotSupportedException {
    if(Config.getGlobalBoolean(GNSConfig.GNSC
            .CREATE_ACCOUNT_REQUIRES_CERTIFICATE)) {
      GNSConfig.getLogger().log(Level.WARNING, "Received unauthorized " +
              "command {0}: {1}", new Object[]{CommandType.RegisterAccount,
              commandPacket});
      throw new OperationNotSupportedException(ResponseCode
              .OPERATION_NOT_SUPPORTED, CommandType.RegisterAccount + " is " +
              "not permitted in this system configuration; use " +
              CommandType.RegisterAccountWithCertificate + " or " + CommandType
              .RegisterAccountSecured + " instead for creating accounts");
    }

    JSONObject json = commandPacket.getCommand();
    String name = json.getString(GNSProtocol.NAME.toString());
    String publicKey = json.getString(GNSProtocol.PUBLIC_KEY.toString());
    String password = json.getString(GNSProtocol.PASSWORD.toString());
    String signature = json.getString(GNSProtocol.SIGNATURE.toString());
    String message = json.getString(GNSProtocol.SIGNATUREFULLMESSAGE.toString());
    
    Set<InetSocketAddress> activesSet = json.has(GNSProtocol.ACTIVES_SET.toString())
    		? Util.getSocketAddresses(json.getJSONArray(GNSProtocol.ACTIVES_SET.toString()))
    		: null;
    		
    
    String guid = SharedGuidUtils.createGuidStringFromBase64PublicKey(publicKey);
    if (!NSAccessSupport.verifySignature(publicKey, signature, message)) {
      return new CommandResponse(ResponseCode.SIGNATURE_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_SIGNATURE.toString());
    }
    try {
      CommandResponse result = AccountAccess.addAccount(header, commandPacket,
              handler.getHttpServerHostPortString(),
              name, guid, publicKey,
              password,
              Config.getGlobalBoolean(GNSConfig.GNSC.ENABLE_EMAIL_VERIFICATION),
              handler, activesSet);
      if (result.getExceptionOrErrorCode().isOKResult()) {
        // Everything is hunkey dorey so return the new guid
        return new CommandResponse(ResponseCode.NO_ERROR, guid);
      } else {
        assert (result.getExceptionOrErrorCode() != null);
        // Otherwise return the error response.
        return result;
      }
    } catch (ClientException | IOException e) {
      return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.UNSPECIFIED_ERROR.toString() + " " + e.getMessage());
    }
  }

}
