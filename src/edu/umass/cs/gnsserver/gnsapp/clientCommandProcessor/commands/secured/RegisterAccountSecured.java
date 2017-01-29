
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.secured;

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
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;


public class RegisterAccountSecured extends AbstractCommand {


  public RegisterAccountSecured(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.RegisterAccountSecured;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException,
          InternalRequestException {
    JSONObject json = commandPacket.getCommand();
    String name = json.getString(GNSProtocol.NAME.toString());
    String publicKey = json.getString(GNSProtocol.PUBLIC_KEY.toString());
    String password = json.getString(GNSProtocol.PASSWORD.toString());
    String signature = json.optString(GNSProtocol.SIGNATURE.toString(), null);
    String message = json.optString(GNSProtocol.SIGNATUREFULLMESSAGE.toString(), null);

    String guid = SharedGuidUtils.createGuidStringFromBase64PublicKey(publicKey);
    if (!NSAccessSupport.verifySignature(publicKey, signature, message)) {
      return new CommandResponse(ResponseCode.SIGNATURE_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_SIGNATURE.toString());
    }
    try {
      // Add the account but don't enable email verification
      CommandResponse result = AccountAccess.addAccount(header, commandPacket,
              handler.getHttpServerHostPortString(),
              name, guid, publicKey,
              password, false, handler);
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
