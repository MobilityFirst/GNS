
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;


public class ResendAuthenticationEmail extends AbstractCommand {


  public ResendAuthenticationEmail(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.ResendAuthenticationEmail;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException, UnsupportedEncodingException {
    JSONObject json = commandPacket.getCommand();
    String guid = json.getString(GNSProtocol.GUID.toString());
    String signature = json.getString(GNSProtocol.SIGNATURE.toString());
    String message = json.getString(GNSProtocol.SIGNATUREFULLMESSAGE.toString());
    GuidInfo guidInfo;
    if ((guidInfo = AccountAccess.lookupGuidInfoLocally(header, guid, handler)) == null) {
      return new CommandResponse(ResponseCode.BAD_GUID_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_GUID.toString() + " " + guid);
    }
    if (!NSAccessSupport.verifySignature(guidInfo.getPublicKey(), signature, message)) {
      return new CommandResponse(ResponseCode.SIGNATURE_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_SIGNATURE.toString());
    }
    AccountInfo accountInfo;
    if ((accountInfo = AccountAccess.lookupAccountInfoFromGuidLocally(header, guid, handler)) == null) {
      return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_ACCOUNT.toString() + " " + guid);
    } else {
      try {
        return AccountAccess.resendAuthenticationEmail(header, commandPacket, accountInfo,  guid, signature, message, handler);
      } catch (UnknownHostException e) {
        return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR,
                GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.UNSPECIFIED_ERROR.toString() + " " + e.getMessage());
      }
    }
  }
}
