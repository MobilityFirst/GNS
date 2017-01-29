
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class RetrieveAliases extends AbstractCommand {


  public RetrieveAliases(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.RetrieveAliases;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {
    JSONObject json = commandPacket.getCommand();
    String guid = json.getString(GNSProtocol.GUID.toString());
    String signature = json.getString(GNSProtocol.SIGNATURE.toString());
    String message = json.getString(GNSProtocol.SIGNATUREFULLMESSAGE.toString());
    GuidInfo guidInfo;
    if ((guidInfo = AccountAccess.lookupGuidInfoLocally(header, guid, handler)) == null) {
      return new CommandResponse(ResponseCode.BAD_GUID_ERROR, GNSProtocol.BAD_RESPONSE.toString() 
              + " " + GNSProtocol.BAD_GUID.toString() + " " + guid);
    }
    if (NSAccessSupport.verifySignature(guidInfo.getPublicKey(), signature, message)) {
      AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuidLocally(header, guid, handler);
      if (accountInfo == null) {
        return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR, GNSProtocol.BAD_RESPONSE.toString() 
                + " " + GNSProtocol.BAD_ACCOUNT.toString() + " " + guid);
      } else if (!accountInfo.isVerified()) {
        return new CommandResponse(ResponseCode.VERIFICATION_ERROR, GNSProtocol.BAD_RESPONSE.toString()
                + " " + GNSProtocol.VERIFICATION_ERROR.toString() + " Account not verified");
      }
      ArrayList<String> aliases = accountInfo.getAliases();
      return new CommandResponse(ResponseCode.NO_ERROR, new JSONArray(aliases).toString());
    } else {
      return new CommandResponse(ResponseCode.SIGNATURE_ERROR, GNSProtocol.BAD_RESPONSE.toString()
              + " " + GNSProtocol.BAD_SIGNATURE.toString());
    }
  }

  
}
