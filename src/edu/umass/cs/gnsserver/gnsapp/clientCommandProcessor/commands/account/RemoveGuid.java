
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

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


public class RemoveGuid extends AbstractCommand {


  public RemoveGuid(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.RemoveGuid;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException,
          InternalRequestException {
    JSONObject json = commandPacket.getCommand();
    String guidToRemove = json.getString(GNSProtocol.GUID.toString());
    String accountGuid = json.optString(GNSProtocol.ACCOUNT_GUID.toString(), null);
    String signature = json.getString(GNSProtocol.SIGNATURE.toString());
    String message = json.getString(GNSProtocol.SIGNATUREFULLMESSAGE.toString());
    GuidInfo accountGuidInfo = null;
    GuidInfo guidInfoToRemove;
    if ((guidInfoToRemove = AccountAccess.lookupGuidInfoLocally(header, guidToRemove, handler)) == null) {
      // Removing a non-existant guid is no longer an error.
      return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
      //return new CommandResponse(ResponseCode.BAD_GUID_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_GUID.toString() + " " + guidToRemove);
    }
    if (accountGuid != null) {
      if ((accountGuidInfo = AccountAccess.lookupGuidInfoAnywhere(header, accountGuid, handler)) == null) {
        return new CommandResponse(ResponseCode.BAD_GUID_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_GUID.toString() + " " + accountGuid);
      }
    }
    if (NSAccessSupport.verifySignature(accountGuidInfo != null ? accountGuidInfo.getPublicKey()
            : guidInfoToRemove.getPublicKey(), signature, message)) {
      AccountInfo accountInfo = null;
      if (accountGuid != null) {
        accountInfo = AccountAccess.lookupAccountInfoFromGuidAnywhere(header, accountGuid, handler);
        if (accountInfo == null) {
          return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_ACCOUNT.toString() + " " + accountGuid);
        }
      }
      return AccountAccess.removeGuid(header, commandPacket, guidInfoToRemove, accountInfo, handler);
    } else {
      return new CommandResponse(ResponseCode.SIGNATURE_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_SIGNATURE.toString());
    }
  }
}
