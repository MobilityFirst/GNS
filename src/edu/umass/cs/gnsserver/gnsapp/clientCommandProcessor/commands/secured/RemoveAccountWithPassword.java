
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.secured;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.ResponseCode;

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


public class RemoveAccountWithPassword extends AbstractCommand {


  public RemoveAccountWithPassword(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.RemoveAccountWithPassword;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException,
          InternalRequestException {
    JSONObject json = commandPacket.getCommand();
    // The name of the account we are removing.
    String name = json.getString(GNSProtocol.NAME.toString());
    // The guid of this account.
    String password = json.getString(GNSProtocol.PASSWORD.toString());
    String guid = AccountAccess.lookupGuidAnywhere(header, name, handler);
    if (guid == null) {
      // Removing a non-existant guid is not longer an error.
      return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
    }
    AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromNameAnywhere(header, name, handler);
    if (accountInfo == null) {
      return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_ACCOUNT.toString());
    }
    if (!password.equals(accountInfo.getPassword())) {
      return new CommandResponse(ResponseCode.ACCESS_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.ACCESS_DENIED.toString());
    } else {
      return AccountAccess.removeAccount(header, commandPacket, accountInfo, handler);
    }
  }

}
