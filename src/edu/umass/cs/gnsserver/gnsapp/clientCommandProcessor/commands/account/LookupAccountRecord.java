
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.ResponseCode;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;


  public class LookupAccountRecord extends AbstractCommand {


  public LookupAccountRecord(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.LookupAccountRecord;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, 
          ClientRequestHandlerInterface handler) throws JSONException {
    JSONObject json = commandPacket.getCommand();
    String guid = json.getString(GNSProtocol.GUID.toString());
    AccountInfo acccountInfo;
    if ((acccountInfo = AccountAccess.lookupAccountInfoFromGuidLocally(header, guid, handler)) == null) {
      return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_ACCOUNT.toString() + " " + guid);
    }
    if (acccountInfo != null) {
      try {
        // the true below omits the list of guids which might be too big to send back to the client
        return new CommandResponse(ResponseCode.NO_ERROR, acccountInfo.toJSONObject(true).toString());
        //return new CommandResponse(acccountInfo.toJSONObject().toString());
      } catch (JSONException e) {
        return new CommandResponse(ResponseCode.JSON_PARSE_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.JSON_PARSE_ERROR.toString());
      }
    } else {
      return new CommandResponse(ResponseCode.BAD_GUID_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_GUID.toString() + " " + guid);
    }
  }

}
