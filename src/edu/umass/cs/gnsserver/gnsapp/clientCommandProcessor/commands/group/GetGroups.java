
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group;

import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GroupAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Date;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class GetGroups extends AbstractCommand {


  public GetGroups(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.GetGroups;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    JSONObject json = commandPacket.getCommand();
    String guid = json.getString(GNSProtocol.GUID.toString());
    // reader might be same as guid
    String reader = json.optString(GNSProtocol.READER.toString(), guid);
    // signature and message can be empty for unsigned cases
    String signature = json.optString(GNSProtocol.SIGNATURE.toString(), null);
    String message = json.optString(GNSProtocol.SIGNATUREFULLMESSAGE.toString(), null);
    Date timestamp = json.has(GNSProtocol.TIMESTAMP.toString()) 
            ? Format.parseDateISO8601UTC(json.getString(GNSProtocol.TIMESTAMP.toString())) : null; // can be null on older client
    return new CommandResponse(ResponseCode.NO_ERROR, new JSONArray(GroupAccess.lookupGroupsLocally(header, 
            commandPacket,
            guid, reader,
            signature, message, timestamp, handler)).toString());
  }
}
