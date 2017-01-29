
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;


  public class ReadArray extends AbstractCommand {


  public ReadArray(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.ReadArray;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    JSONObject json = commandPacket.getCommand();
    String guid = json.getString(GNSProtocol.GUID.toString());
    String field = json.getString(GNSProtocol.FIELD.toString());

    // Reader can be one of three things:
    // 1) a guid - the guid attempting access
    // 2) the value GNSConfig.GNSC.INTERNAL_OP_SECRET - which means this is a request from another server
    // 3) null (or missing from the JSON) - this is an unsigned read 
    String reader = json.optString(GNSProtocol.READER.toString(), null);
    // signature and message can be empty for unsigned cases
    String signature = json.optString(GNSProtocol.SIGNATURE.toString(), null);
    String message = json.optString(GNSProtocol.SIGNATUREFULLMESSAGE.toString(), null);
    Date timestamp;
    if (json.has(GNSProtocol.TIMESTAMP.toString())) {
      timestamp = json.has(GNSProtocol.TIMESTAMP.toString()) ? Format.parseDateISO8601UTC(json.getString(GNSProtocol.TIMESTAMP.toString())) : null; // can be null on older client
    } else {
      timestamp = null;
    }

    if (getCommandType().equals(CommandType.ReadArrayOne)
            || getCommandType().equals(CommandType.ReadArrayOneUnsigned)) {
      if (GNSProtocol.ENTIRE_RECORD.toString().equals(field)) {
        return FieldAccess.lookupOneMultipleValues(header, commandPacket, guid, reader, 
                signature, message, timestamp, handler);
      } else {
        return FieldAccess.lookupOne(header, commandPacket, guid, field, reader, 
                signature, message, timestamp, handler);
      }
    } else if (GNSProtocol.ENTIRE_RECORD.toString().equals(field)) {
      return FieldAccess.lookupMultipleValues(header, commandPacket, guid, reader, 
              signature, message, timestamp, handler);
    } else {
      return FieldAccess.lookupJSONArray(header, commandPacket, guid, field, reader, 
              signature, message, timestamp, handler);
    }
  }
}
