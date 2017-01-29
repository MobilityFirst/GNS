
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;

import org.json.JSONException;


public class Unknown extends AbstractCommand {


  public Unknown(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.Unknown;
  }
  
  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler)
          throws InvalidKeyException, InvalidKeySpecException, JSONException,
          NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException, ParseException {
    throw new UnsupportedOperationException("No implementation.");
  }

  

}
