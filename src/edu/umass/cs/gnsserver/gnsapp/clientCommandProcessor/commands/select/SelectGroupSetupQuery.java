
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;

import org.json.JSONException;
import org.json.JSONObject;


public class SelectGroupSetupQuery extends AbstractCommand {


  public SelectGroupSetupQuery(CommandModule module) {
    super(module);
  }


  @Override
  public CommandType getCommandType() {
    return CommandType.SelectGroupSetupQuery;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws JSONException, InternalRequestException {
    JSONObject json = commandPacket.getCommand();
    String accountGuid = json.getString(GNSProtocol.GUID.toString());
    String query = json.getString(GNSProtocol.QUERY.toString());
    String publicKey = json.getString(GNSProtocol.PUBLIC_KEY.toString());
    int interval = json.optInt(GNSProtocol.INTERVAL.toString(), -1);

    return FieldAccess.selectGroupSetupQuery(header, commandPacket, accountGuid, query, publicKey, interval, handler);
  }

  
}
