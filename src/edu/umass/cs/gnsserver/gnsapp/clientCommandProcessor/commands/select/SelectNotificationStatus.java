package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select;


import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.commandreply.SelectHandleInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

public class SelectNotificationStatus extends AbstractCommand
{
	/**
	 * 
	 * @param module
	 */
	public SelectNotificationStatus(CommandModule module) 
	{
		super(module);
	}
	
	/**
	 * 
	 * @return the command type
	 */
	@Override
	public CommandType getCommandType() 
	{
		return CommandType.SelectNotificationStatus;
	}
	
	@Override
	public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, 
				ClientRequestHandlerInterface handler) throws JSONException, InternalRequestException 
	{
		JSONObject json = commandPacket.getCommand();
		SelectHandleInfo selectHandle = SelectHandleInfo.fromJSONArray(
				json.getJSONArray(GNSProtocol.SELECT_NOTIFICATION_HANDLE.toString()));
		
		String reader = json.optString(GNSProtocol.GUID.toString(), null);
	    String signature = json.optString(GNSProtocol.SIGNATURE.toString(), null);
	    String message = json.optString(GNSProtocol.SIGNATUREFULLMESSAGE.toString(), null);
	    
	    return FieldAccess.selectNotificationStatus(header, commandPacket, reader, selectHandle,
	            signature, message, handler );
	}
}