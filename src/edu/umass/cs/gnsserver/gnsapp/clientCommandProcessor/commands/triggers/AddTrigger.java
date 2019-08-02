package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.triggers;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.OperationNotSupportedException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor
	.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.*;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands
	.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands
	.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data
	.AbstractUpdate;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

public class AddTrigger extends AbstractCommand {


	/**
	 * @param module
	 */
	public AddTrigger(CommandModule module) {
		super(module);
	}

	@Override
	public CommandType getCommandType() {
		return CommandType.AddTrigger;
	}

	@Override
	public CommandResponse execute(InternalRequestHeader header,
								   CommandPacket commandPacket,
								   ClientRequestHandlerInterface handler)
		throws InvalidKeyException, InvalidKeySpecException, JSONException,
		NoSuchAlgorithmException, SignatureException,
		UnsupportedEncodingException, ParseException,
		InternalRequestException, OperationNotSupportedException,
		FailedDBOperationException, FieldNotFoundException {

		JSONObject json = commandPacket.getCommand();
		String guid = json.getString(GNSProtocol.GUID.toString());
		ArrayList<String> fields = json.has(GNSProtocol.FIELDS.toString()) ?
			JSONUtils.JSONArrayToArrayListString(json.getJSONArray(GNSProtocol
				.FIELDS.toString())) : null;

		// TODO: should default address be the client address?

		// The guid that wants to access this field
		String notifiee = json.getString(GNSProtocol.WRITER.toString());

		String signature = json.getString(GNSProtocol.SIGNATURE.toString());
		String message = json.getString(GNSProtocol.SIGNATUREFULLMESSAGE
			.toString());
		Date timestamp = json.has(GNSProtocol.TIMESTAMP.toString()) ? Format
			.parseDateISO8601UTC(json.getString(GNSProtocol.TIMESTAMP.toString
				())) : null; // can be null on older client

		String ip = json.getString(GNSProtocol.TRIGGER_IP.toString());
		int port = json.getInt(GNSProtocol.TRIGGER_PORT.toString());
		InetSocketAddress sockAddr = (ip!=null && port > 0) ? new
			InetSocketAddress(ip, port) : null;
		String protocol = json.has(GNSProtocol.TRIGGER_PROTOCOL.toString()) ?
			json.getString(GNSProtocol.TRIGGER_PROTOCOL.toString()) :
			GNSProtocol.UDP.toString();

		// Only UDP and HTTP are currently supported
		if (!(protocol.equals(GNSProtocol.UDP.toString())
			|| protocol.equals(GNSProtocol.HTTP.toString()))) {
			return new CommandResponse(ResponseCode
				.UNSUPPORTED_TRIGGER_PROTOCOL, GNSProtocol.BAD_RESPONSE
				.toString() + " " + GNSProtocol.UNSUPPORTED_TRIGGER_PROTOCOL
				.toString() + " " + notifiee);
		}

		if(fields==null) return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol
			.OK_RESPONSE.toString());

		for (String field : fields)
			NSFieldAccess.enforceFieldExists(header, guid, field, handler
				.getApp());

		ResponseCode responseCode;

		GuidInfo accessorGuidInfo;
		String notifieePublicKey;
		if ((accessorGuidInfo = AccountAccess.lookupGuidInfoAnywhere(header,
			notifiee, handler)) == null) {
			return new CommandResponse(ResponseCode.BAD_GUID_ERROR,
				GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol
					.BAD_GUID.toString() + " " + notifiee);
		} else {
			notifieePublicKey = accessorGuidInfo.getPublicKey();
		}

		// construct trigger information
		JSONObject triggerInfo = new JSONObject().put(GNSProtocol.GUID
			.toString(), guid).put
			(GNSProtocol
			.TRIGGER_PROTOCOL.toString(), protocol).put(GNSProtocol
			.TRIGGER_IP.toString(), ip).put(GNSProtocol.TRIGGER_PORT.toString
			(), port);

		if(!(responseCode = FieldMetaData.addTrigger(header, commandPacket,
			guid, fields, notifieePublicKey, notifiee, triggerInfo, signature,
			message, timestamp, handler)).isExceptionOrError()) {
			return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol
				.OK_RESPONSE.toString());
		}
		else {
			return new CommandResponse(responseCode);
		}

		//return null;
	}


}
