package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.triggers;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.exceptions.client.OperationNotSupportedException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor
	.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport
	.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands
	.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands
	.CommandModule;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import org.json.JSONException;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;

public class RemoveTrigger extends AbstractCommand {


	/**
	 * Creates a new <code>AbstractCommand</code> object
	 *
	 * @param module
	 */
	public RemoveTrigger(CommandModule module) {
		super(module);
	}

	@Override
	public CommandType getCommandType() {
		return CommandType.RemoveTrigger;
	}

	@Override
	public CommandResponse execute(InternalRequestHeader internalHeader,
								   CommandPacket commandPacket,
								   ClientRequestHandlerInterface handler)
		throws InvalidKeyException, InvalidKeySpecException, JSONException,
		NoSuchAlgorithmException, SignatureException,
		UnsupportedEncodingException, ParseException,
		InternalRequestException, OperationNotSupportedException,
		FailedDBOperationException, FieldNotFoundException {

		throw new OperationNotSupportedException("Unimplemented");
	}
}
