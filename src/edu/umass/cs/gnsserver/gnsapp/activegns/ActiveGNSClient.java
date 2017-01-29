package edu.umass.cs.gnsserver.gnsapp.activegns;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.GNSCommandInternal;
import edu.umass.cs.gnsserver.gnsapp.packet.InternalCommandPacket;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;


public class ActiveGNSClient extends GNSClient implements ActiveDBInterface {


	public ActiveGNSClient() throws IOException {
		super();
	}
	
	public String getLabel() {
		return ActiveGNSClient.class.getSimpleName();
	}

	@Override
	public void write(InternalRequestHeader header, String targetGUID,
			String field, JSONObject value) throws ClientException, InternalRequestException {
		try {
			this.executeCommand(GNSCommandInternal.fieldUpdate(targetGUID, field,
					value, header));
		} catch (IOException | JSONException e) {
			throw new ClientException(e);
		}
	}

	@Override
	public JSONObject read(InternalRequestHeader header, String targetGUID,
			String field) throws ClientException, InternalRequestException {
		try {
			Request request = this.executeCommand(
					GNSCommandInternal.fieldRead(targetGUID, field, header));
			InternalCommandPacket packet = (InternalCommandPacket) request;
			
			return packet.getResultJSONObject();
		} catch (IOException | JSONException e) {
			throw new ClientException(e);
		}
	}

	@Override
	public JSONObject read(InternalRequestHeader header, String targetGUID,
			ArrayList<String> fields) throws ClientException, InternalRequestException {
		try {
			Request request = this.execute(
					GNSCommandInternal.fieldRead(targetGUID, fields, header));
			return ((InternalCommandPacket) request).getResultJSONObject();
		} catch (IOException | JSONException e) {
			throw new ClientException(e);
		}
	}


	public Request executeCommand(CommandPacket commandPacket) throws IOException,
			ClientException {
		if (((GNSCommandInternal) commandPacket).hasBeenCoordinatedOnce()
				&& (commandPacket.needsCoordination() || this
						.isForceCoordinatedReads()
						&& commandPacket.getCommandType().isRead()))
			throw new ClientException(new InternalRequestException(
					ResponseCode.INTERNAL_REQUEST_EXCEPTION,
					"Attempting a second coordinated request in a chain with "
							+ commandPacket.getSummary()));
		return super.execute(commandPacket); //(commandPacket);
	}
}
