package edu.umass.cs.gnsserver.gnsapp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync.ReconfigurationException;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;

/**
 * @author arun
 * 
 *         GNSClient for use at servers for remote server queries.
 *
 */
public class GNSClientInternal extends GNSClient {

	private static final Set<IntegerPacketType> INTERNAL_CLIENT_TYPES = new HashSet<>(
			Arrays.asList(Packet.PacketType.COMMAND,
					Packet.PacketType.COMMAND_RETURN_VALUE,
					Packet.PacketType.SELECT_REQUEST,
					Packet.PacketType.SELECT_RESPONSE));

	private final String myID;

	/**
	 * @param myID
	 * @throws IOException
	 */
	public GNSClientInternal(String myID) throws IOException {
		this.asyncClient = new GNSClient.AsyncClient(
				GNSClient.getStaticReconfigurators(),
				ReconfigurationConfig.getClientSSLMode(),
				ReconfigurationConfig.getClientPortOffset(), false) {
			public Set<IntegerPacketType> getRequestTypes() {
				return INTERNAL_CLIENT_TYPES;
			}
		};
		this.myID = myID;
	}

	private static final long RC_TIMEOUT = 4000;
	// plus 1 second for every 20 names
	private static final double BATCH_TIMEOUT_FACTOR = 1000 / 20;

	private static final long getTimeout(ClientReconfigurationPacket crp) {
		long timeout = RC_TIMEOUT;
		switch (crp.getType()) {
		case CREATE_SERVICE_NAME:
			timeout += ((CreateServiceName) crp).nameStates != null ? ((CreateServiceName) crp).nameStates
					.size() * BATCH_TIMEOUT_FACTOR
					: 0;
		default:
		}
		return timeout;
	}

	public String toString() {
		return super.toString() + ":" + this.myID;
	}

	/**
	 * @param crp
	 *            Create or delete name request.
	 * @return Response code
	 * @throws IOException
	 * @throws ClientException
	 */
	public ResponseCode sendRequest(ClientReconfigurationPacket crp)
			throws IOException, ClientException {
		assert (crp instanceof CreateServiceName || crp instanceof DeleteServiceName);
		ClientReconfigurationPacket response = null;
		try {
			response = (ClientReconfigurationPacket) this.asyncClient
					.sendRequest(crp, getTimeout(crp));
			if (response != null)
				if (!response.isFailed())
					return ResponseCode.NO_ERROR;
				else if (response instanceof CreateServiceName
						&& response.getResponseCode() == ClientReconfigurationPacket.ResponseCodes.DUPLICATE_ERROR)
					return ResponseCode.DUPLICATE_ID_EXCEPTION;
		} catch (ReconfigurationException e) {
			throw new ClientException(ResponseCode.RECONFIGURATION_EXCEPTION,
					e.getCode() + ":" + e.getMessage());
		}
		if (response == null)
			throw new ClientException(ResponseCode.TIMEOUT, this
					+ " timed out on " + crp.getSummary());
		// can't get here
		return ResponseCode.UNSPECIFIED_ERROR;
	}
}
