package edu.umass.cs.gnsserver.gnsapp;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.PacketUtils;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync.ReconfigurationException;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class GNSClientInternal extends GNSClient {

	private static final Set<IntegerPacketType> INTERNAL_CLIENT_TYPES = new HashSet<>(
			Arrays.asList(Packet.PacketType.COMMAND,
					Packet.PacketType.COMMAND_RETURN_VALUE,
					Packet.PacketType.SELECT_REQUEST,
					Packet.PacketType.SELECT_RESPONSE));

	private final String myID;


	public GNSClientInternal(String myID) throws IOException {
		super(false);
		this.myID = myID;
	}

	@Override
	protected Set<IntegerPacketType> getRequestTypes() {
		return INTERNAL_CLIENT_TYPES;
	}

	@Override
	protected String getLabel() {
		return GNSClientInternal.class.getSimpleName()+":"+this.myID+":";
	}



	private static final long DEFAULT_TIMEOUT = 4000;


	private static final long RC_TIMEOUT = 6000;
	// plus 1 second for every 20 names in batch creates
	private static final double BATCH_TIMEOUT_FACTOR = 1000 / 20;

	// plus 1 second for every 16KB in updates
	private static final double SIZE_TIMEOUT_FACTOR = 1000 / 16384;


	private static final long LOCAL_TIMEOUT = DEFAULT_TIMEOUT / 2;
	private static final long COORDINATION_TIMEOUT = DEFAULT_TIMEOUT;

	private static final long getTimeout(CommandPacket command) {
		if (command.needsCoordination())
			return (COORDINATION_TIMEOUT + (long) (PacketUtils
					.getLengthEstimate(command) * SIZE_TIMEOUT_FACTOR));
		return LOCAL_TIMEOUT;
	}

	private static long getTimeout(ClientReconfigurationPacket crp) {
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

	@Override
	public String toString() {
		return super.toString() + ":" + this.myID;
	}


	public ResponseCode sendRequest(ClientReconfigurationPacket crp)
			throws ClientException {
		assert (crp instanceof CreateServiceName || crp instanceof DeleteServiceName);
		ClientReconfigurationPacket response = null;
		try {
			response = this.asyncClient.sendRequest(crp, getTimeout(crp));

			assert (response == null || !response.isFailed());
		} catch (ReconfigurationException | IOException e) {
			throw new ClientException(e);
		}
		if (response == null) {
			throw new ClientException(ResponseCode.TIMEOUT, this
					+ " timed out on " + crp.getSummary());
		}
		return ResponseCode.NO_ERROR;
	}


	public ResponseCode createOrExists(CreateServiceName create)
			throws ClientException {
		try {
			return this.sendRequest(create);
		} catch (ClientException e) {
			if (e.getCode().equals(ResponseCode.DUPLICATE_ID_EXCEPTION))
				return e.getCode();
			throw e;
		}
	}
        

        public ResponseCode deleteOrNotExists(String name, boolean noErrorIfNotExists)
			throws ClientException {
		try {
			return this.sendRequest(new DeleteServiceName(name));
		} catch (ClientException e) {
			if (e.getCode().equals(ResponseCode.NONEXISTENT_NAME_EXCEPTION)) {
                          if (noErrorIfNotExists) {
                            return ResponseCode.NO_ERROR;
                          } else {
                            return e.getCode();
                          }
                        }
			throw e;
		}
	}


	@Override
	public CommandPacket execute(CommandPacket command) throws IOException,
			ClientException {
		return this.execute(command, getTimeout(command));
	}
}
