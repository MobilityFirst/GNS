package edu.umass.cs.gnsserver.gnsapp;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

	/* Note that GNSClient itself doesn't have any fixed timeouts because it is
	 * meant to be used with asynchronous callbacks or with variable timeouts.
	 * GNSClientInternal however always uses blocking calls and must timeout,
	 * otherwise it can limit NIO throughput or or even cause deadlocks under
	 * very high load. */

	private static final long DEFAULT_TIMEOUT = 4000;

	/**
	 * Increased to 6s because it can take ~5s for reconfigurators to checkpoint
	 * RC records for the default CHECKPOINT_INTERVAL of 400. Note that we don't
	 * retransmit create/delete requests in this internal client as we might as
	 * well rely on end-to-end retransmission. But setting a longer timeout here
	 * allows for more requests to succeed in the first or second attempt. A
	 * short timeout might trigger a retransmit of the create (via the
	 * end-client) that might also fail because the original create succeeds
	 * after all after the checkpointing is complete but the retransmitted
	 * create queued right behind it fails silently.
	 */
	private static final long RC_TIMEOUT = 6000;
	// plus 1 second for every 20 names in batch creates
	private static final double BATCH_TIMEOUT_FACTOR = 1000 / 20;

	// plus 1 second for every 16KB in updates
	private static final double SIZE_TIMEOUT_FACTOR = 1000 / 16384;

	/**
	 * Local internal requests should be quick unlike uncoordinated end-to-end
	 * commands like AddGuid or isSelect() commands that can take much longer.
	 */
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

	/**
	 * @param crp
	 *            Create or delete name request.
	 * @return Response code
	 * @throws ClientException
	 */
	public ResponseCode sendRequest(ClientReconfigurationPacket crp)
			throws ClientException {
		assert (crp instanceof CreateServiceName || crp instanceof DeleteServiceName);
		ClientReconfigurationPacket response = null;
		try {
			response = this.asyncClient.sendRequest(crp, getTimeout(crp));
			/* arun: Async client now only returns successful or null (upon a
			 * timeout) responses and throws an exception upon a failure of a
			 * create/delete/request_actives ClientReconfigurationPacket. */
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

	/**
	 * A convenience method to suppress a creation exception if the name already
	 * exists and return {@link ResponseCode#DUPLICATE_ID_EXCEPTION} instead. It
	 * is the caller's responsibility to check whether the pre-existing name's
	 * state is consistent with the state in {@code create}.
	 * 
	 * @param create
	 * @return Response code
	 * @throws ClientException
	 */
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

	/**
	 * Overrides corresponding {@link GNSClient} method with a finite timeout.
	 * 
	 * @return a command packet
	 * @throws java.io.IOException
	 * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
	 */
	@Override
	public CommandPacket execute(CommandPacket command) throws IOException,
			ClientException {
		return this.execute(command, getTimeout(command));
	}
}
