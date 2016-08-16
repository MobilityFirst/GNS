package edu.umass.cs.gnsclient.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.async.RequestCallbackFuture;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnscommon.packets.PacketUtils;
import edu.umass.cs.gnsserver.gnsapp.packet.InternalCommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;

/**
 * @author arun
 *
 *         Cleaner implementation of a GNS client using gigapaxos' async client.
 */
public class GNSClient {
	// initialized from properties file
	private static final Set<InetSocketAddress> STATIC_RECONFIGURATORS = ReconfigurationConfig
			.getReconfiguratorAddresses();

	// initialized upon construction
	private final Set<InetSocketAddress> reconfigurators;
	// ReconfigurableAppClientAsync instance
	private final AsyncClient asyncClient;
	// local name server
	private InetSocketAddress GNSProxy = null;

	

	
	private static final java.util.logging.Logger LOG = GNSConfig.getLogger();

	/**
	 * @throws IOException
	 */
	public GNSClient() throws IOException {
		this(STATIC_RECONFIGURATORS != null
				&& !STATIC_RECONFIGURATORS.isEmpty() ? STATIC_RECONFIGURATORS
				.iterator().next() : null);
	}


	/**
	 * Bootstrap with a single, arbitrarily chosen valid reconfigurator address.
	 * The client can enquire and know of other reconfigurator addresses from
	 * this reconfigurator. If it is unable to do so, it will throw an
	 * IOException.
	 *
	 * @param anyReconfigurator
	 * @throws IOException
	 */
	public GNSClient(InetSocketAddress anyReconfigurator) throws IOException {
		this.reconfigurators = this.knowOtherReconfigurators(anyReconfigurator);
		if (this.reconfigurators == null || this.reconfigurators.isEmpty()) {
			throw new IOException(
					"Unable to find any reconfigurator addresses; "
							+ "at least one needed to initialize client");
		}
		this.asyncClient = new AsyncClient(reconfigurators,
				ReconfigurationConfig.getClientSSLMode(),
				ReconfigurationConfig.getClientPortOffset());
		this.checkConnectivity();
	}

	/**
	 * TODO: implement request/response to know of other reconfigurators. It is
	 * also okay to just use a single reconfigurator address if it is an anycast
	 * address (with the TCP error caveat under route changes).
	 */
	private Set<InetSocketAddress> knowOtherReconfigurators(
			InetSocketAddress anyReconfigurator) throws IOException {
		return anyReconfigurator != null ? new HashSet<>(
				Arrays.asList(anyReconfigurator)) : STATIC_RECONFIGURATORS;
	}

	@Override
	public String toString() {
		return this.asyncClient.toString();
	}



	private static final String GNS_KEY = "GNS";
	
	/**
	 * This name represents the service to which this client connects. Currently
	 * this name is unused as the reconfigurator(s) are read from a properties
	 * file, but it is conceivable also to use a well known service to query for
	 * the reconfigurators given this name. This name is currently also used by
	 * the client key database to distinguish between stores corresponding to
	 * different GNS services.
	 * 
	 * <p>
	 * 
	 * This name can be changed by setting the system property "GNS" as "-DGNS=".
	 *
	 * @return GNS service instance
	 */
	public String getGNSProvider() {
		return System.getProperty(GNS_KEY) != null ? System.getProperty(GNS_KEY)
				: "gns.name";
	}
	  
	private static boolean isAnycast(CommandPacket packet) {
		return packet.getCommandType().isCreateDelete()
				|| packet.getCommandType().isSelect()
				|| packet.getServiceName().equals(
						Config.getGlobalString(RC.SPECIAL_NAME));
	}

	/**
	 * @throws IOException
	 */
	public void checkConnectivity() throws IOException {
		this.asyncClient.checkConnectivity();
	}

	/**
	 * Returns true if the client is forcing read operations to be coordinated.
	 *
	 * @return true if the client is forcing read operations to be coordinated
	 */
	protected boolean isForceCoordinatedReads() {
		return this.forceCoordinatedReads;
	}

	private boolean forceCoordinatedReads = false;

	/**
	 * Sets the value of forcing read operations to be coordinated.
	 *
	 * @param forceCoordinatedReads
	 */
	public void setForceCoordinatedReads(boolean forceCoordinatedReads) {
		this.forceCoordinatedReads = forceCoordinatedReads;
	}
	
	/**
	 * Closes the underlying async client.
	 */
	public void close() {
		this.asyncClient.close();
	}


	/**
	 * All sends go ultimately go through this async send method because that is
	 * what {@link ReconfigurableAppClientAsync} supports.
	 * 
	 * @param packet
	 * @param callback
	 * @return Long request ID if successfully sent, else null.
	 * @throws IOException
	 */
	protected RequestFuture<CommandPacket> sendAsync(CommandPacket packet,
			final Callback<Request, CommandPacket> callback) throws IOException {
		ClientRequest request = packet
				.setForceCoordinatedReads(isForceCoordinatedReads());

		if (isAnycast(packet))
			return this.asyncClient.sendRequestAnycast(request, callback);
		else if (this.GNSProxy != null)
			return this.asyncClient.sendRequest(request, this.GNSProxy,
					callback);
		else
			return this.asyncClient.sendRequest(request, callback);
	}

	protected Request sendSync(CommandPacket packet, final long timeout)
			throws IOException, ClientException {
		ClientRequest request = packet
				.setForceCoordinatedReads(isForceCoordinatedReads());

		Request response = null;
		if (isAnycast(packet))
			response = this.asyncClient.sendRequestAnycast(request, timeout);
		else if (this.GNSProxy != null)
			response = this.asyncClient.sendRequest(request, this.GNSProxy,
					timeout);
		else
			response = this.asyncClient.sendRequest(request, timeout);
		return defaultHandleAndCheckResponse(packet, response);
	}

	protected Request sendSync(CommandPacket packet) throws IOException,
			ClientException {
		return this.sendSync(packet, 0);
	}

	private static final ResponsePacket defaultHandleResponse(
			Request response) {
		return response instanceof ResponsePacket ? (ResponsePacket) response
				: new ResponsePacket(response.getServiceName(),
						((ActiveReplicaError) response).getRequestID(),
						GNSResponseCode.ACTIVE_REPLICA_EXCEPTION,
						((ActiveReplicaError) response).getResponseMessage());
	}

	private static final CommandPacket defaultHandleResponse(
			CommandPacket commandPacket, Request response) {
		return PacketUtils.setResult(commandPacket, defaultHandleResponse(response));
	}

	private static final CommandPacket defaultHandleAndCheckResponse(
			CommandPacket commandPacket, Request response)
			throws ClientException {
		ResponsePacket cvrp = null;
		PacketUtils.setResult(commandPacket, cvrp = defaultHandleResponse(response));
		CommandUtils.checkResponse(cvrp, commandPacket);
		return commandPacket;
	}

	/**
	 * This method exists only for backwards compatibility. 
	 * 
	 * @param commandPacket
	 * @param timeout
	 * @return Response from the server or null if the timeout expires.
	 * @throws IOException
	 */
	protected ResponsePacket getCommandValueReturnPacket(
			CommandPacket commandPacket, long timeout) throws IOException {
		Object monitor = new Object();
		ResponsePacket[] retval = new ResponsePacket[1];

		// send sync also internally sends async first
		this.sendAsync(commandPacket, (response) -> {
			retval[0] = defaultHandleResponse(response);
			assert (retval[0].getErrorCode() != null);
			synchronized (monitor) {
				monitor.notify();
			}
			return commandPacket;
		});

		try {
			synchronized (monitor) {
				// wait for response until timeout
				if (retval[0] == null)
					monitor.wait(timeout);
			}
		} catch (InterruptedException e) {
			throw new IOException(
					"sendSync interrupted while waiting for a response for "
							+ commandPacket.getSummary());
		}
		return retval[0] != null ? retval[0] : new ResponsePacket(commandPacket.getServiceName(),
				commandPacket.getRequestID(), GNSResponseCode.TIMEOUT,
				GNSCommandProtocol.BAD_RESPONSE + " "
						+ GNSCommandProtocol.TIMEOUT + " for command "
						+ commandPacket.getSummary());
	}

	/**
	 * @param packet
	 * @return Same as {@link #getCommandValueReturnPacket(CommandPacket, long)}
	 *         but with an infinite timeout.
	 *
	 * @throws IOException
	 */
	protected ResponsePacket getCommandValueReturnPacket(
			CommandPacket packet) throws IOException {
		return this.getCommandValueReturnPacket(packet, 0);
	}

	/**
	 * Straightforward async client implementation that expects only one packet
	 * type, {@link Packet.PacketType.COMMAND_RETURN_VALUE}.
	 */
	static class AsyncClient extends
			ReconfigurableAppClientAsync<CommandPacket> implements
			AppRequestParserBytes {

		private static Stringifiable<String> unstringer = new StringifiableDefault<>(
				"");

		static final Set<IntegerPacketType> clientPacketTypes = new HashSet<>(
				Arrays.asList(Packet.PacketType.COMMAND_RETURN_VALUE));

		public AsyncClient(Set<InetSocketAddress> reconfigurators,
				SSL_MODES sslMode, int clientPortOffset) throws IOException {
			super(reconfigurators, sslMode, clientPortOffset);
			this.enableJSONPackets();
		}

		@Override
		public Request getRequest(String msg) throws RequestParseException {
			Request response = null;
			JSONObject json = null;
			try {
				return this.getRequestFromJSON(new JSONObject(msg));
			} catch (JSONException e) {
				LOG.log(Level.WARNING, "Problem parsing packet from {0}: {1}",
						new Object[] { json, e });
			}
			return response;
		}

		@Override
		public Request getRequestFromJSON(JSONObject json)
				throws RequestParseException {
			Request response = null;
			try {
				Packet.PacketType type = Packet.getPacketType(json);
				if (type != null) {
					LOG.log(Level.FINER,
							"{0} retrieving packet from received json {1}",
							new Object[] { this, json });
					if (clientPacketTypes.contains(Packet.getPacketType(json))) {
						response = (Request) Packet.createInstance(json,
								unstringer);
					}
					assert (response == null || response.getRequestType() == Packet.PacketType.COMMAND_RETURN_VALUE);
				}
			} catch (JSONException e) {
				LOG.log(Level.WARNING, "Problem parsing packet from {0}: {1}",
						new Object[] { json, e });
			}
			return response;
		}

		@Override
		public Set<IntegerPacketType> getRequestTypes() {
			return clientPacketTypes;
		}
		
		/** FIXME: This should return a separate packet type meant for 
		 * admin commands that is different from {@link Packet.PacketType#COMMAND}
		 * and carries {@link CommandType} types corresponding to admin commands.
		 */
		@SuppressWarnings("javadoc")
		@Override
		public Set<IntegerPacketType> getMutualAuthRequestTypes() {
			Set<IntegerPacketType> types = new HashSet<IntegerPacketType>(
					Arrays.asList(Packet.PacketType.ADMIN_REQUEST));
			if (InternalCommandPacket.SEPARATE_INTERNAL_TYPE)
				types.add(Packet.PacketType.INTERNAL_COMMAND);
			return types;
		}

		@Override
		public Request getRequest(byte[] bytes, NIOHeader header)
				throws RequestParseException {
			return GNSApp.getRequestStatic(bytes, header, unstringer);
		}
	}

	/**
	 * @return The socket address of the GNS proxy if any being used.
	 */
	public InetSocketAddress getGNSProxy() {
		return this.GNSProxy;
	}

	/**
	 * @param LNS
	 *            The address of a GNS proxy if any. Setting this parameter to
	 *            null (also the default value) disables the use of a proxy.
	 */
	public void setGNSProxy(InetSocketAddress LNS) {
		this.GNSProxy = LNS;
	}

	/* **************** Start of execute methods ****************** */

	/**
	 * The result of the execution may be retrieved using
	 * {@link GNSCommand#getResult()} or {@link GNSCommand#getResultString()} or
	 * other methods with the prefix "getResult" depending on the
	 * {@link GNSCommand.ResultType}.
	 * 
	 * @param command
	 * @return GNSCommand after execution containing the result if any.
	 * @throws IOException
	 * @throws ClientException
	 */
	public GNSCommand execute(CommandPacket command) throws IOException,
			ClientException {
		return (GNSCommand) this.sendSync(command);
	}

	/**
	 * @param command
	 * @param timeout
	 * @return GNSCommand after execution containing the result if any.
	 * @throws IOException
	 * @throws ClientException
	 */
	public GNSCommand execute(CommandPacket command, long timeout) throws IOException,
			ClientException {
		return (GNSCommand) this.sendSync(command, timeout);
	}

	/**
	 * The result of the execution may be retrieved using
	 * {@link RequestFuture#get()} on the returned future and then invoking a
	 * suitable "getResult" method as in {@link #execute(CommandPacket)} on the
	 * returned {@link CommandPacket}.
	 * 
	 * @param command
	 * @return A future to retrieve the result of executing {@code command}
	 *         using {@link RequestFuture#get()}.
	 * @throws IOException
	 */
	public RequestFuture<CommandPacket> executeAsync(CommandPacket command)
			throws IOException {
		return this.sendAsync(command, (response) -> {
			return defaultHandleResponse(command, response);
		});
	}

	/**
	 * Refer {@link #executeAsync(CommandPacket)}.
	 * 
	 * @param command
	 * @param callback
	 * @return A future to retrieve the result of executing {@code command}
	 *         using {@link RequestFuture#get()}.
	 * @throws IOException
	 */
	public RequestFuture<CommandPacket> execute(CommandPacket command,
			Callback<CommandPacket, CommandPacket> callback) throws IOException {
		return this.sendAsync(command, (response) -> {
			defaultHandleResponse(command, response);
			return callback != null ? callback.processResponse(command)
					: command;
		});
	}
}
