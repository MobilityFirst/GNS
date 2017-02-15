/* Copyright (1c) 2016 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (1the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy */
package edu.umass.cs.gnsclient.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.PacketUtils;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
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
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;

/**
 * Implementation of a GNS client using gigapaxos' async client.
 * 
 * @author arun
 */
public class GNSClient {

	/**
	 * If no properties file can be found, this client will attempt to connect
	 * to a local reconfigurator at the default port
	 * {@link GNSConfig#DEFAULT_RECONFIGURATOR_PORT}.
	 */
	private static final InetSocketAddress DEFAULT_LOCAL_RECONFIGURATOR = new InetSocketAddress(
			InetAddress.getLoopbackAddress(),
			GNSConfig.DEFAULT_RECONFIGURATOR_PORT);

	protected AsyncClient asyncClient;
	// local name server
	private InetSocketAddress GNSProxy = null;

	protected String getLabel() {
		return GNSClient.class.getSimpleName();
	}

	/**
	 * The default constructor that expects a gigapaxos properties file that is
	 * either at the default location,
	 * {@link PaxosConfig#DEFAULT_GIGAPAXOS_CONFIG_FILE} relative to the current
	 * directory, or is specified as a JVM property as
	 * -DgigapaxosConfig=absoluteFilePath. To use {@link GNSClient} with default
	 * properties without a properties file, use
	 * {@link #GNSClient(InetSocketAddress)} that requires knowledge of at least
	 * one reconfigurator address. If this default constructor is used and the
	 * client is unable to find the properties file, it will attempt to connect
	 * to {@code localhost}:{@link GNSConfig#DEFAULT_RECONFIGURATOR_PORT}.
	 *
	 * @throws IOException
	 */
	public GNSClient() throws IOException {
		this(getStaticReconfigurators());
	}
	
	/**
	 * Bootstrap with a single, arbitrarily chosen valid reconfigurator address.
	 * The client can enquire and know of other reconfigurator addresses from
	 * this reconfigurator. If the supplied argument is null, the behavior will
	 * be identical to {@link #GNSClient()}.
	 *
	 * @param anyReconfigurator
	 * @throws IOException
	 */
	public GNSClient(InetSocketAddress anyReconfigurator) throws IOException {
		this(anyReconfigurator != null ? new HashSet<InetSocketAddress>(
				Arrays.asList(anyReconfigurator)) : getStaticReconfigurators());
	}
	
	/**
	 * Same as {@link #GNSClient(InetSocketAddress)} but with the service name
	 * {@code anyReconfiguratorHostName} and implicitly the default
	 * reconfigurator port {@link GNSConfig#DEFAULT_RECONFIGURATOR_PORT}. If the
	 * supplied argument is null, the behavior will be identical to
	 * {@link #GNSClient()}. The supplied {@code anyReconfiguratorHostName}
	 * service name must resolve to one or more valid reconfigurator addresses.
	 *
	 * @param anyReconfiguratorHostName
	 * @throws IOException
	 */
	public GNSClient(String anyReconfiguratorHostName) throws IOException {
		this(new InetSocketAddress(anyReconfiguratorHostName,
				GNSConfig.DEFAULT_RECONFIGURATOR_PORT));
	}
	
	// non-public constructors below
	
	protected GNSClient(boolean checkConnectivity) throws IOException {
		this(getStaticReconfigurators(), checkConnectivity);
	}

	/**
	 * Initialized from properties file. This constant is called static to
	 * reinforce the fact that the set of reconfigurators may change dynamically
	 * but this constant and for that matter even the contents of client
	 * properties file may not.
	 * 
	 * @return the static reconfigurators
	 */
	protected static Set<InetSocketAddress> getStaticReconfigurators() {
		try {
			return ReconfigurationConfig.getReconfiguratorAddresses();
		} catch (Exception e) {
			System.err.println("WARNING: " + e + "\n["
					+ GNSClient.class.getSimpleName()
					+ " unable to find any reconfigurators; falling back to "
					+ ((DEFAULT_LOCAL_RECONFIGURATOR)) + "]");
			return new HashSet<>(Arrays.asList(DEFAULT_LOCAL_RECONFIGURATOR));
		}
	}

	private GNSClient(Set<InetSocketAddress> reconfigurators)
			throws IOException {
		this(reconfigurators, false);
	}
	
	private GNSClient(Set<InetSocketAddress> reconfigurators, boolean checkConnectivity)
			throws IOException {
		this.asyncClient = new AsyncClient(reconfigurators,
				ReconfigurationConfig.getClientSSLMode(),
				ReconfigurationConfig.getClientPortOffset(), checkConnectivity) {
			@Override
			protected String getLabel() {
				return GNSClient.this.getLabel();
			}

			@Override
			public Set<IntegerPacketType> getRequestTypes() {
				return GNSClient.this.getRequestTypes();
			}
		};
	}

	protected Set<IntegerPacketType> getRequestTypes() {
		return CLIENT_PACKET_TYPES;
	}

	@Override
	public String toString() {
		return this.asyncClient.toString();
	}

	private static final String GNS_KEY = "GNS";

	/**
	 * This name represents the prefix used in the local client key database,
	 * and it currently not very meaningful
	 * 
	 * Earlier this name used to represent the service to which this client
	 * connected. This name was also used by the client key database intending
	 * to distinguish between stores corresponding to different GNS services.
	 * Currently reconfigurator(s) are simply read from a properties file, so
	 * this name is unused. It is conceivable also to use a well known service
	 * to query for the reconfigurators given the name of a GNS service provider
	 * but there is no reason to tie it both to {@link GNSClient} and the
	 * keystore. There is also no reason to have different stores for different
	 * GNS providers. A single store can store the information of the provider
	 * if needed, or this information can simply be queried for from a default
	 * provider specified in the properties file.
	 *
	 * <p>
	 *
	 * This name can be changed by setting the system property "GNS" as
	 * "-DGNS=".
	 *
	 * @return GNS service instance
	 */
	@Deprecated
	public static String getGNSProvider() {
		return System.getProperty(GNS_KEY) != null ? System
				.getProperty(GNS_KEY) : "gns.name";
	}

	private static boolean isAnycast(CommandPacket packet) {
		return packet.getCommandType().isCreateDelete()
				|| packet.getCommandType().isSelect()
				|| packet.getServiceName().equals(
						Config.getGlobalString(RC.SPECIAL_NAME));
	}

	/**
	 * This method will force a connectivity check to at least one
	 * reconfigurator or throw an IOException trying to do so.
	 * 
	 * @throws IOException
	 */
	public void checkConnectivity() throws IOException {
		this.asyncClient.checkConnectivity();
	}

	/**
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
	 * @return {@code this}
	 */
	public GNSClient setForceCoordinatedReads(boolean forceCoordinatedReads) {
		this.forceCoordinatedReads = forceCoordinatedReads;
		return this;
	}

	/**
	 * Closes the underlying async client.
	 */
	public void close() {
		this.asyncClient.close();
	}

	/**
	 * All async sends go ultimately go through this async send method because
	 * that is conveniently supported by {@link ReconfigurableAppClientAsync}.
	 *
	 * @param packet
	 * @param callback
	 * @return Long request ID if successfully sent, else null.
	 * @throws IOException
	 */
	private RequestFuture<CommandPacket> sendAsync(CommandPacket packet,
			final Callback<Request, CommandPacket> callback) throws IOException {
		ClientRequest request = packet
				.setForceCoordinatedReads(isForceCoordinatedReads());

		if (isAnycast(packet)) {
			return this.asyncClient.sendRequestAnycast(request, callback);
		} else if (this.GNSProxy != null) {
                        GNSClientConfig.getLogger().log(Level.FINER,
                                "Sending using proxy to {0}", 
                                GNSProxy);
			return this.asyncClient.sendRequest(request, this.GNSProxy,
					callback);
		} else {
			return this.asyncClient.sendRequest(request, callback);
		}
	}

	/**
	 * This method synchronously retrieves the response and checks for and
	 * throws exceptions if needed. This checkResponse behavior is unlike
	 * sendAsync that can not throw exceptions to the caller because the
	 * response is processed by a separate thread.
	 * 
	 * @param packet
	 * @param timeout
	 * @param retries
	 * @return the request
	 * @throws IOException
	 * @throws ClientException
	 */
	private CommandPacket sendSync(CommandPacket packet, final long timeout,
			int retries) throws IOException, ClientException {
		ResponsePacket response = this.sendSyncInternal(packet, timeout,
				retries);
		CommandUtils.checkResponse(nullToTimeoutResponse(response, packet),
				PacketUtils.setResult(packet, response));
		GNSClientConfig.getLogger()
				.log(Level.FINE,
						"{0} received response {0} for request {1}",
						new Object[] { this, response.getSummary(),
								packet.getSummary() });
		return packet;
	}

	/**
	 * Gets a {@link RespponsePacket} or {@link ActiveReplicaError} as a
	 * response.
	 * 
	 * @param packet
	 * @param timeout
	 * @param retries
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	private ResponsePacket sendSyncInternal(CommandPacket packet,
			final long timeout, int retries) throws IOException,
			ClientException {
		ResponsePacket response = null;
		int count = 0;
		do {
			if (count > 0)
				GNSClientConfig
						.getLogger()
						.log(Level.INFO,
								"{0} attempting retransmission {1} upon timeout of {2}; {3}",
								new Object[] { this, count, packet.getSummary(), response==null? "[null response]" : "" });

			try {
				response = defaultHandleResponse(this.sendSyncInternal(packet,
						timeout));
			} catch (ClientException ce) {
				if (ce.getCode() == ResponseCode.TIMEOUT)
					// do nothing
					;
			}
		} while ((count++ < this.numRetriesUponTimeout && (response == null || response
				.getErrorCode() == ResponseCode.TIMEOUT)));
		return (response);
	}

	/**
	 * All sync sends come here, which in turn calls
	 * {@link #sendAsync(CommandPacket, Callback)}{@code .get(timeout)}.
	 * 
	 * @param packet
	 * @param timeout
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	private ResponsePacket sendSyncInternal(CommandPacket packet,
			final long timeout) throws IOException, ClientException {

		ResponsePacket[] processed = new ResponsePacket[1];
		Callback<Request, CommandPacket> future = new Callback<Request, CommandPacket>() {
			@Override
			public CommandPacket processResponse(Request response) {
				GNSClientConfig.getLogger().log(
						Level.FINE,
						"{0} received response {1} for request {2}",
						new Object[] { GNSClient.this, packet.getSummary(),
								response.getSummary() });
				processed[0] = defaultHandleResponse(response);
				return packet;
			}
		};
		try {
			this.sendAsync(packet, future).get(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new ClientException(e);
		}
		return processed[0];

		/* We could also simply have used gigapaxos' sync send above, but using
		 * the async code above avoids the redundancy with sendAsync on checking
		 * for anycast/proxy/default. */
	}

	private int numRetriesUponTimeout = 0;

	/**
	 * @param retries
	 * @return {code this}
	 */
	public GNSClient setNumRetriesUponTimeout(int retries) {
		this.numRetriesUponTimeout = retries;
		return this;
	}

	private CommandPacket sendSync(CommandPacket packet, final long timeout)
			throws IOException, ClientException {
		return this.sendSync(packet, timeout, numRetriesUponTimeout);
	}

	/**
	 *
	 * @param packet
	 * @return the request
	 * @throws IOException
	 * @throws ClientException
	 */
	private CommandPacket sendSync(CommandPacket packet) throws IOException,
			ClientException {
		return this.sendSync(packet, this.forcedTimeout);
	}

	private static ResponsePacket defaultHandleResponse(Request response) {
		return response == null ? null
				: response instanceof ResponsePacket ? (ResponsePacket) response
						: new ResponsePacket(response.getServiceName(),
								((ActiveReplicaError) response).getRequestID(),
								ResponseCode.ACTIVE_REPLICA_EXCEPTION,
								((ReconfiguratorRequest) response)
										.getResponseMessage());
	}

	private static CommandPacket defaultHandleResponse(
			CommandPacket commandPacket, Request response) {
		return PacketUtils.setResult(commandPacket,
				defaultHandleResponse(response));
	}

	/**
	 * This method exists only for backwards compatibility.
	 *
	 * @param commandPacket
	 * @param timeout
	 * @return Response from the server or null if the timeout expires.
	 * @throws IOException
	 * @throws ClientException
	 */
	@Deprecated
	protected ResponsePacket getResponsePacket(CommandPacket commandPacket,
			long timeout) throws IOException, ClientException {
		return nullToTimeoutResponse(this.sendSyncInternal(commandPacket,
				timeout, this.numRetriesUponTimeout), commandPacket);
	}

	private static ResponsePacket nullToTimeoutResponse(
			ResponsePacket response, CommandPacket commandPacket) {
		return response != null ? response : new ResponsePacket(
				commandPacket.getServiceName(), commandPacket.getRequestID(),
				ResponseCode.TIMEOUT, GNSProtocol.BAD_RESPONSE.toString() + " "
						+ GNSProtocol.TIMEOUT.toString() + " for command "
						+ commandPacket.getSummary());
	}

	/**
	 * @param packet
	 * @return Same as {@link #getResponsePacket(CommandPacket, long)} but with
	 *         an infinite timeout.
	 *
	 * @throws IOException
	 * @throws ClientException 
	 */
	protected ResponsePacket getCommandValueReturnPacket(CommandPacket packet)
			throws IOException, ClientException {
		return this.getResponsePacket(packet, 0);
	}

	private static final Set<IntegerPacketType> CLIENT_PACKET_TYPES = new HashSet<>(
			Arrays.asList(Packet.PacketType.COMMAND_RETURN_VALUE));

	/**
	 * Straightforward async client implementation that expects only one packet
	 * type,
	 * {@link edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType#COMMAND_RETURN_VALUE}
	 * . Public in scope so that it can be overridden for testing purposes.
	 */
	public static class AsyncClient extends
			ReconfigurableAppClientAsync<CommandPacket> implements
			AppRequestParserBytes {

		private static Stringifiable<String> unstringer = new StringifiableDefault<>(
				"");

		/**
		 *
		 * @param reconfigurators
		 * @param sslMode
		 * @param clientPortOffset
		 * @param checkConnectivity
		 * @throws IOException
		 */
		public AsyncClient(Set<InetSocketAddress> reconfigurators,
				SSL_MODES sslMode, int clientPortOffset,
				boolean checkConnectivity) throws IOException {
			super(reconfigurators, sslMode, clientPortOffset, checkConnectivity);
			this.enableJSONPackets();
		}

		/**
		 *
		 * @param msg
		 * @return the request
		 * @throws RequestParseException
		 */
		@Override
		public Request getRequest(String msg) throws RequestParseException {
			Request response = null;
			JSONObject json = null;
			try {
				return this.getRequestFromJSON(new JSONObject(msg));
			} catch (JSONException e) {
				GNSClientConfig.getLogger().log(Level.WARNING,
						"Problem parsing packet from {0}: {1}",
						new Object[] { json, e });
			}
			return response;
		}

		/**
		 *
		 * @param json
		 * @return the request
		 * @throws RequestParseException
		 */
		@Override
		public Request getRequestFromJSON(JSONObject json)
				throws RequestParseException {
			Request response = null;
			try {
				Packet.PacketType type = Packet.getPacketType(json);
				if (type != null) {
					GNSClientConfig.getLogger().log(Level.FINER,
							"{0} retrieving packet from received json {1}",
							new Object[] { this, json });
					if (CLIENT_PACKET_TYPES
							.contains(Packet.getPacketType(json))) {
						response = (Request) Packet.createInstance(json,
								unstringer);
					}
					assert (response == null || response.getRequestType() == Packet.PacketType.COMMAND_RETURN_VALUE);
				}
			} catch (JSONException e) {
				GNSClientConfig.getLogger().log(Level.WARNING,
						"Problem parsing packet from {0}: {1}",
						new Object[] { json, e });
			}
			return response;
		}

		/**
		 *
		 * @return a set of packet types
		 */
		@Override
		public Set<IntegerPacketType> getRequestTypes() {
			return CLIENT_PACKET_TYPES;
		}

		/**
		 * FIXME: This should return a separate packet type meant for admin
		 * commands that is different from
		 * {@link edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType#COMMAND}
		 * and carries {@link CommandType} types corresponding to admin
		 * commands.
		 *
		 * @return a set of packet types
		 */
		@Override
		public Set<IntegerPacketType> getMutualAuthRequestTypes() {
			Set<IntegerPacketType> types = new HashSet<IntegerPacketType>(
					Arrays.asList(Packet.PacketType.ADMIN_COMMAND));
			if (InternalCommandPacket.SEPARATE_INTERNAL_TYPE) {
				types.add(Packet.PacketType.INTERNAL_COMMAND);
			}
			return types;
		}

		/**
		 *
		 * @param bytes
		 * @param header
		 * @return the request
		 * @throws RequestParseException
		 */
		@Override
		public Request getRequest(byte[] bytes, NIOHeader header)
				throws RequestParseException {
			return GNSApp.getRequestStatic(bytes, header, unstringer);
		}
	} // End of AsyncClient

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

	private long forcedTimeout = 0;

	/**
	 * @param t
	 * @return {@code this} with a forced default timeout of {@code t}.
	 */
	public GNSClient setForcedTimeout(long t) {
		this.forcedTimeout = t;
		return this;
	}

	/* **************** Start of execute methods ****************** */
	/**
	 * Execute the command in a blocking manner. The result of the execution may
	 * be retrieved using {@link CommandPacket#getResult()} or
	 * {@link CommandPacket#getResultString()} or other methods with the prefix
	 * "getResult" depending on the {@link CommandResultType}.
	 * 
	 * This command will wait arbitrarily long (or a timeout of 0) for a
	 * response; this default behavior may be changed using
	 * {@link #setForcedTimeout(long)}.
	 * 
	 * @param command
	 *            The request to be executed.
	 * @return CommandPacket after execution containing the result if any.
	 * @throws IOException
	 *             if local network or file exceptions occur before execution.
	 * @throws ClientException
	 *             if one of the exception or errors in {@link ResponseCode}
	 *             occurs.
	 */
	public CommandPacket execute(CommandPacket command) throws IOException,
			ClientException {
		return this.sendSync(command);
	}

	/**
	 * Execute the command in a blocking manner. The result of the execution may
	 * be retrieved using {@link CommandPacket#getResult()} or
	 * {@link CommandPacket#getResultString()} or other methods with the prefix
	 * "getResult" depending on the {@link CommandResultType}.
	 * 
	 * @param command
	 *            The request to be executed.
	 * @param timeout
	 *            The period after which, if no response is recieved, a
	 *            {@link ClientException} with a response code
	 *            {@link ResponseCode#TIMEOUT} will be thrown.
	 * @return GNSCommand after execution containing the result if any.
	 * @throws IOException
	 *             if local network or file exceptions occur before execution.
	 * @throws ClientException
	 *             if one of the exception or errors in {@link ResponseCode}
	 *             occurs.
	 */
	public CommandPacket execute(CommandPacket command, long timeout)
			throws IOException, ClientException {
		return this.sendSync(command, timeout);
	}

	/**
	 * Execute the command asynchronously. The result of the execution may be
	 * retrieved using {@link RequestFuture#get()} on the returned future and
	 * then invoking a suitable "getResult" method as in
	 * {@link #execute(CommandPacket)} on the returned {@link CommandPacket}.
	 * 
	 * The {@link RequestFuture#get()} invocation may throw an
	 * {@link ExecutionException} if the execution incurred a
	 * {@link ClientException}.
	 *
	 * @param commandPacket
	 *            The request to be executed.
	 * 
	 * @return A future to retrieve the result of executing {@code command}
	 *         using {@link RequestFuture#get()}.
	 * @throws IOException
	 *             if local network or file exceptions occur before execution.
	 */
	public RequestFuture<CommandPacket> executeAsync(CommandPacket commandPacket)
			throws IOException {
		return this.sendAsync(commandPacket,
				new Callback<Request, CommandPacket>() {
					@Override
					public CommandPacket processResponse(Request response) {
						return defaultHandleResponse(commandPacket, response);
					}
				});
		// Lambdas were causing issues in Andriod - 9/16
	}

	/**
	 * Execute the command asynchronously. Refer
	 * {@link #executeAsync(CommandPacket)}.
	 *
	 * @param commandPacket
	 *            The request to be executed.
	 * 
	 * @param callback
	 * @return A future to retrieve the result of executing {@code command}
	 *         using {@link RequestFuture#get()}.
	 * @throws IOException
	 *             if local network or file exceptions occur during execution.
	 */
	public RequestFuture<CommandPacket> execute(CommandPacket commandPacket,
			Callback<CommandPacket, CommandPacket> callback) throws IOException {
		return this.sendAsync(commandPacket,
				new Callback<Request, CommandPacket>() {
					@Override
					public CommandPacket processResponse(Request response) {
						defaultHandleResponse(commandPacket, response);
						return callback != null ? callback
								.processResponse(commandPacket) : commandPacket;
					}
				});
		// Lambdas were causing issues in Andriod - 9/16
	}

	/**
	 * Used only for testing.
	 *
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new GNSClient();
	}
}
