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
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
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

	// ReconfigurableAppClientAsync instance, protected and nonfinal so
	// BadClient in an admin test can override this.
	/**
   *
   */
	protected AsyncClient asyncClient;
	// local name server
	private InetSocketAddress GNSProxy = null;

	private static final java.util.logging.Logger LOG = GNSClientConfig
			.getLogger();

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
	 * to localhost:{@link GNSConfig#DEFAULT_RECONFIGURATOR_PORT}.
	 *
	 * @throws IOException
	 */
	public GNSClient() throws IOException {
		this(getStaticReconfigurators());
	}

	/**
	 * Initialized from properties file. This constant is called static to
	 * reinforce the fact that the set of reconfigurators may change dynamically
	 * but this constant and for that matter even the contents of client
	 * properties file may not.
	 */
	private static Set<InetSocketAddress> getStaticReconfigurators() {
		try {
			return ReconfigurationConfig.getReconfiguratorAddresses();
		} catch (Exception e) {
			System.err.println("WARNING: " + e + "\n["
					+ GNSClient.class.getSimpleName()
					+ " unable to find any reconfigurators; falling back to "
					+ ((DEFAULT_LOCAL_RECONFIGURATOR)) + "]");
			return new HashSet<InetSocketAddress>(
					Arrays.asList(DEFAULT_LOCAL_RECONFIGURATOR));
		}
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

	private GNSClient(Set<InetSocketAddress> reconfigurators)
			throws IOException {
		this.asyncClient = new AsyncClient(reconfigurators,
				ReconfigurationConfig.getClientSSLMode(),
				ReconfigurationConfig.getClientPortOffset(), true);
	}

	/**
	 * Same as {@link #GNSClient(InetSocketAddress)} but with the host name
	 * {@code anyReconfiguratorHostName} and implicitly the default
	 * reconfigurator port {@link GNSConfig#DEFAULT_RECONFIGURATOR_PORT}. If the
	 * supplied argument is null, the behavior will be identical to
	 * {@link #GNSClient()}.
	 *
	 * @param anyReconfiguratorHostName
	 * @throws IOException
	 */
	public GNSClient(String anyReconfiguratorHostName) throws IOException {
		this(new InetSocketAddress(anyReconfiguratorHostName,
				GNSConfig.DEFAULT_RECONFIGURATOR_PORT));
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
	 * This name can be changed by setting the system property "GNS" as
	 * "-DGNS=".
	 *
	 * @return GNS service instance
	 */
	public String getGNSProvider() {
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

		if (isAnycast(packet)) {
			return this.asyncClient.sendRequestAnycast(request, callback);
		} else if (this.GNSProxy != null) {
			return this.asyncClient.sendRequest(request, this.GNSProxy,
					callback);
		} else {
			return this.asyncClient.sendRequest(request, callback);
		}
	}

	/**
	 *
	 * This method synchronously retrieves the response and checks for and
	 * throws exceptions if needed. This checkResponse behavior is unlike
	 * sendAsync that can not throw exceptions to the caller because the
	 * response is processed by a separate thread.
	 * 
	 * @param packet
	 * @param timeout
	 * @return the request
	 * @throws IOException
	 * @throws ClientException
	 */
	protected CommandPacket sendSync(CommandPacket packet, final long timeout,
			int retries) throws IOException, ClientException {
		CommandUtils.checkResponse(
				this.sendSyncInternal(packet, timeout, retries), packet);
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
	 */
	private ResponsePacket sendSyncInternal(CommandPacket packet,
			final long timeout, int retries) throws IOException {
		Request response = null;
		int count = 0;
		do {
			// if timeout is 0, the retries are pointless
			assert (count == 0 || timeout > 0);
			if (count > 0)
				LOG.log(Level.INFO,
						"{0} attempting retransmission upon timeout #{1}",
						new Object[] { this, count });
			response = this.sendSyncInternal(packet, timeout);
		} while (response == null && count++ < this.numRetriesUponTimeout);
		return defaultHandleResponse(response);
	}

	private ResponsePacket sendSyncInternal(CommandPacket packet,
			final long timeout) throws IOException {
		ClientRequest request = packet
				.setForceCoordinatedReads(isForceCoordinatedReads());

		Request response = null;
		if (isAnycast(packet)) {
			response = this.asyncClient.sendRequestAnycast(request, timeout);
		} else if (this.GNSProxy != null) {
			response = this.asyncClient.sendRequest(request, this.GNSProxy,
					timeout);
		} else {
			response = this.asyncClient.sendRequest(request, timeout);
		}
		return defaultHandleResponse(response);
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

	protected CommandPacket sendSync(CommandPacket packet, final long timeout)
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
	protected CommandPacket sendSync(CommandPacket packet) throws IOException,
			ClientException {
		return this.sendSync(packet, 0);
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
	 */
	protected ResponsePacket getResponsePacket(CommandPacket commandPacket,
			long timeout) throws IOException {
		ResponsePacket response = this.sendSyncInternal(commandPacket, timeout,
				this.numRetriesUponTimeout);
		return response != null ? (response) : new ResponsePacket(
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
	 */
	protected ResponsePacket getCommandValueReturnPacket(CommandPacket packet)
			throws IOException {
		return this.getResponsePacket(packet, 0);
	}

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

		static final Set<IntegerPacketType> clientPacketTypes = new HashSet<>(
				Arrays.asList(Packet.PacketType.COMMAND_RETURN_VALUE));

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
			super(reconfigurators, sslMode, clientPortOffset);
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
				LOG.log(Level.WARNING, "Problem parsing packet from {0}: {1}",
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

		/**
		 *
		 * @return a set of packet types
		 */
		@Override
		public Set<IntegerPacketType> getRequestTypes() {
			return clientPacketTypes;
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

	/* **************** Start of execute methods ****************** */
	/**
	 * Execute the command immediately. The result of the execution may be
	 * retrieved using {@link CommandPacket#getResult()} or
	 * {@link CommandPacket#getResultString()} or other methods with the prefix
	 * "getResult" depending on the {@link CommandResultType}.
	 *
	 * @param command
	 * @return GNSCommand after execution containing the result if any.
	 * @throws IOException
	 * @throws ClientException
	 */
	public CommandPacket execute(CommandPacket command) throws IOException,
			ClientException {
		return (CommandPacket) this.sendSync(command);
	}

	/**
	 * Execute the command immediately. The result of the execution may be
	 * retrieved using {@link CommandPacket#getResult()} or
	 * {@link CommandPacket#getResultString()} or other methods with the prefix
	 * "getResult" depending on the {@link CommandResultType}.
	 * 
	 * @param command
	 * @param timeout
	 * @return GNSCommand after execution containing the result if any.
	 * @throws IOException
	 * @throws ClientException
	 */
	public CommandPacket execute(CommandPacket command, long timeout)
			throws IOException, ClientException {
		return (CommandPacket) this.sendSync(command, timeout);
	}

	/**
	 * Execute the command asynchronously. The result of the execution may be
	 * retrieved using {@link RequestFuture#get()} on the returned future and
	 * then invoking a suitable "getResult" method as in
	 * {@link #execute(CommandPacket)} on the returned {@link CommandPacket}.
	 *
	 * @param commandPacket
	 * @return A future to retrieve the result of executing {@code command}
	 *         using {@link RequestFuture#get()}.
	 * @throws IOException
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
	 * @param callback
	 * @return A future to retrieve the result of executing {@code command}
	 *         using {@link RequestFuture#get()}.
	 * @throws IOException
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
