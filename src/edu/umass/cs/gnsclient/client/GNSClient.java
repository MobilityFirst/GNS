
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.gnsclient.client.util.GUIDUtilsGNSClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.PacketUtils;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
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
import org.json.JSONException;
import org.json.JSONObject;

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


public class GNSClient {


	private static final InetSocketAddress DEFAULT_LOCAL_RECONFIGURATOR = new InetSocketAddress(
			InetAddress.getLoopbackAddress(),
			GNSConfig.DEFAULT_RECONFIGURATOR_PORT);

	protected AsyncClient asyncClient;
	// local name server
	private InetSocketAddress GNSProxy = null;

	protected String getLabel() {
		return GNSClient.class.getSimpleName();
	}


	public GNSClient() throws IOException {
		this(getStaticReconfigurators());
	}
	

	public GNSClient(InetSocketAddress anyReconfigurator) throws IOException {
		this(anyReconfigurator != null ? new HashSet<InetSocketAddress>(
				Arrays.asList(anyReconfigurator)) : getStaticReconfigurators());
	}
	

	public GNSClient(String anyReconfiguratorHostName) throws IOException {
		this(new InetSocketAddress(anyReconfiguratorHostName,
				GNSConfig.DEFAULT_RECONFIGURATOR_PORT));
	}
	
	// non-public constructors below
	
	protected GNSClient(boolean checkConnectivity) throws IOException {
		this(getStaticReconfigurators(), checkConnectivity);
	}


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


	public void checkConnectivity() throws IOException {
		this.asyncClient.checkConnectivity();
	}


	protected boolean isForceCoordinatedReads() {
		return this.forceCoordinatedReads;
	}

	private boolean forceCoordinatedReads = false;


	public GNSClient setForceCoordinatedReads(boolean forceCoordinatedReads) {
		this.forceCoordinatedReads = forceCoordinatedReads;
		return this;
	}


	public void close() {
		this.asyncClient.close();
	}


	private RequestFuture<CommandPacket> sendAsync(CommandPacket packet,
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
				if (ce.getCode() == ResponseCode.TIMEOUT) {

				}

			}
		} while ((count++ < this.numRetriesUponTimeout && (response == null || response
				.getErrorCode() == ResponseCode.TIMEOUT)));
		return (response);
	}


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


	}

	private int numRetriesUponTimeout = 0;


	public GNSClient setNumRetriesUponTimeout(int retries) {
		this.numRetriesUponTimeout = retries;
		return this;
	}

	private CommandPacket sendSync(CommandPacket packet, final long timeout)
			throws IOException, ClientException {
		return this.sendSync(packet, timeout, numRetriesUponTimeout);
	}


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


	protected ResponsePacket getCommandValueReturnPacket(CommandPacket packet)
			throws IOException, ClientException {
		return this.getResponsePacket(packet, 0);
	}

	private static final Set<IntegerPacketType> CLIENT_PACKET_TYPES = new HashSet<>(
			Arrays.asList(Packet.PacketType.COMMAND_RETURN_VALUE));


	public static class AsyncClient extends
			ReconfigurableAppClientAsync<CommandPacket> implements
			AppRequestParserBytes {

		private static Stringifiable<String> unstringer = new StringifiableDefault<>(
				"");


		public AsyncClient(Set<InetSocketAddress> reconfigurators,
				SSL_MODES sslMode, int clientPortOffset,
				boolean checkConnectivity) throws IOException {
			super(reconfigurators, sslMode, clientPortOffset, checkConnectivity);
			this.enableJSONPackets();
		}


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


		@Override
		public Set<IntegerPacketType> getRequestTypes() {
			return CLIENT_PACKET_TYPES;
		}


		@Override
		public Set<IntegerPacketType> getMutualAuthRequestTypes() {
			Set<IntegerPacketType> types = new HashSet<IntegerPacketType>(
					Arrays.asList(Packet.PacketType.ADMIN_COMMAND));
			if (InternalCommandPacket.SEPARATE_INTERNAL_TYPE) {
				types.add(Packet.PacketType.INTERNAL_COMMAND);
			}
			return types;
		}


		@Override
		public Request getRequest(byte[] bytes, NIOHeader header)
				throws RequestParseException {
			return SharedGuidUtils.getRequestStatic(bytes, header, unstringer);
		}
	} // End of AsyncClient


	public InetSocketAddress getGNSProxy() {
		return this.GNSProxy;
	}


	public void setGNSProxy(InetSocketAddress LNS) {
		this.GNSProxy = LNS;
	}

	private long forcedTimeout = 0;


	public GNSClient setForcedTimeout(long t) {
		this.forcedTimeout = t;
		return this;
	}



	public CommandPacket execute(CommandPacket command) throws IOException,
			ClientException {
		return this.sendSync(command);
	}


	public CommandPacket execute(CommandPacket command, long timeout)
			throws IOException, ClientException {
		return this.sendSync(command, timeout);
	}


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


	public static void main(String[] args) throws Exception {
		GNSClient client = new GNSClient();
		GuidEntry guid;
		guid = GUIDUtilsGNSClient.lookupOrCreateAccountGuid(client, "test",
				"password", true);
		client.execute(GNSCommand.update(guid, new JSONObject()));
	}
}
