package edu.umass.cs.gnsclient.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.localnameserver.LNSRequestInfo;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.GCConcurrentHashMapCallback;

/**
 * @author arun
 * 
 *         Cleaner implementation of a GNS client using gigapaxos' async client.
 */
public class GNSClient extends UniversalTcpClientExtended {

	private static final long CONNECTIVITY_CHECK_TIMEOUT = 4000;
	private static final int CONNECTIVITY_CHECK_ATTEMPTS = 4;

	// initialized from properties file
	private static final Set<InetSocketAddress> staticReconfigurators = ReconfigurationConfig
			.getReconfiguratorAddresses();

	// initialized upon contsruction
	private final Set<InetSocketAddress> reconfigurators;
	private final AsyncClient asyncClient;

	/**
	 * @throws IOException
	 * 
	 */
	public GNSClient() throws IOException {
		this(false);
	}

	/**
	 * @param useSSL
	 * @throws IOException
	 */
	public GNSClient(boolean useSSL) throws IOException {
		this(
				staticReconfigurators != null
						&& !staticReconfigurators.isEmpty() ? staticReconfigurators
						.iterator().next() : null, !useSSL);
	}

	/**
	 * Bootstrap with a single, arbitrarily chosen valid reconfigurator address.
	 * The client can enquire and know of other reconfigurator addresses from
	 * this reconfigurator. If it is unable to do so, it will throw an
	 * IOException.
	 * 
	 * @param anyReconfigurator
	 * @param localNameServer
	 * @param useSSL
	 * @throws IOException
	 */
	public GNSClient(InetSocketAddress anyReconfigurator,
			InetSocketAddress localNameServer, boolean useSSL)
			throws IOException {
		super(anyReconfigurator, localNameServer != null ? localNameServer
				.getAddress().toString() : null,
				localNameServer != null ? localNameServer.getPort() : -1,
				!useSSL);
		this.reconfigurators = this.knowOtherReconfigurators(anyReconfigurator);
		if (this.reconfigurators == null || this.reconfigurators.isEmpty())
			throw new IOException(
					"Unable to find any reconfigurator addresses; "
							+ "at least one needed to initialize client");
		this.asyncClient = new AsyncClient(reconfigurators,
				useSSL ? ReconfigurationConfig.getClientSSLMode()
						: SSL_MODES.CLEAR,
				useSSL ? ReconfigurationConfig.getClientPortSSLOffset()
						: ReconfigurationConfig.getClientPortClearOffset());
		if (!checkConnectivity(CONNECTIVITY_CHECK_TIMEOUT,
				CONNECTIVITY_CHECK_ATTEMPTS))
			throw new IOException(CONNECTION_CHECK_ERROR);
	}

	/**
	 * Bootstrap with a single, arbitrarily chosen valid reconfigurator address.
	 * The client can enquire and know of other reconfigurator addresses from
	 * this reconfigurator. If it is unable to do so, it will throw an
	 * IOException.
	 * 
	 * @param anyReconfigurator
	 * @param useSSL
	 * @throws IOException
	 */
	public GNSClient(InetSocketAddress anyReconfigurator, boolean useSSL)
			throws IOException {
		this(anyReconfigurator, null, !useSSL);
	}

	private static final String CONNECTION_CHECK_ERROR = "Unable to establish connection with any reconfigurator";

	/**
	 * TODO: implement request/response to know of other reconfigurators. It is
	 * also okay to just use a single reconfigurator address if it is an anycast
	 * address (with the TCP error caveat under route changes).
	 */
	private Set<InetSocketAddress> knowOtherReconfigurators(
			InetSocketAddress anyReconfigurator) throws IOException {
		return anyReconfigurator != null ? new HashSet<InetSocketAddress>(
				Arrays.asList(anyReconfigurator)) : staticReconfigurators;
	}

	public String toString() {
		return this.asyncClient.toString();
	}

	private long genRandID() {
		return (long) (Math.random() * Long.MAX_VALUE);
	}

	/**
	 * Overrides older implementation of
	 * {@link #sendCommandPacket(CommandPacket)} with simpler async
	 * implementation.
	 * 
	 * @param packet
	 * @throws IOException
	 */
	@Override
	protected void sendCommandPacket(CommandPacket packet) throws IOException {
		RequestCallback callback = new RequestCallback() {
			@Override
			public void handleResponse(Request response) {
				try {
					GNSClient.this.handleCommandValueReturnPacket(response,
							System.currentTimeMillis());
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
		};
		if (GnsProtocol.CREATE_DELETE_COMMANDS
				.contains(packet.getCommandName())
				|| packet.getCommandName().equals(GnsProtocol.SELECT))
			this.asyncClient.sendRequestAnycast(packet, callback);
		else
			this.asyncClient.sendRequest(packet, callback);
	}

	// blocking connectivity check
	private boolean checkConnectivity(long timeout) {
		Object monitor = new Object();
		boolean[] success = new boolean[1];
		long id;
		try {
			this.asyncClient.sendRequest(new CommandPacket(id = genRandID(),
					null, -1, createCommand(GnsProtocol.CONNECTION_CHECK)),
					new RequestCallback() {
						@Override
						public void handleResponse(Request response) {
							// any news is good news
							success[0] = true;
							synchronized (monitor) {
								monitor.notify();
							}
						}
					});
			GNSConfig.getLogger().info(this + " sent connectivity check " + id);
			if (!success[0])
				synchronized (monitor) {
					monitor.wait(timeout);
				}
		} catch (InterruptedException | IOException | GnsClientException e) {
			return false;
		}
		if (success[0])
			GNSConfig.getLogger().info(
					this + " connectivity check " + id + " successful");
		return success[0];
	}

	private boolean checkConnectivity(long attemptTimeout, int numAttempts) {
		int attempts = 0;
		while (attempts++ < numAttempts)
			if (this.checkConnectivity(attemptTimeout))
				return true;
			else {
				System.out
						.print((attempts == 1 ? "Retrying connection check..."
								: "") + attempts + " ");
				this.checkConnectivity(attemptTimeout);
			}
		return false;
	}

	/**
	 * @throws IOException
	 */
	@Override
	public void checkConnectivity() throws IOException {
		if (!this.checkConnectivity(CONNECTIVITY_CHECK_TIMEOUT, 1))
			throw new IOException(CONNECTION_CHECK_ERROR);
	}

	/**
	 * Closes the underlying async client.
	 */
	@Override
	public void close() {
		this.asyncClient.close();
	}

	/**
	 * Sets an optional local name server address.
	 * 
	 * @param lnsAddr
	 */
	public void setLNSAddress(InetSocketAddress lnsAddr) {
		this.localNameServerAddress = lnsAddr;
	}

	/**
	 * @param packet
	 * @param callback
	 * @throws JSONException
	 * @throws IOException
	 */
	public void sendAsync(CommandPacket packet, RequestCallback callback)
			throws JSONException, IOException {
		if (packet.getServiceName().equals(
				Config.getGlobalString(RC.SPECIAL_NAME))
				|| packet.getCommandName().equals(GnsProtocol.SELECT))
			this.asyncClient.sendRequestAnycast(packet, callback);
		else
			this.asyncClient.sendRequest(packet, callback);
	}

	/**
	 * @param packet
	 * @param timeout
	 * @return Response from the server or null if the timeout expires.
	 * @throws IOException
	 */
	public CommandValueReturnPacket sendSync(CommandPacket packet, Long timeout)
			throws IOException {
		Object monitor = new Object();
		CommandValueReturnPacket[] retval = new CommandValueReturnPacket[1];

		// send
		this.asyncClient.sendRequest(packet, new RequestCallback() {

			@Override
			public void handleResponse(Request response) {
				if (response instanceof CommandValueReturnPacket)
					retval[0] = (CommandValueReturnPacket) response;
				synchronized (monitor) {
					monitor.notify();
				}
			}
		});

		// wait for timeout
		if (retval[0] == null)
			try {
				synchronized (monitor) {
					if (timeout != null)
						monitor.wait(timeout);
					else
						monitor.wait();
				}
			} catch (InterruptedException e) {
				throw new IOException(
						"sendSync interrupted while waiting for a response for "
								+ packet.getSummary());
			}
		return retval[0];
	}

	/**
	 * @param packet
	 * @return Same as {@link #sendSync(CommandPacket, Long)} but with an
	 *         infinite timeout.
	 * 
	 * @throws IOException
	 */
	public CommandValueReturnPacket sendSync(CommandPacket packet)
			throws IOException {
		return this.sendSync(packet, null);
	}

	static class AsyncClient extends ReconfigurableAppClientAsync {
		private static Stringifiable<String> unstringer = new StringifiableDefault<String>(
				"");

		static final Set<IntegerPacketType> clientPacketTypes = new HashSet<IntegerPacketType>(
				Arrays.asList(Packet.PacketType.COMMAND_RETURN_VALUE));

		public AsyncClient(Set<InetSocketAddress> reconfigurators,
				SSL_MODES sslMode, int clientPortOffset) throws IOException {
			super(reconfigurators, sslMode, clientPortOffset);
		}

		@Override
		public Request getRequest(String msg) throws RequestParseException {
			Request response = null;
			JSONObject json = null;
			try {
				json = new JSONObject(msg);
				Packet.PacketType type = Packet.getPacketType(json);
				if (type != null) {
					GNSConfig.getLogger().log(Level.INFO,
							"{0} retrieving packet from received json {1}",
							new Object[] { this, json });
					if (clientPacketTypes.contains(Packet.getPacketType(json)))
						response = (Request) Packet.createInstance(json,
								unstringer);
					assert (response == null || response.getRequestType() == Packet.PacketType.COMMAND_RETURN_VALUE);
				}
			} catch (JSONException e) {
				GNSConfig.getLogger().warning(
						"Problem parsing packet from " + json + ": " + e);
			}
			return response;
		}

		@Override
		public Set<IntegerPacketType> getRequestTypes() {
			return clientPacketTypes;
		}
	}
}
