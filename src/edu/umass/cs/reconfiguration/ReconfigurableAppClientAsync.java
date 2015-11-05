package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Application;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.GCConcurrentHashMapCallback;

/**
 * @author arun
 *
 */
public class ReconfigurableAppClientAsync {
	private static final long MIN_RTX_INTERVAL = 1000;
	private static final long GC_TIMEOUT = 60000;

	final Application app;
	final MessageNIOTransport<String, String> niot;
	final InetSocketAddress[] reconfigurators;

	final GCConcurrentHashMapCallback defaultGCCallback = new GCConcurrentHashMapCallback() {
		@Override
		public void callbackGC(Object key, Object value) {
			Reconfigurator.getLogger().info(
					this + " garbage-collecting " + key + ":" + value);
		}
	};

	final GCConcurrentHashMap<Long, RequestCallback> callbacks = new GCConcurrentHashMap<Long, RequestCallback>(
			defaultGCCallback, GC_TIMEOUT);

	final GCConcurrentHashMap<String, RequestCallback> callbacksCRP = new GCConcurrentHashMap<String, RequestCallback>(
			defaultGCCallback, GC_TIMEOUT);

	// name->actives map
	final GCConcurrentHashMap<String, InetSocketAddress[]> activeReplicas = new GCConcurrentHashMap<String, InetSocketAddress[]>(
			defaultGCCallback, GC_TIMEOUT);
	// name->unsent app requests for which active replicas are not yet known
	final GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>> requestsPendingActives = new GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>>(
			defaultGCCallback, GC_TIMEOUT);
	// name->last queried time to rate limit RequestActiveReplicas queries
	final GCConcurrentHashMap<String, Long> lastQueriedActives = new GCConcurrentHashMap<String, Long>(
			defaultGCCallback, GC_TIMEOUT);

	/**
	 * @throws IOException
	 * 
	 */
	public ReconfigurableAppClientAsync() throws IOException {
		this(ReconfigurationConfig.createApp());
	}

	/**
	 * The constructor specifies the default set of reconfigurators. This set
	 * may change over time, so it is the caller's responsibility to ensure that
	 * this set remains up-to-date. Some staleness however can be tolerated as
	 * reconfigurators will by design forward a request to the responsible
	 * reconfigurator if they are not responsible.
	 * 
	 * @param app
	 * 
	 * @param reconfigurators
	 * @throws IOException
	 */
	public ReconfigurableAppClientAsync(Application app,
			Set<InetSocketAddress> reconfigurators) throws IOException {
		this.app = app;
		this.niot = (new MessageNIOTransport<String, String>(null, null,
				(new ClientPacketDemultiplexer(app.getRequestTypes())), true));
		this.reconfigurators = reconfigurators
				.toArray(new InetSocketAddress[0]);
	}

	/**
	 * @param app
	 * @throws IOException
	 */
	public ReconfigurableAppClientAsync(Application app)
			throws IOException {
		this(app, ReconfigurationConfig.getReconfiguratorAddresses());
	}

	private static Stringifiable<?> unstringer = new StringifiableDefault<String>(
			"");

	class ClientPacketDemultiplexer extends AbstractPacketDemultiplexer<String> {

		ClientPacketDemultiplexer(Set<IntegerPacketType> types) {
			this.register(ReconfigurationPacket.clientPacketTypes);
			this.register(types);
		}

		private ClientReconfigurationPacket parseAsClientReconfigurationPacket(
				String strMsg) {
			ReconfigurationPacket<?> rcPacket = null;
			try {
				rcPacket = ReconfigurationPacket.getReconfigurationPacket(
						new JSONObject(strMsg), unstringer);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return (rcPacket instanceof ClientReconfigurationPacket) ? (ClientReconfigurationPacket) rcPacket
					: null;
		}

		private Request parseAsAppRequest(String strMsg) {
			Request request = null;
			try {
				request = app.getRequest(strMsg);
			} catch (RequestParseException e) {
				// e.printStackTrace();
			}
			assert (request == null || request instanceof ClientRequest);
			return request;
		}

		@Override
		public boolean handleMessage(String strMsg) {
			Request response = null;
			// first try parsing as app request
			if ((response = this.parseAsAppRequest(strMsg)) == null)
				// else try parsing as ClientReconfigurationPacket
				response = parseAsClientReconfigurationPacket(strMsg);

			assert (response != null);

			RequestCallback callback = null;
			if (response != null) {
				// execute registered callback
				if ((response instanceof ClientRequest)
						&& (callback = callbacks
								.remove(((ClientRequest) response)
										.getRequestID())) != null)
					callback.handleResponse(((ClientRequest) response));
				// ActiveReplicaError has to be dealt with separately
				else if ((response instanceof ActiveReplicaError)
						&& (callback = callbacks
								.remove(((ActiveReplicaError) response)
										.getRequestID())) != null) {
				} else if (response instanceof ClientReconfigurationPacket) {
					if ((callback = ReconfigurableAppClientAsync.this.callbacksCRP
							.remove(getKey((ClientReconfigurationPacket) response))) != null) {
						callback.handleResponse(response);
					}
					// if RequestActiveReplicas, send pending requests
					if (response instanceof RequestActiveReplicas)
						try {
							ReconfigurableAppClientAsync.this
									.sendRequestsPendingActives((RequestActiveReplicas) response);
						} catch (IOException e) {
							e.printStackTrace();
						}
				}
			}
			return true;
		}

		private static final boolean SHORT_CUT_TYPE_CHECK = true;

		/**
		 * We can simply return a constant integer corresponding to either a
		 * ClientReconfigurationPacket or app request here as this method is
		 * only used to confirm the demultiplexer, which is needed only in the
		 * case of multiple chained demultiplexers, but we only have one
		 * demultiplexer in this simple client.
		 * 
		 * @param strMsg
		 * @return
		 */
		@Override
		protected Integer getPacketType(String strMsg) {
			if (SHORT_CUT_TYPE_CHECK)
				return ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME
						.getInt();
			Request request = this.parseAsAppRequest(strMsg);
			if (request == null)
				request = this.parseAsClientReconfigurationPacket(strMsg);
			return request != null ? request.getRequestType().getInt() : null;
		}

		@Override
		protected String getMessage(String message) {
			return message;
		}

		@Override
		protected String processHeader(String message, NIOHeader header) {
			return message;
		}

		@Override
		protected boolean matchesType(Object message) {
			return message instanceof String;
		}
	}

	/**
	 * @param request
	 * @param server
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 */
	public Long sendRequest(ClientRequest request,
			InetSocketAddress server, RequestCallback callback)
			throws IOException {
		int sent = -1;
		assert (request.getServiceName() != null);
		try {
			this.callbacks.putIfAbsent(request.getRequestID(), callback);
			if (this.callbacks.get(request.getRequestID()) == callback)
				sent = this.niot.sendToAddress(server, request.toString());
		} finally {
			if (sent <= 0) {
				this.callbacks.remove(request.getRequestID(), callback);
				return null;
			}
		}
		return request.getRequestID();
	}

	/**
	 * @param request
	 * @param callback
	 * @throws IOException
	 */
	public void sendRequest(ClientReconfigurationPacket request,
			RequestCallback callback) throws IOException {
		assert (request.getServiceName() != null);
		// overwrite the most recent callback
		this.callbacksCRP.put(getKey(request), callback);
		this.sendRequest(request);
	}

	/**
	 * @param type
	 * @param name
	 * @param initialState
	 *            Used only if type is
	 *            {@link edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType#CREATE_SERVICE_NAME}
	 *            .
	 * @param callback
	 * @throws IOException
	 */
	public void sendReconfigurationRequest(
			ReconfigurationPacket.PacketType type, String name,
			String initialState, RequestCallback callback) throws IOException {
		ClientReconfigurationPacket request = null;
		switch (type) {
		case CREATE_SERVICE_NAME:
			request = new CreateServiceName(name, initialState);
			break;
		case DELETE_SERVICE_NAME:
			request = new DeleteServiceName(name);
			break;
		case REQUEST_ACTIVE_REPLICAS:
			request = new RequestActiveReplicas(name);
			break;
		default:
			break;
		}
		this.sendRequest(request, callback);
	}

	private void sendRequest(ClientReconfigurationPacket request)
			throws IOException {
		this.niot.sendToAddress(getRandom(this.reconfigurators),
				request.toString());
	}

	private InetSocketAddress getRandom(InetSocketAddress[] isas) {
		return isas != null && isas.length > 0 ? isas[(int) (Math.random() * isas.length)]
				: null;
	}

	private String getKey(ClientReconfigurationPacket crp) {
		return crp.getRequestType() + ":" + crp.getServiceName();
	}

	class RequestAndCallback {
		final ClientRequest request;
		final RequestCallback callback;

		RequestAndCallback(ClientRequest request,
				RequestCallback callback) {
			this.request = request;
			this.callback = callback;
		}
	}

	/**
	 * @param request
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 * @throws JSONException
	 */
	public Long sendRequest(ClientRequest request,
			RequestCallback callback) throws IOException, JSONException {
		if (request instanceof ClientReconfigurationPacket)
			return this
					.sendRequest(
							request,
							reconfigurators[(int) (Math.random() * this.reconfigurators.length)],
							callback);

		// lookup actives in the cache first
		if (this.activeReplicas.containsKey(request.getServiceName())) {
			InetSocketAddress[] actives = this.activeReplicas.get(request
					.getServiceName());
			return this.sendRequest(request,
					actives[(int) (Math.random() * actives.length)], callback);
		}
		// else enqueue them
		this.enqueue(new RequestAndCallback(request, callback));
		this.queryForActives(request.getServiceName());
		return request.getRequestID();
	}

	private synchronized boolean enqueue(RequestAndCallback rc) {
		this.requestsPendingActives.putIfAbsent(rc.request.getServiceName(),
				new LinkedBlockingQueue<RequestAndCallback>());
		LinkedBlockingQueue<RequestAndCallback> pending = this.requestsPendingActives
				.get(rc.request.getServiceName());
		assert (pending != null);
		return pending.add(rc);
	}

	private void queryForActives(String name) throws IOException {
		Long lastQueriedTime = this.lastQueriedActives.get(name);
		if (lastQueriedTime == null)
			lastQueriedTime = 0L;
		if (System.currentTimeMillis() - lastQueriedTime > MIN_RTX_INTERVAL) {
			this.sendRequest(new RequestActiveReplicas(name));
		}
	}

	private void sendRequestsPendingActives(RequestActiveReplicas response)
			throws IOException {
		if (response.isFailed())
			return;
		InetSocketAddress[] actives = response.getActives().toArray(
				new InetSocketAddress[0]);
		if (actives == null || actives.length == 0)
			return;
		this.activeReplicas.put(response.getServiceName(), actives);
		if (this.requestsPendingActives.containsKey(response.getServiceName())) {
			for (Iterator<RequestAndCallback> reqIter = this.requestsPendingActives
					.get(response.getServiceName()).iterator(); reqIter
					.hasNext();) {
				RequestAndCallback rc = reqIter.next();
				this.sendRequest(rc.request,
						actives[((int) rc.request.getRequestID())
								% actives.length], rc.callback);
				reqIter.remove();
			}
		}
	}

	/**
	 * @return The list of default servers.
	 */
	public Set<InetSocketAddress> getDefaultServers() {
		return new HashSet<InetSocketAddress>(
				Arrays.asList(this.reconfigurators));
	}
}
