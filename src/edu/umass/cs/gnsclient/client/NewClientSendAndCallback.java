package edu.umass.cs.gnsclient.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.GCConcurrentHashMapCallback;

/**
 * @author Westy
 *
 */
public class NewClientSendAndCallback {

  private static final long MIN_RTX_INTERVAL = 1000;
  private static final long GC_TIMEOUT = 60000;

  private final MessageNIOTransport<String, String> niot;
  private final InetSocketAddress[] reconfigurators;
  private Set<IntegerPacketType> clientPacketTypes;

  // Enables all the debug logging statements in the client.
  protected boolean debuggingEnabled = true;

  private final GCConcurrentHashMapCallback defaultGCCallback = new GCConcurrentHashMapCallback() {
    @Override
    public void callbackGC(Object key, Object value) {
      Reconfigurator.getLogger().info(
              this + " garbage-collecting " + key + ":" + value);
    }
  };

  private final GCConcurrentHashMap<Long, RequestCallback> callbacks = new GCConcurrentHashMap<Long, RequestCallback>(
          defaultGCCallback, GC_TIMEOUT);

  private final GCConcurrentHashMap<String, RequestCallback> callbacksCRP = new GCConcurrentHashMap<String, RequestCallback>(
          defaultGCCallback, GC_TIMEOUT);

  // name->actives map
  private final GCConcurrentHashMap<String, InetSocketAddress[]> activeReplicas = new GCConcurrentHashMap<String, InetSocketAddress[]>(
          defaultGCCallback, GC_TIMEOUT);
  // name->unsent app requests for which active replicas are not yet known
  private final GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>> requestsPendingActives = new GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>>(
          defaultGCCallback, GC_TIMEOUT);
  // name->last queried time to rate limit RequestActiveReplicas queries
  private final GCConcurrentHashMap<String, Long> lastQueriedActives = new GCConcurrentHashMap<String, Long>(
          defaultGCCallback, GC_TIMEOUT);

  /**
   * The constructor specifies the default set of reconfigurators. This set
   * may change over time, so it is the caller's responsibility to ensure that
   * this set remains up-to-date. Some staleness however can be tolerated as
   * reconfigurators will by design forward a request to the responsible
   * reconfigurator if they are not responsible.
   *
   *
   * @param types
   * @param reconfigurators
   * @throws IOException
   */
  public NewClientSendAndCallback(Set<IntegerPacketType> types, Set<InetSocketAddress> reconfigurators) throws IOException {
    this.niot = (new MessageNIOTransport<>(null, null,
            (new ClientPacketDemultiplexer(types)), true));
    this.reconfigurators = reconfigurators.toArray(new InetSocketAddress[0]);
    this.clientPacketTypes = types;
  }

  /**
   * @param types
   * @throws IOException
   */
  public NewClientSendAndCallback(Set<IntegerPacketType> types) throws IOException {
    this(types, ReconfigurationConfig.getReconfiguratorAddresses());
  }

  private static Stringifiable<String> unstringer = new StringifiableDefault<>("");

  class ClientPacketDemultiplexer extends AbstractPacketDemultiplexer<String> {

    ClientPacketDemultiplexer(Set<IntegerPacketType> types) {
      register(ReconfigurationPacket.clientPacketTypes);
      register(types);
    }

    private ClientReconfigurationPacket parseAsClientReconfigurationPacket(
            String strMsg) {
      ReconfigurationPacket<?> rcPacket = null;
      if (debuggingEnabled) {
        GNSClient.getLogger().info("Parse as recon packet: " + strMsg);
      }
      try {
        rcPacket = ReconfigurationPacket.getReconfigurationPacket(
                new JSONObject(strMsg), unstringer);
      } catch (JSONException e) {
        e.printStackTrace();
      }
      ClientReconfigurationPacket result = (rcPacket instanceof ClientReconfigurationPacket) ? (ClientReconfigurationPacket) rcPacket
              : null;
      if (debuggingEnabled) {
        GNSClient.getLogger().info("Parse as recon packet returns: " + result);
      }
      return result;
    }

    private Request parseAsAppRequest(String strMsg) {
      Request request = null;
      if (debuggingEnabled) {
        GNSClient.getLogger().info("Parse as app request: " + strMsg);
      }
      try {
        JSONObject json = new JSONObject(strMsg);
        if (clientPacketTypes.contains(Packet.getPacketType(json))) {
          request = (Request) Packet.createInstance(json, unstringer);
        }
        //request = app.getRequest(strMsg);
      } catch (JSONException e) {
        e.printStackTrace();

      }
      assert (request == null || request instanceof ClientRequest);
      if (debuggingEnabled) {
        GNSClient.getLogger().info("Parse as app request returns: " + request);
      }
      return request;
    }

    @Override
    public boolean handleMessage(String strMsg) {
      if (debuggingEnabled) {
        GNSClient.getLogger().info("Handle message: " + strMsg);
      }
      Request response = null;
      // first try parsing as app request
      if ((response = parseAsAppRequest(strMsg)) == null) // else try parsing as ClientReconfigurationPacket
      {
        response = parseAsClientReconfigurationPacket(strMsg);
      }

      assert (response != null);

      if (debuggingEnabled) {
        GNSClient.getLogger().info("Handle message response: " + response);
      }
      RequestCallback callback = null;
      if (response != null) {
        // execute registered callback
        if ((response instanceof ClientRequest)
                && (callback = callbacks.remove(((ClientRequest) response).getRequestID())) != null) {
          if (debuggingEnabled) {
            GNSClient.getLogger().info("Handle message client request call back");
          }
          callback.handleResponse(((ClientRequest) response));
        } // ActiveReplicaError has to be dealt with separately
        else if ((response instanceof ActiveReplicaError)
                && (callback = callbacks.remove(((ActiveReplicaError) response).getRequestID())) != null) {
          if (debuggingEnabled) {
            GNSClient.getLogger().info("Handle message ActiveReplicaError ");
          }
        } else if (response instanceof ClientReconfigurationPacket) {
          if ((callback = callbacksCRP.remove(getKey((ClientReconfigurationPacket) response))) != null) {
            if (debuggingEnabled) {
              GNSClient.getLogger().info("Handle message recon packet callback");
            }
            callback.handleResponse(response);
          }
          // if RequestActiveReplicas, send pending requests
          if (response instanceof RequestActiveReplicas) {
            try {
              if (debuggingEnabled) {
                GNSClient.getLogger().info("Handle message send pending requests");
              }
              sendRequestsPendingActives((RequestActiveReplicas) response);
            } catch (IOException e) {
              e.printStackTrace();
            }
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
      if (SHORT_CUT_TYPE_CHECK) {
        return ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME
                .getInt();
      }
      Request request = parseAsAppRequest(strMsg);
      if (request == null) {
        request = parseAsClientReconfigurationPacket(strMsg);
      }
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
    if (debuggingEnabled) {
      GNSClient.getLogger().info("Send client request: " + request.toString());
    }
    try {
      callbacks.putIfAbsent(request.getRequestID(), callback);
      if (callbacks.get(request.getRequestID()) == callback) {
        sent = niot.sendToAddress(server, request.toString());
      }
    } finally {
      if (sent <= 0) {
        callbacks.remove(request.getRequestID(), callback);
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
    callbacksCRP.put(getKey(request), callback);
    sendRequest(request);
  }

  /**
   * @param type
   * @param name
   * @param initialState
   * Used only if type is
   * {@link edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType#CREATE_SERVICE_NAME}
   * .
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
    sendRequest(request, callback);
  }

  private void sendRequest(ClientReconfigurationPacket request)
          throws IOException {
    InetSocketAddress reconfigurator = getRandom(reconfigurators);
    if (debuggingEnabled) {
      GNSClient.getLogger().info("Send to " + reconfigurator + " request: " + request.toString());
    }
    niot.sendToAddress(reconfigurator, request.toString());
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
  public Long sendRequest(ClientRequest request, RequestCallback callback) throws IOException, JSONException {
    if (request instanceof ClientReconfigurationPacket) {
      return sendRequest(request,
              reconfigurators[(int) (Math.random() * reconfigurators.length)],
              callback);
    }

    // lookup actives in the cache first
    if (activeReplicas.containsKey(request.getServiceName())) {
      InetSocketAddress[] actives = activeReplicas.get(request
              .getServiceName());
      if (debuggingEnabled) {
        GNSClient.getLogger().info("Found actives: " + actives);
      }
      return sendRequest(request,
              actives[(int) (Math.random() * actives.length)], callback);
    }

    // else enqueue them
    enqueue(new RequestAndCallback(request, callback));
    queryForActives(request.getServiceName());
    if (debuggingEnabled) {
      GNSClient.getLogger().info("Enqueue: " + request.getRequestID());
    }
    return request.getRequestID();
  }

  private synchronized boolean enqueue(RequestAndCallback rc) {
    requestsPendingActives.putIfAbsent(rc.request.getServiceName(),
            new LinkedBlockingQueue<RequestAndCallback>());
    LinkedBlockingQueue<RequestAndCallback> pending = requestsPendingActives
            .get(rc.request.getServiceName());
    assert (pending != null);
    return pending.add(rc);
  }

  private void queryForActives(String name) throws IOException {
    if (debuggingEnabled) {
      GNSClient.getLogger().info("Query for actives: " + name);
    }
    Long lastQueriedTime = lastQueriedActives.get(name);
    if (lastQueriedTime == null) {
      lastQueriedTime = 0L;
    }
    if (System.currentTimeMillis() - lastQueriedTime > MIN_RTX_INTERVAL) {
      sendRequest(new RequestActiveReplicas(name));
    }
  }

  private void sendRequestsPendingActives(RequestActiveReplicas response)
          throws IOException {
    if (response.isFailed()) {
      return;
    }
    InetSocketAddress[] actives = response.getActives().toArray(
            new InetSocketAddress[0]);
    if (actives == null || actives.length == 0) {
      return;
    }
    activeReplicas.put(response.getServiceName(), actives);
    if (requestsPendingActives.containsKey(response.getServiceName())) {
      for (Iterator<RequestAndCallback> reqIter = requestsPendingActives
              .get(response.getServiceName()).iterator(); reqIter
              .hasNext();) {
        RequestAndCallback rc = reqIter.next();
        sendRequest(rc.request,
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
            Arrays.asList(reconfigurators));
  }

  public void stop() {
    niot.stop();
  }
}
