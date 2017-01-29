
package edu.umass.cs.gnsserver.localnameserver;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnscommon.utils.NetworkUtils;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.localnameserver.nodeconfig.LNSConsistentReconfigurableNodeConfig;
import edu.umass.cs.gnsserver.localnameserver.nodeconfig.LNSNodeConfig;
import edu.umass.cs.gnsserver.utils.Shutdownable;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;


public class LocalNameServer implements RequestHandlerInterface, Shutdownable {


  public static final int DEFAULT_VALUE_CACHE_TTL = 10000; // milleseconds


  public static final Logger LOGGER = Logger.getLogger(LocalNameServer.class.getName());

  private static final ConcurrentMap<Long, LNSRequestInfo> outstandingRequests
          = new ConcurrentHashMap<>(10, 0.75f, 3);

  private final Cache<String, CacheEntry> cache;
  private JSONMessenger<InetSocketAddress> messenger;
  // FIXME: Eventually need separate servers for ssl and clear
  //private JSONMessenger<InetSocketAddress> sslServer;
  private ProtocolExecutor<InetSocketAddress, ReconfigurationPacket.PacketType, String> protocolExecutor;
  private final LNSNodeConfig nodeConfig;
  private final LNSConsistentReconfigurableNodeConfig crNodeConfig;
  private final InetSocketAddress address;
  private final AbstractJSONPacketDemultiplexer demultiplexer;

  //Fixme: Why is this here?
  private static boolean usePublicIP = false;


  public LocalNameServer() throws IOException {
    this(new InetSocketAddress(usePublicIP ? NetworkUtils.getLocalHostLANAddress()
            : InetAddress.getLoopbackAddress(),
            Config.getGlobalInt(GNSClientConfig.GNSCC.LOCAL_NAME_SERVER_PORT)),
            new LNSNodeConfig());
  }


  public LocalNameServer(InetSocketAddress originalNodeAddress, LNSNodeConfig nodeConfig) throws IOException {
    SSLDataProcessingWorker.SSL_MODES sslMode = ReconfigurationConfig.getClientSSLMode();
    // Get out address by convert the address to the client facing address. 
    this.address = new InetSocketAddress(originalNodeAddress.getAddress(),
            ReconfigurationConfig.getClientFacingSSLPort(originalNodeAddress.getPort()));
    LOGGER.log(Level.INFO, "LNS: SSL Mode is {0}; listening on {1}",
            new Object[]{sslMode.name(), address});

    this.nodeConfig = nodeConfig;
    this.crNodeConfig = new LNSConsistentReconfigurableNodeConfig(nodeConfig);
    AsyncLNSClient asyncClient;
    this.demultiplexer = new LNSPacketDemultiplexer<>(this, asyncClient = new AsyncLNSClient(
            ReconfigurationConfig.getReconfiguratorAddresses(),
            ReconfigurationConfig.getClientSSLMode(),
            ReconfigurationConfig.getClientPortSSLOffset()));
    // FIXME: Eventually need separate servers for ssl and clear
    //LNSPacketDemultiplexer<String> sslDemultiplexer = new LNSPacketDemultiplexer<>(this, asyncClient);

    this.cache = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(1000).build();
    try {
      JSONNIOTransport<InetSocketAddress> gnsNiot = new JSONNIOTransport<>(
              address, crNodeConfig, demultiplexer, sslMode);
      messenger = new JSONMessenger<>(gnsNiot);
      this.protocolExecutor = new ProtocolExecutor<>(messenger);
    } catch (IOException e) {
      LOGGER.log(Level.INFO, "Unabled to start LNS listener: " + e + "...ignoring.");
      // Just ignore this error for now... probably means we're running on a single machine.
      return;
    }
    LOGGER.log(Level.INFO, "Started LNS listener on {0}", address);
  }


  @Override
  public void shutdown() {
    messenger.stop();
    demultiplexer.stop();
    protocolExecutor.stop();
  }



  public static void main(String[] args) throws IOException {
    try {
      new LocalNameServer();
      //lns.testCache();
    } catch (IOException e) {
      System.out.println("Usage: java -cp GNS.jar edu.umass.cs.gnsserver.LocalNameServer");
    }
  }


  @Override
  public ProtocolExecutor<InetSocketAddress, PacketType, String> getProtocolExecutor() {
    return protocolExecutor;
  }


  @Override
  public LNSConsistentReconfigurableNodeConfig getNodeConfig() {
    return crNodeConfig;
  }


  @Override
  public InetSocketAddress getNodeAddress() {
    return address;
  }


  @Override
  public AbstractJSONPacketDemultiplexer getDemultiplexer() {
    return demultiplexer;
  }


  @Override
  public void addRequestInfo(long id, LNSRequestInfo requestInfo, NIOHeader header) {
    LOGGER.log(Level.INFO,
            "{0} inserting outgoing request {1}:{2} with header {3}",
            new Object[]{this, id + "", requestInfo, header});
    outstandingRequests.put(id, requestInfo);
    assert (outstandingRequests.get(id) != null);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }


  @Override
  public LNSRequestInfo removeRequestInfo(long id) {
    LOGGER.log(Level.INFO, "{0} matching repsonse with id {1}",
            new Object[]{this, id + ""});
    return outstandingRequests.remove(id);
  }


  @Override
  public LNSRequestInfo getRequestInfo(long id) {
    return outstandingRequests.get(id);
  }

  // marker for broken and/or hacky code

  protected static final String LNS_BAD_HACKY = "Bad hacky code: ";


  @Override
  public Set<InetSocketAddress> getReplicatedActives(String name) {
    // arun
    LOGGER.log(Level.WARNING, LNS_BAD_HACKY + "name = {0}", name);
    // FIXME: this needs work
    Set<InetSocketAddress> result = new HashSet<>();
    // FIXME: I don't think this returns the same set as the server uses so it's a good
    // thing this code is going away soon.
    for (InetSocketAddress socketAddress : crNodeConfig.getReplicatedActives(name)) {
      // If we're doing SSL we need to get the correct SSL port on the server.
      result.add(new InetSocketAddress(socketAddress.getAddress(),
              ReconfigurationConfig.getClientFacingSSLPort(socketAddress.getPort())));

    }
    return result;
  }

  static class AsyncLNSClient extends ReconfigurableAppClientAsync<Request> {

    private static Stringifiable<String> unstringer = new StringifiableDefault<String>(
            "");

    static final Set<IntegerPacketType> clientPacketTypes = new HashSet<IntegerPacketType>(
            Arrays.asList(Packet.PacketType.COMMAND_RETURN_VALUE));

    public AsyncLNSClient(Set<InetSocketAddress> reconfigurators,
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
          LOGGER.log(Level.INFO,
                  "{0} retrieving packet from received json {1}",
                  new Object[]{this, json});
          if (clientPacketTypes.contains(Packet.getPacketType(json))) {
            response = (Request) Packet.createInstance(json,
                    unstringer);
          }
          assert (response == null || response.getRequestType() == Packet.PacketType.COMMAND_RETURN_VALUE);
        }
      } catch (JSONException e) {
        LOGGER.log(Level.WARNING,
                "Problem parsing packet from {0}: {1}", new Object[]{json, e});
      }
      return response;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
      return clientPacketTypes;
    }
  }


  @Override
  public InetSocketAddress getClosestReplica(Set<InetSocketAddress> servers) {
    return getClosestReplica(servers, null);
  }


  @Override
  public InetSocketAddress getClosestReplica(Set<InetSocketAddress> serverIds, Set<InetSocketAddress> excludeServers) {
    if (serverIds == null || serverIds.isEmpty()) {
      return null;
    }

    long lowestLatency = Long.MAX_VALUE;
    InetSocketAddress serverAddress = null;
    for (InetSocketAddress serverId : serverIds) {
      if (excludeServers != null && excludeServers.contains(serverId)) {
        continue;
      }
      long pingLatency = nodeConfig.getPingLatency(serverId);
      if (pingLatency != LNSNodeConfig.INVALID_PING_LATENCY && pingLatency < lowestLatency) {
        lowestLatency = pingLatency;
        serverAddress = serverId;
      }
    }
    LOGGER.log(Level.FINE, "Closest server is {0}", serverAddress);
    return serverAddress;
  }


  @Override
  public void updateCacheEntry(String name, String value) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null) {
      cacheEntry.updateCacheEntry(value);
    } else {
      CacheEntry entry = new CacheEntry(name, value);
      cache.put(entry.getName(), entry);
    }
  }


  @Override
  public String getValueIfValid(String name) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null && cacheEntry.isValidValue()) {
      return cacheEntry.getValue();
    } else {
      return null;
    }
  }


  @Override
  public void updateCacheEntry(String name, Set<InetSocketAddress> actives) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null) {
      cacheEntry.updateCacheEntry(actives);
    } else {
      CacheEntry entry = new CacheEntry(name, actives);
      cache.put(entry.getName(), entry);
    }
  }

  @Override
  public void invalidateCacheEntry(String name) {
    cache.invalidate(name);
  }


  @Override
  public Set<InetSocketAddress> getActivesIfValid(String name) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null && cacheEntry.isValidActives()) {
      return cacheEntry.getActiveNameServers();
    } else {
      return null;
    }
  }


  @Override
  public void invalidateCache() {
    cache.invalidateAll();
  }


  @Override
  public boolean containsCacheEntry(String name) {
    return cache.getIfPresent(name) != null;
  }


  @SuppressWarnings("unchecked")
  @Override
  public boolean handleEvent(JSONObject json) throws JSONException {
    BasicReconfigurationPacket<String> rcEvent
            = (BasicReconfigurationPacket<String>) ReconfigurationPacket.getReconfigurationPacket(json, nodeConfig);
    return this.protocolExecutor.handleEvent(rcEvent);
  }


  @Override
  public void sendToClosestReplica(Set<InetSocketAddress> servers, JSONObject packet) throws IOException {
    InetSocketAddress replicaAddress = LocalNameServer.this.getClosestReplica(servers);
    // Remove these so the stamper will put new ones in so the packet will find it's way back here.
    // FIXME: arun: why not just not include them in toJSONObject()?
    packet.remove(MessageNIOTransport.SNDR_IP_FIELD);
    packet.remove(MessageNIOTransport.SNDR_PORT_FIELD);
    // Don't get a client facing port for these because they are returned as already translated.
    LOGGER.log(Level.INFO, "Sending to {0}: {1}", new Object[]{replicaAddress, packet});
    messenger.sendToAddress(replicaAddress, packet);
  }


  @Override
  public void sendToClient(InetSocketAddress isa, JSONObject msg) throws IOException {
    messenger.sendToAddress(isa, msg);
  }


  public void testCache() {
    String serviceName = "fred";
    Set<InetSocketAddress> actives;
    if ((actives = getActivesIfValid(serviceName)) != null) {
      LOGGER.severe("Cache should be empty!");
    }
    updateCacheEntry(serviceName, new HashSet<>(Arrays.asList(new InetSocketAddress(35000))));
    if ((actives = getActivesIfValid(serviceName)) == null) {
      LOGGER.severe("Cache should not be empty!");
    }
    StringBuilder cacheString = new StringBuilder();
    for (Entry<String, CacheEntry> entry : cache.asMap().entrySet()) {
      cacheString.append(entry.getKey());
      cacheString.append(" => ");
      cacheString.append(entry.getValue());
      cacheString.append("\n");
    }
    LOGGER.log(Level.INFO, "Cache Test: \n{0}", cacheString.toString());
  }

}
