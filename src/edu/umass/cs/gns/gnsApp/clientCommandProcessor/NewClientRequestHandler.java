  /*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.RequestInfo;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.SelectInfo;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.Admintercessor;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.gnsApp.GnsApp;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.gnsApp.packet.SelectRequestPacket;
import edu.umass.cs.gns.util.MovingAverage;
import edu.umass.cs.gns.util.Util;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.InterfaceSSLMessenger;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;

import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.json.JSONException;

/**
 * Implements basic functionality needed by servers to handle client type requests.
 * Abstracts out the storing of request info, caching and communication needs of
 * a node.
 *
 * Note: This is based on original LNS code, but at some point the idea was that the LNS and NS
 * could both use this interface. Not sure if that is going to happen now, but there
 * is a need for certain services at the NS that the LNS implements (like caching and
 * retransmission of lookups).
 *
 * @author westy
 */
public class NewClientRequestHandler implements ClientRequestHandlerInterface {

  private final Intercessor intercessor;
  private final Admintercessor admintercessor;
  private final RequestHandlerParameters parameters;
  private final ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(5);
  /**
   * Map of information about queries transmitted. Key: QueryId, Value: QueryInfo (id, name, time etc.)
   */
  private final ConcurrentMap<Integer, RequestInfo> requestInfoMap;
  private final ConcurrentMap<Integer, SelectInfo> selectTransmittedMap;
  // For backward compatibility between old Add and Remove record code and new name service code.
  // Maps between service name and LNS Request ID (which is the key to the above maps).
  private final ConcurrentMap<String, Integer> createServiceNameMap;
  private final ConcurrentMap<String, Integer> deleteServiceNameMap;
  private final ConcurrentMap<String, Integer> activesServiceNameMap;
  
  /**
   * GNS node config object used by LNS to toString node information, such as IP, Port, ping latency.
   */
  private final GNSNodeConfig<String> gnsNodeConfig;

  private final ConsistentReconfigurableNodeConfig<String> nodeConfig;

  private final InterfaceSSLMessenger<String, JSONObject> messenger;

  private final Random random;

  private final ProtocolExecutor<String, ReconfigurationPacket.PacketType, String> protocolExecutor;
  private final CCPProtocolTask<String> protocolTask;

  /**
   * Host address of the local name server.
   */
  private final InetSocketAddress nodeAddress;
  //
  private final String activeReplicaID;
  private final GnsApp app;

  private long receivedRequests = 0;

  public NewClientRequestHandler(Intercessor intercessor, Admintercessor admintercessor,
          InetSocketAddress nodeAddress,
          String activeReplicaID,
          GnsApp app,
          GNSNodeConfig<String> gnsNodeConfig,
          JSONMessenger<String> messenger, RequestHandlerParameters parameters) {
    this.intercessor = intercessor;
    this.admintercessor = admintercessor;
    this.parameters = parameters;
    this.nodeAddress = nodeAddress;
    // a little hair to convert fred to fred-activeReplica if we just get fred
    this.activeReplicaID = gnsNodeConfig.isActiveReplica(activeReplicaID) ? activeReplicaID
            : gnsNodeConfig.getReplicaNodeIdForTopLevelNode(activeReplicaID);
    this.app = app;
    // FOR NOW WE KEEP BOTH
    this.nodeConfig = new ConsistentReconfigurableNodeConfig<String>(gnsNodeConfig);
    this.gnsNodeConfig = gnsNodeConfig;
    this.requestInfoMap = new ConcurrentHashMap<>(10, 0.75f, 3);
    this.selectTransmittedMap = new ConcurrentHashMap<>(10, 0.75f, 3);
    this.random = new Random(System.currentTimeMillis());
    this.messenger = messenger;
    this.protocolExecutor = new ProtocolExecutor<>(messenger);
    this.protocolTask = new CCPProtocolTask<>(this);
    this.protocolExecutor.register(this.protocolTask.getEventTypes(), this.protocolTask);
    this.createServiceNameMap = new ConcurrentHashMap<>(10, 0.75f, 3);
    this.deleteServiceNameMap = new ConcurrentHashMap<>(10, 0.75f, 3);
    this.activesServiceNameMap = new ConcurrentHashMap<>(10, 0.75f, 3);
  }

  @Override
  public boolean handleEvent(JSONObject json) throws JSONException {
    @SuppressWarnings("unchecked")
    BasicReconfigurationPacket<String> rcEvent
            = (BasicReconfigurationPacket<String>) ReconfigurationPacket.getReconfigurationPacket(json, gnsNodeConfig);
    return this.protocolExecutor.handleEvent(rcEvent);
  }

  /**
   * @return the executorService
   */
  @Override
  public ScheduledThreadPoolExecutor getExecutorService() {
    return executorService;
  }

  @Override
  public GNSNodeConfig<String> getGnsNodeConfig() {
    return gnsNodeConfig;
  }

  @Override
  public ConsistentReconfigurableNodeConfig<String> getNodeConfig() {
    return nodeConfig;
  }

  @Override
  public InetSocketAddress getNodeAddress() {
    return nodeAddress;
  }

  @Override
  public String getActiveReplicaID() {
    return activeReplicaID;
  }

  @Override
  public Intercessor getIntercessor() {
    return intercessor;
  }

  @Override
  public Admintercessor getAdmintercessor() {
    return admintercessor;
  }

  @Override
  public RequestHandlerParameters getParameters() {
    return parameters;
  }

  public GnsApp getApp() {
    return app;
  }

  // REQUEST INFO METHODS 
  // What happens when this overflows?
  private int currentRequestID = 0;

  @Override
  public synchronized int getUniqueRequestID() {
    return currentRequestID++;
  }

  @Override
  public void addRequestInfo(int id, RequestInfo requestInfo) {
    requestInfoMap.put(id, requestInfo);
  }

  // These next four are for backward compatibility between old Add and Remove record 
  // code and new name service code.
  // Maps between service name and LNS Request ID (which is the key to the above maps).
  @Override
  /**
   * Creates a mapping between a create service name and the Add/RemoveRecord that triggered it.
   */
  public void addCreateRequestNameToIDMapping(String name, int id) {
    createServiceNameMap.put(name, id);
  }

  @Override
  /**
   * Looks up the mapping between a CreateServiceName and the Add/RemoveRecord that triggered it.
   */
  public Integer getCreateRequestNameToIDMapping(String name) {
    return createServiceNameMap.get(name);
  }

  @Override
  /**
   * Looks up and removes the mapping between a CreateServiceName and the Add/RemoveRecord that triggered it.
   */
  public Integer removeCreateRequestNameToIDMapping(String name) {
    return createServiceNameMap.remove(name);
  }

  // These next four are for backward compatibility between old Add and Remove record 
  // code and new name service code.
  // Maps between service name and LNS Request ID (which is the key to the above maps).
  @Override
  /**
   * Creates a mapping between a create service name and the Add/RemoveRecord that triggered it.
   */
  public void addDeleteRequestNameToIDMapping(String name, int id) {
    deleteServiceNameMap.put(name, id);
  }

  @Override
  /**
   * Looks up the mapping between a CreateServiceName and the Add/RemoveRecord that triggered it.
   */
  public Integer getDeleteRequestNameToIDMapping(String name) {
    return deleteServiceNameMap.get(name);
  }

  @Override
  /**
   * Looks up and removes the mapping between a CreateServiceName and the Add/RemoveRecord that triggered it.
   */
  public Integer removeDeleteRequestNameToIDMapping(String name) {
    return deleteServiceNameMap.remove(name);
  }

  // These next four are for backward compatibility between old Add and Remove record 
  // code and new name service code.
  // Maps between service name and LNS Request ID (which is the key to the above maps).
  @Override
  /**
   * Creates a mapping between a create service name and the Add/RemoveRecord that triggered it.
   */
  public void addActivesRequestNameToIDMapping(String name, int id) {
    activesServiceNameMap.put(name, id);
  }

  @Override
  /**
   * Looks up the mapping between a CreateServiceName and the Add/RemoveRecord that triggered it.
   */
  public Integer getActivesRequestNameToIDMapping(String name) {
    return activesServiceNameMap.get(name);
  }

  @Override
  /**
   * Looks up and removes the mapping between a CreateServiceName and the Add/RemoveRecord that triggered it.
   */
  public Integer removeActivesRequestNameToIDMapping(String name) {
    return activesServiceNameMap.remove(name);
  }

  @Override
  public int addSelectInfo(String recordKey, SelectRequestPacket incomingPacket) {
    int id;
    do {
      id = random.nextInt();
    } while (selectTransmittedMap.containsKey(id));
    //Add query info
    SelectInfo query = new SelectInfo(id);
    selectTransmittedMap.put(id, query);
    return id;
  }

  @Override
  public RequestInfo getRequestInfo(int id) {
    return requestInfoMap.get(id);
  }

  /**
   **
   * Removes and returns QueryInfo entry from the map for a query Id..
   *
   * @param id Query Id
   * @return
   */
  @Override
  public RequestInfo removeRequestInfo(int id) {
    return requestInfoMap.remove(id);
  }

  @Override
  public SelectInfo removeSelectInfo(int id) {
    return selectTransmittedMap.remove(id);
  }

  @Override
  public SelectInfo getSelectInfo(int id) {
    return selectTransmittedMap.get(id);
  }
  
  private boolean reallySendtoReplica = false;

  @Override
  public boolean reallySendUpdateToReplica() {
    return reallySendtoReplica;
  }

  @Override
  public void setReallySendUpdateToReplica(boolean reallySend) {
    reallySendtoReplica = reallySend;
  }

//  static class CacheComparator implements Comparator<CacheEntry> {
//
//    @Override
//    public int compare(CacheEntry t1, CacheEntry t2) {
//      return t1.compareTo(t2);
//    }
//  }

  /**
   **
   * Return a Set containing ids of primary replica for <i>name</i>
   *
   * @param name
   * @return
   */
  @Override
  public Set<String> getReplicaControllers(String name) {
    return nodeConfig.getReplicatedReconfigurators(name);
  }

  /**
   **
   * Returns the closest primary name server for <i>name</i>.
   *
   * @return Closest primary name server for <i>name</i>, or -1 if no such name server is present.
   *
   */
  @Override
  public String getClosestReplicaController(String name, Set<String> nameServersQueried) {
    try {
      Set<String> primaries = getReplicaControllers(name);
      if (parameters.isDebugMode()) {
        GNS.getLogger().info("Primary Name Servers: " + Util.setOfNodeIdToString(primaries) + " for name: " + name);
      }

      String x = gnsNodeConfig.getClosestServer(primaries, nameServersQueried);
      if (parameters.isDebugMode()) {
        GNS.getLogger().info("Closest Primary Name Server: " + x.toString() + " NS Queried: " + Util.setOfNodeIdToString(nameServersQueried));
      }
      return x;
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public String getRandomReplica() {
    int index = (int) (this.gnsNodeConfig.getActiveReplicas().size() * Math.random());
    return (String) (this.gnsNodeConfig.getActiveReplicas().toArray()[index]);
  }

  @Override
  public String getRandomReconfigurator() {
    int index = (int) (this.gnsNodeConfig.getReconfigurators().size() * Math.random());
    return (String) (this.gnsNodeConfig.getReconfigurators().toArray()[index]);
  }

  @Override
  public String getFirstReplica() {
    return this.gnsNodeConfig.getActiveReplicas().iterator().next();
  }

  @Override
  public String getFirstReconfigurator() {
    return this.gnsNodeConfig.getReconfigurators().iterator().next();
  }
  
  @Override
  public void sendRequestToReconfigurator(BasicReconfigurationPacket req, String id) throws JSONException, IOException {
    if (parameters.isDebugMode()) {
      GNS.getLogger().info("Sending " + req.getSummary() + " to " + id + ":" + this.nodeConfig.getNodeAddress(id) + ":" + this.nodeConfig.getNodePort(id) + ": " + req);
    }
    this.messenger.send(new GenericMessagingTask<String, Object>(id, req.toJSONObject()));
  }

  /**
   * Send packet to NS
   *
   * @param json
   * @param id
   */
  @Override
  public void sendToNS(JSONObject json, String id) {
    try {
      if (parameters.isDebugMode()) {
        GNS.getLogger().info("Send to: " + id + " json: " + json);
      }
      messenger.sendToID(id, json);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Send packet to NS
   */
  @Override
  public void sendToAddress(JSONObject json, String address, int port) {
    try {
      messenger.sendToAddress(new InetSocketAddress(address, port), json);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  long lastRecordedTime = -1;
  // Maintains a moving average of server request load to smooth out the burstiness.
  private long deferedCnt = 0; // a little hair in case we are getting requests too fast for the nanosecond timer (is this likely?)
  private final MovingAverage averageRequestsPerSecond = new MovingAverage(40);

  @Override
  public void updateRequestStatistics() {
    // first time do nothing
    if (lastRecordedTime == -1) {
      lastRecordedTime = System.nanoTime();
      return;
    }
    long currentTime = System.nanoTime();
    long timeDiff = currentTime - lastRecordedTime;
    deferedCnt++;
    // in case we are running faster than the clock
    if (timeDiff != 0) {
      // multiple by 1,000,000,000 cuz we're computing Ops per SECOND
      averageRequestsPerSecond.add((int) (deferedCnt * 1000000000L / timeDiff));
      deferedCnt = 0;
      lastRecordedTime = currentTime;
    }
    receivedRequests++;
  }

  /**
   * Instrumentation.
   *
   * @return
   */
  @Override
  public long getReceivedRequests() {
    return receivedRequests;
  }

  /**
   * Instrumentation.
   *
   * @return
   */
  @Override
  public int getRequestsPerSecond() {
    return (int) Math.round(averageRequestsPerSecond.getAverage());
  }
}
