/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Admintercessor;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Intercessor;
import edu.umass.cs.gns.newApp.clientCommandProcessor.RequestHandlerParameters;
import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.newApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.newApp.packet.DNSPacket;
import edu.umass.cs.gns.newApp.packet.RequestActivesPacket;
import edu.umass.cs.gns.newApp.packet.SelectRequestPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import java.net.InetSocketAddress;
import java.util.Set;
import org.json.JSONObject;
import java.util.concurrent.ScheduledThreadPoolExecutor;


/**
 **
 * An interface for handling of client requests, comms and cacheing.
 * Abstracts out the methods for storing of request info, caching and
 * communication needs of a node. A lot of this code used to be static methods in the 
 * ClientCommandProcessor (CCP) (formerly the LocalNameServer).
 * This class makes the code that uses it not depend statically on the CCP.
 *
 */
public interface ClientRequestHandlerInterface {

  /**
   * The executor that runs tasks.
   *
   * @return the executorService
   */
  public ScheduledThreadPoolExecutor getExecutorService();

  /**
   * Maintains information about other nodes.
   *
   * @return
   */
  public GNSNodeConfig<String> getGnsNodeConfig();

  // FIXME: During transition we have both this and the above.
  public ConsistentReconfigurableNodeConfig<String> getNodeConfig();

  /**
   * Returns that set of parameters used to control the handlers behavior.
   *
   * @return
   */
  public RequestHandlerParameters getParameters();

  /**
   * Returns the address of this node.
   *
   * @return
   */
  public InetSocketAddress getNodeAddress();

  public String getActiveReplicaID();

  public Intercessor getIntercessor();

  public Admintercessor getAdmintercessor();

  // REQUEST INFO METHODS
  public int getUniqueRequestID();

  public void addRequestInfo(int id, RequestInfo requestInfo);

  /**
   **
   * Removes and returns QueryInfo entry from the map for a query Id..
   *
   * @param id Query Id
   * @return
   */
  public RequestInfo removeRequestInfo(int id);

  /**
   * Returns the update info for id.
   *
   * @param id
   * @return
   */
  public RequestInfo getRequestInfo(int id);

  /**
   * Adds information of a transmitted select to a query transmitted map.
   *
   * @param recordKey
   * @param incomingPacket
   * @return
   */
  public int addSelectInfo(String recordKey, SelectRequestPacket<String> incomingPacket);

  /**
   * @param id
   * @return
   */
  public SelectInfo removeSelectInfo(int id);

  /**
   * Returns the select info for id.
   *
   * @param id
   * @return
   */
  public SelectInfo getSelectInfo(int id);

  // CACHE METHODS
  /**
   * Clears the cache
   */
  public void invalidateCache();

  /**
   **
   * Returns true if the local name server cache contains DNS record for the specified name, false otherwise
   *
   * @param name Host/Domain name
   * @return
   */
  public boolean containsCacheEntry(String name);

  /**
   **
   * Adds a new CacheEntry (NameRecord) from a DNS packet. Overwrites existing cache entry for a name, if the name
   * record exist in the cache.
   *
   * @param packet DNS packet containing record
   * @return
   */
  public CacheEntry<String> addCacheEntry(DNSPacket<String> packet);

  /**
   *
   * @param packet
   * @return
   */
  public CacheEntry<String> addCacheEntry(RequestActivesPacket<String> packet);

  /**
   * Updates an existing cache entry with new information from a DNS packet.
   *
   * @param packet DNS packet containing record
   * @return
   */
  public CacheEntry<String> updateCacheEntry(DNSPacket<String> packet);

  /**
   *
   * @param packet
   */
  public void updateCacheEntry(RequestActivesPacket<String> packet);

  /**
   *
   * @param packet
   * @param name
   * @param key
   */
  public void updateCacheEntry(ConfirmUpdatePacket<String> packet, String name, String key);

  /**
   * Returns a cache entry for the specified name. Returns null if the cache does not have the key mapped to an entry
   *
   * @param name Host/Domain name
   * @return
   */
  public CacheEntry<String> getCacheEntry(String name);

  /**
   * Checks the validity of active nameserver set in cache.
   *
   * @param name Host/device/domain name whose name record is cached.
   * @return Returns true if the entry is valid, false otherwise
   */
  public boolean isValidNameserverInCache(String name);

  /**
   *
   * @param name
   * @param recordKey
   * @return
   */
  public int timeSinceAddressCached(String name, String recordKey);

  /**
   * Invalidates the active name server set in cache by setting its value to <i>null</i>.
   *
   * @param name
   */
  public void invalidateActiveNameServer(String name);

  /**
   * Prints cache to a string (and sorts it for convenience)
   *
   * @param preamble
   * @return
   */
  public String getCacheLogString(String preamble);

  // NETWORK METHODS
  /**
   **
   * Return a Set containing ids of primary replica for <i>name</i>
   *
   * @param name
   * @return
   */
  public Set<String> getReplicaControllers(String name);

  /**
   **
   * Returns the closest primary name server for <i>name</i>.
   *
   * @param name
   * @param nameServersQueried
   * @return Closest primary name server for <i>name</i>, or -1 if no such name server is present.
   *
   */
  public String getClosestReplicaController(String name, Set<String> nameServersQueried);

  /**
   * Send packet to NS
   *
   * @param json
   * @param ns
   */
  public void sendToNS(JSONObject json, String ns);

  /**
   * Send a JSON packet to an IP address / port.
   *
   * @param json
   * @param address
   * @param port
   */
  public void sendToAddress(JSONObject json, String address, int port);

  // STATS MAP
  /**
   * Returns the NameRecordStats object for a given guid.
   *
   * @param name
   * @return
   */
  public NameRecordStats getStats(String name);

  public Set<String> getNameRecordStatsKeySet();

  public void incrementLookupRequest(String name);

  public void incrementUpdateRequest(String name);

  public void incrementLookupResponse(String name);

  public void incrementUpdateResponse(String name);

  /**
   **
   * Prints name record statistic
   *
   * @return
   */
  public String getNameRecordStatsMapLogString();

  /**
   * Instrumentation - Updates various instrumentation including the request counter and requests per second
   */
  public void updateRequestStatistics();

  /**
   * Instrumentation - Return the request counter.
   *
   * @return
   */
  public long getReceivedRequests();

  /**
   * Instrumentation - Return the requests per second measure.
   *
   * @return
   */
  public int getRequestsPerSecond();
  
  public boolean reallySendUpdateToReplica();
  
  public void setReallySendUpdateToReplica(boolean reallySend);

}
