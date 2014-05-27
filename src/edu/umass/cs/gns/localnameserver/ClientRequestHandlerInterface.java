/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.RequestHandlerParameters;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.BasicPacket;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.RequestActivesPacket;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket;
import edu.umass.cs.gns.util.NameRecordKey;
import java.util.Set;
import org.json.JSONObject;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 **
 * An interface for handling of client requests, comms and cacheing.
 * Abstracts out the methods for storing of request info, caching and 
 * communication needs of
 * a node.
 * 
 * Someday maybe both NS and LNS will use this.
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
  public GNSNodeConfig getGnsNodeConfig();
  
  /**
   * Returns that set of parameters used to control the handlers behavior.
   * 
   * @return 
   */
  public RequestHandlerParameters getParameters();
  
  /**
   * Returns the id of this node.
   * 
   * @return 
   */
  public int getNodeID();

  // REQUEST AND UPDATE INFO METHODS
  
  /**
   **
   * Adds information of a transmitted query to a query transmitted map.
   *
   * @param name Host/Domain name
   * @param recordKey
   * @param nameserverID Name server Id
   * @param time System time during transmission
   * @param queryStatus
   * @param lookupNumber
   * @param incomingPacket
   * @param numRestarts
   * @return A unique id for the query
   */
  public int addDNSRequestInfo(String name, NameRecordKey recordKey,
          int nameserverID, long time, String queryStatus, int lookupNumber,
          DNSPacket incomingPacket, int numRestarts);

  /**
   * Adds information of a transmitted update to a query transmitted map.
   * 
   * @param name
   * @param nameserverID
   * @param time
   * @param numRestarts
   * @param updateAddressPacket
   * @return 
   */
  public int addUpdateInfo(String name, int nameserverID, long time,
          int numRestarts, BasicPacket updateAddressPacket);

  /**
   * Adds information of a transmitted select to a query transmitted map.
   * @param recordKey
   * @param incomingPacket
   * @return 
   */
  public int addSelectInfo(NameRecordKey recordKey, SelectRequestPacket incomingPacket);

  /**
   **
   * Removes and returns QueryInfo entry from the map for a query Id..
   *
   * @param id Query Id
   * @return 
   */
  public DNSRequestInfo removeDNSRequestInfo(int id);

  /**
   * 
   * @param id
   * @return 
   */
  public UpdateInfo removeUpdateInfo(int id);

  /**
   * 
   * @param id
   * @return 
   */
  public SelectInfo removeSelectInfo(int id);

  /**
   * Returns the update info for id.
   * @param id
   * @return 
   */
  public UpdateInfo getUpdateInfo(int id);

  /**
   * Returns the select info for id.
   * 
   * @param id
   * @return 
   */
  public SelectInfo getSelectInfo(int id);

  /**
   **
   * Returns true if the map contains the specified query id, false otherwise.
   *
   * @param id Query Id
   * @return 
   */
  public boolean containsDNSRequestInfo(int id);

  public DNSRequestInfo getDNSRequestInfo(int id);

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
  public CacheEntry addCacheEntry(DNSPacket packet);

  /**
   * 
   * @param packet
   * @return 
   */
  public CacheEntry addCacheEntry(RequestActivesPacket packet);

  /**
   * Updates an existing cache entry with new information from a DNS packet.
   *
   * @param packet DNS packet containing record
   * @return 
   */
  public CacheEntry updateCacheEntry(DNSPacket packet);

  /**
   * 
   * @param packet 
   */
  public void updateCacheEntry(RequestActivesPacket packet);

  /**
   * 
   * @param packet
   * @param name
   * @param key 
   */
  public void updateCacheEntry(ConfirmUpdatePacket packet, String name, NameRecordKey key);

  /**
   * Returns a cache entry for the specified name. Returns null if the cache does not have the key mapped to an entry
   *
   * @param name Host/Domain name
   * @return 
   */
  public CacheEntry getCacheEntry(String name); 

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
  public int timeSinceAddressCached(String name, NameRecordKey recordKey);

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
  public Set<Integer> getReplicaControllers(String name);
  
  /**
   **
   * Returns the closest primary name server for <i>name</i>.
   *
   * @param name
   * @param nameServersQueried
   * @return Closest primary name server for <i>name</i>, or -1 if no such name server is present.
   *
   */
  public int getClosestReplicaController(String name, Set<Integer> nameServersQueried);

  /**
   * Send packet to NS
   *
   * @param json
   * @param ns
   */
  public void sendToNS(JSONObject json, int ns); 

  /**
   * Send a JSON packet to an IP address / port.
   * @param json
   * @param address
   * @param port 
   */
  public void sendToAddress(JSONObject json, String address, int port);
  
  /**
   * 
   * @param name
   * @param nodeIDs
   * @return 
   */
  public int getDefaultCoordinatorReplica(String name, Set<Integer> nodeIDs); 
  
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
}
  