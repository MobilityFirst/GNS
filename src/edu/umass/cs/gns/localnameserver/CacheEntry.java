/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.util.ConsistentHashing;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONException;

/**
 * Represents the cache entry used at the local name server to cache DNS records.
 * The cache store three key information:
 * (1) set of key-value pairs for a name record.
 * (2) ttl value for record.
 * (3) set of active replicas for a name.
 *
 * The TTL is used only to determine whether key value pairs are valid.
 * The set of active replicas is valid for an infinite period until
 * explicitly invalidated or updated by local name server. The TTL value is set based on TTL value
 * stored at name servers, and is not determined by the local name server.
 *
 * @author abhigyan
 */
public class CacheEntry<NodeIDType> implements Comparable<CacheEntry> {

  /**
   * The GUID containing the key value pair.
   */
  private String name;
  /**
   * Time interval (in seconds) that the resource record may be cached before it should be discarded (default to zero)
   * Notice that we have ONE TTL for the entire cache entry which means one TTL for the whole name record.
   * But we keep individual timestamps for each key / value mapping.
   */
  private int timeToLiveInSeconds = GNS.DEFAULT_TTL_SECONDS;
  /**
   * Time stamp (in milleseconds) when the value for each field was inserted into record.
   */
  private ConcurrentHashMap<String, Long> timestampAddress = new ConcurrentHashMap<String, Long>();
  /**
   * The value of the key value pair.. NOTE: Value can be NULL meaning that the value is not valid.
   */
  private ValuesMap valuesMap = new ValuesMap();
  /**
   * A list of primary name servers for the name
   */
  private HashSet<NodeIDType> replicaControllers;
  /**
   * A list of Active Nameservers for the name.
   */
  private Set<NodeIDType> activeNameServers;

  /**
   * Constructs a cache entry for a name from a list of replica controllers and a list of active replicas.
   *
   * @param name
   * @param replicaControllers
   * @param activeNameServers
   */
  public CacheEntry(String name, HashSet<NodeIDType> replicaControllers, Set<NodeIDType> activeNameServers) {
    this.name = name;
    this.replicaControllers = replicaControllers;
    this.activeNameServers = activeNameServers;
  }

  /**
   * Constructs a cache entry using data from a DNS packet
   *
   * @param packet DNS packet
   */
  public CacheEntry(DNSPacket<NodeIDType> packet) {
    this.name = packet.getGuid();
    // this will depend on TTL sent by NS.
    // UPDATE: NEVER LET IT BE -1 which means infinite
    this.timeToLiveInSeconds = packet.getTTL() == -1 ? GNS.DEFAULT_TTL_SECONDS : packet.getTTL();
    // ONLY CACHE DNSPackets where the key is +ALL-FIELDS+ or a top-level field
    if (packet.keyIsAllFieldsOrTopLevel()) {
      // pull all the keys and values out of the returned value and cache them
      ValuesMap packetRecordValue = packet.getRecordValue();
      Iterator<?> keyIter = packetRecordValue.keys();
      while (keyIter.hasNext()) {
        String fieldKey = (String) keyIter.next();
        try {
          Object fieldValue = packetRecordValue.get(fieldKey);
          this.valuesMap.put(fieldKey, fieldValue);
        } catch (JSONException e) {
          GNS.getLogger().severe("Unabled to create cache entry for key " + fieldKey + ":" + e);
        }
//      ResultValue fieldValue = packetRecordValue.getAsArray(fieldKey);
//      this.valuesMap.putAsArray(fieldKey, fieldValue);
        // set the timestamp for that field
        this.timestampAddress.put(fieldKey, System.currentTimeMillis());
      }
    }
    this.replicaControllers = (HashSet<NodeIDType>) ConsistentHashing.getReplicaControllerSet(name);
//    this.activeNameServer = packet.getActiveNameServers();
  }

  /**
   * Constructs a cache entry in the case where active name servers will be the same as replica controllers.
   *
   * @param name
   * @param primaryNameServers
   */
  public CacheEntry(String name, Set<NodeIDType> primaryNameServers) {
    this(name, (HashSet<NodeIDType>) primaryNameServers, primaryNameServers);
  }

  /**
   * Constructs a cache entry from a RequestActivesPacket response packet.
   *
   * @param packet
   */
  public CacheEntry(RequestActivesPacket<NodeIDType> packet) {
    this(packet.getName(), (HashSet) ConsistentHashing.getReplicaControllerSet(packet.getName()),
            packet.getActiveNameServers());
  }

  public synchronized void updateCacheEntry(DNSPacket<NodeIDType> packet) {

    if (valuesMap == null) {
      valuesMap = new ValuesMap();
    }
    ValuesMap packetRecordValue = packet.getRecordValue();
    Iterator<?> keyIter = packetRecordValue.keys();
    while (keyIter.hasNext()) {
      String fieldKey = (String) keyIter.next();
      try {
        Object fieldValue = packetRecordValue.get(fieldKey);
        this.valuesMap.put(fieldKey, fieldValue);
      } catch (JSONException e) {
        GNS.getLogger().severe("Unabled to update cache entry for key " + fieldKey + ":" + e);
      }
//      ResultValue fieldValue = packetRecordValue.getAsArray(fieldKey);
//      valuesMap.putAsArray(fieldKey, fieldValue);
      // set the timestamp for that field
      this.timestampAddress.put(fieldKey, System.currentTimeMillis());
    }
    timeToLiveInSeconds = packet.getTTL();
  }

  public synchronized void updateCacheEntry(RequestActivesPacket<NodeIDType> packet) {
    activeNameServers = packet.getActiveNameServers();
  }

  public synchronized void updateCacheEntry(ConfirmUpdatePacket<NodeIDType> packet) {
    // invalidate the valuesMap part of the cache... best we can do since the packet has no info
    // it will be refreshed on next read
    valuesMap = null;
  }

  /**
   * @return the name
   */
  public synchronized String getName() {
    return name;
  }

  /**
   * @return the primaryNameServer
   */
  public synchronized HashSet<NodeIDType> getReplicaControllers() {
    return replicaControllers;
  }

  /**
   * @return the time to live of the cache entry
   */
  public synchronized int getTTL() {
    return timeToLiveInSeconds;
  }

  public synchronized Set<NodeIDType> getActiveNameServers() {
    return activeNameServers;
  }

  // FIXME: Handle returning cache values for non-array keys as well
  // as the entire record.
  /**
   * Returns the value in the cache for this key as an array.
   * If it's not an array an exception will be thrown and ignored, basically.
   *
   * @param key
   * @return
   */
  public synchronized ResultValue getValueAsArray(String key) {
    if (isValidValue(key)) {
      return valuesMap.getAsArray(key);
    }
    return null;
  }

  /**
   * Returns true if the contains the key and the ttl associated with key has not expired in the cache.
   *
   * @param key
   * @return true or false value regarding cache being valid
   */
  private synchronized boolean isValidValue(String key) {
    // NULL MEANS THE VALUE IS INVALID
    if (valuesMap == null || valuesMap.has(key) == false) {
      return false;
    }
    int keyTimeToLiveInSeconds = getKeyTTL(key);
    //-1 means infinite TTL
    if (keyTimeToLiveInSeconds == -1) {
      return true;
    } // 0 means TTL == 0
    else if (keyTimeToLiveInSeconds == 0) {
      return false;
    } // else TTL is the value of field timeToLive.
    else {
      Long timeStampInMillesconds = timestampAddress.get(key);
      if (timeStampInMillesconds == null) {
        return false;
      }
      return (System.currentTimeMillis() - timeStampInMillesconds) < (keyTimeToLiveInSeconds * 1000);
    }
  }

  /**
   * Allow special case handling of TTLs for certain keys
   *
   * @param key
   */
  private int getKeyTTL(String key) {
    return timeToLiveInSeconds;
  }

  /**
   * Returns true if a non-empty set of active name servers is stored in cache.
   *
   * @return true if a non-empty set of active name servers is stored in cache
   */
  public synchronized boolean isValidNameserver() {
    return activeNameServers != null && activeNameServers.size() != 0;
  }

  public synchronized int timeSinceAddressCached(String key) {
    Long ts = timestampAddress.get(key);
    if (ts == null) {
      return -1;
    }
    return (int) (System.currentTimeMillis() - ts);
  }

  public synchronized void invalidateActiveNameServer() {
    activeNameServers = null;
  }

  /**
   * Attempts to come up with a pretty string representation of the cache entry.
   *
   * @return string representation of the cache entry
   */
  @Override
  public synchronized String toString() {
    StringBuilder entry = new StringBuilder();

    entry.append("Name:" + getName());
    //entry.append(" Key: " + getRecordKey().getName());
    entry.append("\n    TTLAddress:" + timeToLiveInSeconds);
    entry.append("\n    TimestampAddress: " + timeStampHashToString(timestampAddress, timeToLiveInSeconds * 1000));
    entry.append("\n    PrimaryNS:[");
    boolean first = true;
    for (NodeIDType id : replicaControllers) {
      if (first) {
        entry.append(id.toString());
        first = false;
      } else {
        entry.append(", " + id.toString());
      }
    }
    entry.append("]");

    entry.append("\n    ActiveNS:[");
    if (activeNameServers != null) {
      first = true;
      for (NodeIDType id : activeNameServers) {
        if (first) {
          entry.append(id);
          first = false;
        } else {
          entry.append(", " + id);
        }
      }
    }
    entry.append("]");
    // NULL MEANS THE VALUE IS INVALID
    entry.append("\n    Value:" + (valuesMap == null ? "INVALID" : valuesMap.toString()));
    return entry.toString();
  }

  private String timeStampHashToString(ConcurrentHashMap<String, Long> map, long ttlInMilleseconds) {
    long currentTime = System.currentTimeMillis();
    StringBuilder result = new StringBuilder();
    String prefix = "";
    for (Entry<String, Long> entry : map.entrySet()) {
      result.append(prefix);
      result.append(entry.getKey());
      result.append("=");
      result.append(entry.getValue());
      if (currentTime - entry.getValue() >= ttlInMilleseconds) {
        result.append("(*EXPIRED*)");
      }
      prefix = ", ";
    }
    return result.toString();
  }

  /**
   * So we can sort them when we display them.
   *
   * @param d CacheEntry
   * @return
   */
  @Override
  public int compareTo(CacheEntry d) {
    return (this.getName()).compareTo(d.getName());
  }

}
