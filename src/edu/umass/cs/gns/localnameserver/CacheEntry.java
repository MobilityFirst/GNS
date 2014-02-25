/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.RequestActivesPacket;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents the cache entry used at the local name server to cache DNS records
 *
 */
public class CacheEntry implements Comparable<CacheEntry> {

  /**
   * The GUID containing the key value pair.
   */
  private String name;
  /**
   * Time interval (in seconds) that the resource record may be cached before it should be discarded (default to zero)
   * Notice that we have ONE TTL for the entire cache entry which means one TTL for the whole name records.
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
  private HashSet<Integer> primaryNameServer;
  /**
   * A list of Active Nameservers for the name.
   */
  private Set<Integer> activeNameServer;

  /**
   * Constructs a cache entry using data from a DNS packet
   *
   * @param packet DNS packet
   */
  public CacheEntry(DNSPacket packet) {
    this.name = packet.getGuid();
    // this will depend on TTL sent by NS. 
    // UPDATE: NEVER LET IT BE -1 which means infinite
    this.timeToLiveInSeconds = packet.getTTL() == -1 ? GNS.DEFAULT_TTL_SECONDS : packet.getTTL();
    // pull all the keys and values out of the returned value and cache them
    for (Entry<String, ResultValue> entry : packet.getRecordValue().entrySet()) {
      String fieldKey = entry.getKey();
      ResultValue fieldValue = entry.getValue();
      this.valuesMap.put(fieldKey, fieldValue);
      // set the timestamp for that field
      this.timestampAddress.put(fieldKey, System.currentTimeMillis());
    }
    // Also update this
    this.primaryNameServer = (HashSet<Integer>) LocalNameServer.getPrimaryNameServers(name);
    this.activeNameServer = packet.getActiveNameServers();
  }

  public CacheEntry(RequestActivesPacket packet) {
    this.name = packet.getName();
    this.primaryNameServer = (HashSet<Integer>) LocalNameServer.getPrimaryNameServers(name);
    this.activeNameServer = packet.getActiveNameServers();
  }


  public synchronized void updateCacheEntry(DNSPacket packet) {

    activeNameServer = packet.getActiveNameServers();
    if (valuesMap == null) {
      valuesMap = new ValuesMap();
    }
    for (Entry<String, ResultValue> entry : packet.getRecordValue().entrySet()) {
      String fieldKey = entry.getKey();
      ResultValue fieldValue = entry.getValue();
      valuesMap.put(fieldKey, fieldValue);
      // set the timestamp for that field
      this.timestampAddress.put(fieldKey, System.currentTimeMillis());
    }
    timeToLiveInSeconds = packet.getTTL();
  }

  public synchronized void updateCacheEntry(RequestActivesPacket packet) {
    activeNameServer = packet.getActiveNameServers();
  }

  public synchronized void updateCacheEntry(ConfirmUpdateLNSPacket packet) {
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
  public synchronized HashSet<Integer> getPrimaryNameServer() {
    return primaryNameServer;
  }

  /**
   * @return the time to live of the cache entry
   */
  public synchronized int getTTL() {
    return timeToLiveInSeconds;
  }

  public synchronized Set<Integer> getActiveNameServers() {
    return activeNameServer;
  }

  public synchronized ResultValue getValue(NameRecordKey key) {
    if (isValidValue(key.getName())) {
      return valuesMap.get(key.getName());
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
    if (valuesMap == null || valuesMap.containsKey(key) == false) {
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
    return activeNameServer != null && activeNameServer.size() != 0;
  }

  public synchronized int timeSinceAddressCached(NameRecordKey nameRecordKey) {
    Long ts = timestampAddress.get(nameRecordKey.getName());
    if (ts == null) {
      return -1;
    }
    return (int) (System.currentTimeMillis() - ts);
  }

  public synchronized void invalidateActiveNameServer() {
    activeNameServer = null;
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
    for (int id : getPrimaryNameServer()) {
      if (first) {
        entry.append(id);
        first = false;
      } else {
        entry.append(", " + id);
      }
    }
    entry.append("]");

    entry.append("\n    ActiveNS:[");
    if (activeNameServer != null) {
      first = true;
      for (int id : activeNameServer) {
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
