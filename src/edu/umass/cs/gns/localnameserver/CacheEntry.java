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
import edu.umass.cs.gns.packet.TinyQuery;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents the cache entry used at the local name server to cache DNS records
 *
 */
public class CacheEntry {

  /**
   * The GUID containing the key value pair.
   */
  private String name;
  /**
   * Time interval (in seconds) that the resource record may be cached before it should be discarded (default to zero)
   * Notice that we have ONE TTL for the entire cache entry which means one TTL for the whole name records.
   * But we keep individual timestamps for each key / value mapping.
   */
  private int timeToLive = GNS.DEFAULTTTLINSECONDS;
  /**
   * Time stamp when the value for each field was inserted into record.
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
    this.name = packet.getQname();
    // this will depend on TTL sent by NS. 
    // UPDATE: NEVER LET IT BE -1 which means infinite
    this.timeToLive = packet.getTTL() == -1 ? GNS.DEFAULTTTLINSECONDS : packet.getTTL();
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
  }

  public CacheEntry(RequestActivesPacket packet) {
    this.name = packet.getName();
    this.primaryNameServer = (HashSet<Integer>) LocalNameServer.getPrimaryNameServers(name);
    this.activeNameServer = packet.getActiveNameServers();
  }

  public synchronized ResultValue getValue(NameRecordKey key) {
    if (isValidValue(key.getName())) {
      return valuesMap.get(key.getName());
    }
    return null;
  }

  /**
   * Special case handling of TTLs for certain keys
   * @param key 
   */
  private int getKeyTTL(String key) {
    if (GNS.isInternalField(key)) {
      // Fields used by the GNS are cached forever (or at least until they get updated).
      return -1;
    } else {
      return timeToLive;
    }
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
    int keyTTL = getKeyTTL(key);
    //-1 means infinite TTL
    if (keyTTL == -1) {
      return true;
    } // 0 means TTL == 0
    else if (keyTTL == 0) {
      return false;
    } // else TTL is the value of field timeToLive.
    else {
      Long ts = timestampAddress.get(key);
      if (ts == null) {
        return false;
      }
      int secondPast = (int) ((System.currentTimeMillis() - ts) / 1000);
      return secondPast < keyTTL;
    }
  }

  public synchronized void updateCacheEntry(DNSPacket packet) {

    activeNameServer = new HashSet<Integer>(packet.getActiveNameServers());
    for (Entry<String, ResultValue> entry : packet.getRecordValue().entrySet()) {
      String fieldKey = entry.getKey();
      ResultValue fieldValue = entry.getValue();
      this.valuesMap.put(fieldKey, fieldValue);
      // set the timestamp for that field
      this.timestampAddress.put(fieldKey, System.currentTimeMillis());
    }
    timeToLive = packet.getTTL();
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
    return timeToLive;
  }

  public synchronized Set<Integer> getActiveNameServers() {
    return activeNameServer;
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
    entry.append(" TTLAddress:" + timeToLive);
    // NULL MEANS THE VALUE IS INVALID
    entry.append(" Value:" + (valuesMap == null ? "INVALID" : valuesMap.toString()));
    entry.append(" PrimaryNS:[");
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

    entry.append(" ActiveNS:[");
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
    entry.append(" TimestampAddress:" + timestampAddress);
    return entry.toString();
  }

  // WHAT IS THIS STUFF? CAN WE DELETE IT?
  public CacheEntry(TinyQuery packet) {
    this.name = packet.getName();
    this.timeToLive = 0; // this will depend on TTL sent by NS.
    this.valuesMap = new ValuesMap();
    this.valuesMap.put(NameRecordKey.EdgeRecord.getName(), new ResultValue(Arrays.asList(getRandomString())));
    //
    this.primaryNameServer = (HashSet<Integer>) packet.getPrimaries();

    this.activeNameServer = new HashSet<Integer>(packet.getActives());

    this.timestampAddress.put(NameRecordKey.EdgeRecord.getName(), System.currentTimeMillis());
  }

  public synchronized void updateCacheEntry(TinyQuery tinyQuery) {
    //Update the list of active name servers
    activeNameServer = new HashSet<Integer>(tinyQuery.getActives());
    //Check for updates to ttl values
    timeToLive = 0;
    //Update the timestamp of when data was last fetched and cached
    timestampAddress.put(NameRecordKey.EdgeRecord.getName(), System.currentTimeMillis());
  }

  /**
   * Returns a 8-character string.
   *
   * @return
   */
  public static String getRandomString() {
    Random rand = new Random();
    int intRange = 1000000;
    Integer x = intRange + rand.nextInt(1000000);
    return x.toString();
  }
}
