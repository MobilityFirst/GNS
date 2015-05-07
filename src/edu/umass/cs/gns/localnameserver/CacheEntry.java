/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Represents the cache entry used at the local name server to cache records.
 */
public class CacheEntry implements Comparable<CacheEntry> {

  /**
   * The GUID.
   */
  private String name;
  /**
   * Time interval (in milliseconds) that the resource record may be cached before it should be discarded (default to zero)
   * Notice that we have ONE TTL for the entire cache entry which means one TTL for the whole record.
   */
  private int timeToLive = LocalNameServer.DEFAULT_VALUE_CACHE_TTL;
  /**
   * Time stamp (in milleseconds) when the value for each field was inserted into record.
   */
  private long valueTimestamp;
  /**
   * The value.
   */
  private String value = null;
  
  private Set<InetSocketAddress> activeNameServers;
  
  private long activeNameServersTimestamp;

  /**
   * Constructs a cache entry for a name from a list of replica controllers and a list of active replicas.
   *
   * @param name
   * @param value
   */
  public CacheEntry(String name, String value) {
    this.name = name;
    this.value = value;
    this.valueTimestamp = System.currentTimeMillis();
    this.activeNameServers = null;
    this.activeNameServersTimestamp = 0;
  }
  
  public CacheEntry(String name, Set<InetSocketAddress> activeNameServers) {
    this.name = name;
    this.activeNameServers = activeNameServers;
    this.activeNameServersTimestamp = System.currentTimeMillis();
    this.value = null;
    this.valueTimestamp = 0;
  }

  public synchronized void updateCacheEntry(String value) {
    this.value = value;
    this.valueTimestamp = System.currentTimeMillis();
  }
  
  public synchronized void updateCacheEntry(Set<InetSocketAddress> activeNameServers) {
    this.activeNameServers = activeNameServers;
    this.activeNameServersTimestamp = System.currentTimeMillis();
  }

  /**
   * Returns true if the contains the key and the ttl associated with key has not expired in the cache.
   * 
   * @return true or false value regarding cache being valid
   */
  public synchronized boolean isValidValue() {
    if (value == null) {
      return false;
    }
    return (System.currentTimeMillis() - valueTimestamp) < timeToLive;
  }
  
  /**
   * Returns true if the contains the key and the ttl associated with key has not expired in the cache.
   * 
   * @return true or false value regarding cache being valid
   */
  public synchronized boolean isValidActives() {
    if (activeNameServers == null) {
      return false;
    }
    return (System.currentTimeMillis() - activeNameServersTimestamp) < timeToLive;
  }

  public synchronized long timeSinceValueCached(String key) {
    return (int) (System.currentTimeMillis() - valueTimestamp);
  }
  
  public synchronized long timeSinceActivesCached(String key) {
    return (int) (System.currentTimeMillis() - activeNameServersTimestamp);
  }

  /**
   * Attempts to come up with a pretty string representation of the cache entry.
   *
   * @return string representation of the cache entry
   */
  @Override
  public synchronized String toString() {
    StringBuilder result = new StringBuilder();
    result.append("Name:" + name);
    result.append("Value:" + value);
    if (!isValidValue()) {
      result.append("\n    ***Expired***");
    }
    result.append("Actives: " + activeNameServers);
    if (!isValidActives()) {
      result.append("\n    ***Expired***");
    }
    result.append("\n    TTL:" + timeToLive + "ms");
    result.append("\n    Value Timestamp: " + valueTimestamp);
    result.append("\n    Actives Timestamp: " + activeNameServersTimestamp);
    
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
    return (this.name).compareTo(d.name);
  }

  public String getName() {
    return name;
  }

  public int getTimeToLive() {
    return timeToLive;
  }

  public long getValueTimestamp() {
    return valueTimestamp;
  }

  public String getValue() {
    return value;
  }

  public Set<InetSocketAddress> getActiveNameServers() {
    return activeNameServers;
  }

  public long getActiveNameServersTimestamp() {
    return activeNameServersTimestamp;
  }
  
  

}
