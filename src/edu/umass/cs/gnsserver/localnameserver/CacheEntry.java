/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.localnameserver;

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
   * Constructs a cache entry for a name.
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

  /**
   * Constructs a cache entry for a name from a list of active replicas.
   * 
   * @param name
   * @param activeNameServers
   */
  public CacheEntry(String name, Set<InetSocketAddress> activeNameServers) {
    this.name = name;
    this.activeNameServers = activeNameServers;
    this.activeNameServersTimestamp = System.currentTimeMillis();
    this.value = null;
    this.valueTimestamp = 0;
  }

  /**
   * Updates a cache entry with a new value.
   * 
   * @param value
   */
  public synchronized void updateCacheEntry(String value) {
    this.value = value;
    this.valueTimestamp = System.currentTimeMillis();
  }

  /**
   * Updates a cache entry with a new list of active replicas.
   * 
   * @param activeNameServers
   */
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

  /**
   * Returns the time since the cache value was updated.
   * 
   * @param key
   * @return a long
   */
  public synchronized long timeSinceValueCached(String key) {
    return (int) (System.currentTimeMillis() - valueTimestamp);
  }

  /**
   * Returns the time since the active replicas were updated.
   * 
   * @param key
   * @return a long
   */
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
    result.append("\nValue:" + value);
    if (value != null) {
      result.append(" (age: " + (System.currentTimeMillis() - valueTimestamp) + "ms)");
      if (!isValidValue()) {
        result.append("\n    ***Expired***");
      }
    }
    result.append("\nActives: " + activeNameServers);
    if (activeNameServers != null) {
      result.append("  (age: " + (System.currentTimeMillis() - activeNameServersTimestamp) + "ms)");
      if (!isValidActives()) {
        result.append("\n    ***Expired***");
      }
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
   * @return a positive if this record follows the argument; zero if the are equal
   */
  @Override
  public int compareTo(CacheEntry d) {
    return (this.name).compareTo(d.name);
  }

  /**
   * Returns the name.
   * 
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the TTL.
   * 
   * @return the ttl
   */
  public int getTimeToLive() {
    return timeToLive;
  }

  /**
   * Returns the timestamp of the value.
   * 
   * @return the timestamp
   */
  public long getValueTimestamp() {
    return valueTimestamp;
  }

  /**
   * Returns the cached value.
   * 
   * @return the value
   */
  public String getValue() {
    return value;
  }

  /**
   * Returns the set of active replicas.
   * 
   * @return the set of active replicas
   */
  public Set<InetSocketAddress> getActiveNameServers() {
    return activeNameServers;
  }

  /**
   * Returns the active replicas timestamp.
   * 
   * @return the active replicas timestamp
   */
  public long getActiveNameServersTimestamp() {
    return activeNameServersTimestamp;
  }

}
