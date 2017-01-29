
package edu.umass.cs.gnsserver.localnameserver;

import java.net.InetSocketAddress;
import java.util.Set;


public class CacheEntry implements Comparable<CacheEntry> {


  private String name;

  private int timeToLive = LocalNameServer.DEFAULT_VALUE_CACHE_TTL;

  private long valueTimestamp;

  private String value = null;

  private Set<InetSocketAddress> activeNameServers;

  private long activeNameServersTimestamp;


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


  public synchronized boolean isValidValue() {
    if (value == null) {
      return false;
    }
    return (System.currentTimeMillis() - valueTimestamp) < timeToLive;
  }


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


  @Override
  public synchronized String toString() {
    StringBuilder result = new StringBuilder();
    result.append("Name:").append(name);
    result.append("\nValue:").append(value);
    if (value != null) {
      result.append(" (age: ").append(System.currentTimeMillis() - valueTimestamp).append("ms)");
      if (!isValidValue()) {
        result.append("\n    ***Expired***");
      }
    }
    result.append("\nActives: ").append(activeNameServers);
    if (activeNameServers != null) {
      result.append("  (age: ").append(System.currentTimeMillis() - activeNameServersTimestamp).append("ms)");
      if (!isValidActives()) {
        result.append("\n    ***Expired***");
      }
    }
    result.append("\n    TTL:").append(timeToLive).append("ms");
    result.append("\n    Value Timestamp: ").append(valueTimestamp);
    result.append("\n    Actives Timestamp: ").append(activeNameServersTimestamp);

    return result.toString();
  }


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
