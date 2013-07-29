package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.packet.RequestActivesPacket;
import edu.umass.cs.gns.packet.TinyQuery;
import java.util.Arrays;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Represents the cache entry used at the local name server to cache DNS records
 *
 * @author Hardeep Uppal
 */
public class CacheEntry {

  /**
   * Often the GUID containing the key value pair.
   */
  private String name;
  /**
   * The key of the key value pair.
   */
//  private NameRecordKey recordKey;
//  /**
//   * Time interval (in seconds) that the resource record may be cached before it should be discarded
//   */
  private int timeToLive;
  /**
   * System timestamp when the entry was inserted into the cache *
   */
  private long timestampAddress;
  /**
   * The value of the key value pair.. NOTE: Value can be NULL meaning that the value is not valid.
   */
  private ValuesMap value;
  /**
   * A list of primary name servers for the name
   */
  private HashSet<Integer> primaryNameServer;
  /**
   * A list of Active Nameservers for the name.
   */
  private Set<Integer> activeNameServer;

  /**
   * ************************************************************
   * Constructs a new cache entry with the specified name information
   *
   * @param recordKey The key of the value key pair. For GNRS this will be EdgeRecord, CoreRecord or GroupRecord.
   * @param name A host/domain name
   * @param timeToLive Time to live in cache
   * @param value List of IP addresses for the name
   * @param primaryNameServer List of primary name servers for the name
   * @param activeNameServer List of active name servers for the name ***********************************************************
   */
  public CacheEntry(NameRecordKey recordKey, String name, int timeToLive, int ttlNameserver, ValuesMap value,
          HashSet<Integer> primaryNameServer,
          Set<Integer> activeNameServer) {

    //this.recordKey = recordKey;
    this.name = name;
    this.timeToLive = timeToLive;
    this.value = value;
    this.primaryNameServer = primaryNameServer;
    this.activeNameServer = new HashSet<Integer>(activeNameServer);
    this.timestampAddress = System.currentTimeMillis();
  }
  private static int DEFAULTTTLINSECONDS = 2;

  public CacheEntry(TinyQuery packet) {
    //this.recordKey = NameRecordKey.EdgeRecord;
    this.name = packet.getName();
    this.timeToLive = 0; // this will depend on TTL sent by NS.
    this.value = new ValuesMap();
    this.value.put("Frank", new QueryResultValue(Arrays.asList(getRandomString())));
    //

    this.primaryNameServer = (HashSet<Integer>) packet.getPrimaries();

    this.activeNameServer = new HashSet<Integer>(packet.getActives());
    //Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>(packet.activeNameServers.size(), 0.75f, 3));
//    for (Integer id : packet.activeNameServers) {
//      this.activeNameServer.add(id);
//    }

    this.timestampAddress = System.currentTimeMillis();
  }

  /**
   * Constructs a cache entry using data from a DNS packet
   *
   * @param packet DNS packet 
   */
  public CacheEntry(DNSPacket packet) {
    //this.recordKey = packet.getQrecordKey();
    this.name = packet.getQname();
    // this will depend on TTL sent by NS. UPDATE: NEVER LET IT BE -1 which means infinite
    this.timeToLive = packet.getTTL() == -1 ? DEFAULTTTLINSECONDS : packet.getTTL();
    this.value = new ValuesMap(packet.getRecordValue());
    //this.value.put(name, new QueryResultValue(packet.getRdata()));
    //this.ipAddressList = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>(packet.rdata.size(), 0.75f, 3));
    
    this.primaryNameServer = packet.getPrimaryNameServers();

    this.activeNameServer = new HashSet<Integer>(packet.getActiveNameServers());

    this.timestampAddress = System.currentTimeMillis();
  }

  public CacheEntry(RequestActivesPacket packet) {
    //this.recordKey = packet.getRecordKey();
    this.name = packet.getName();

    this.primaryNameServer = (HashSet<Integer>) LocalNameServer.getPrimaryNameServers(name//, recordKey
            );
    this.activeNameServer = packet.getActiveNameServers();
  }

  public synchronized int getTTL() {
    return timeToLive;
  }

  public synchronized long getTimestampValue() {
    return timestampAddress;
  }

  public synchronized boolean isValidValue() {
    return isValidAddress(false);
  }

  public synchronized Set<Integer> getActiveNameServers() {
    return activeNameServer;
  }

  public QueryResultValue getValue(NameRecordKey key) {
    return value.get(key.getName());
  }

  public ValuesMap getValue() {
    return value;
  }
  
  /**
   * ************************************************************
   * Returns true if the ttl associated with address has not expired in the cache. Returns false if the ttl has expired.
   * ***********************************************************
   */
  public synchronized boolean isValidAddress(boolean checkSize) {
    // NULL MEANS THE VALUE IS INVALID
    if (value == null || (checkSize && value.isEmpty())) {
      return false;
    } else {
      if (timeToLive == -1) {  //-1 means infinite TTL
        return true;
      } else if (timeToLive == 0) { // 0 means TTL == 0
        return false;
      } else { // else TTL is actual value.
        int secondPast = (int) ((System.currentTimeMillis() - timestampAddress) / 1000);
        return secondPast < timeToLive;
      }
    }
  }

  public synchronized void updateCacheEntry(DNSPacket packet) {
    activeNameServer = new HashSet<Integer>(packet.getActiveNameServers());
    value = packet.getRecordValue();
    //value = packet.getRdata();
    timeToLive = packet.getTTL();
    timestampAddress = System.currentTimeMillis();
  }

  public synchronized void updateCacheEntry(TinyQuery tinyQuery) {
    //Update the list of active name servers
    activeNameServer = new HashSet<Integer>(tinyQuery.getActives());
    //Check for updates in IP address
//    value = packet.rdata;
    //Check for updates to ttl values
    timeToLive = 0;
    //Update the timestamp of when data was last fetched and cached
    timestampAddress = System.currentTimeMillis();
  }

  public synchronized void updateCacheEntry(RequestActivesPacket packet) {
    activeNameServer = packet.getActiveNameServers();
  }

  public synchronized void updateCacheEntry(ConfirmUpdateLNSPacket packet) {
    // DEAL WITH THIS MESS LATER
//    String oldValueString;
//    if (value == null) {
//      value = new HashMap<String, QueryResultValue>();
//      oldValueString = "*INVALID*";
//    } else {
//      oldValueString = value.toString();
//    }
//    switch (packet.getType()) {
//      case CONFIRM_UPDATE_LNS:
//        switch (packet.getOperation()) {
//          case REPLACE_ALL:
//            value.clear();
//            value.addAll(packet.getUpdateValue());
//            break;
//          case REPLACESINGLETON:
//            value.clear();
//            if (!packet.getUpdateValue().isEmpty()) {
//              value.add(packet.getUpdateValue().get(0));
//            }
//            break;
//          case REMOVE:
//            value.removeAll(packet.getUpdateValue());
//            break;
//          case APPEND_WITH_DUPLICATION:
//            value.addAll(packet.getUpdateValue());
//            break;
//          case CLEAR:
//            value.clear();
//            break;
//          default:
//            // otherwise we punt and make the value not valid
//            value = null;
//            timeToLive = 0;
//            break;
//        }
//        break;
//      default:
//        GNRS.getLogger().warning("Bad packet type for updating cache:" + packet.getType().name());
//        break;
//    }
    // punt and make the value not valid
    value = null;
    timeToLive = 0;
    //Update the timestamp of when data was last fetched and cached
    timestampAddress = System.currentTimeMillis();
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Cache CLEARED for " + packet.getName());
      //GNRS.getLogger().fine("Cache update oldvalue = " + oldValueString + " new value = " + value);
    }
  }

  public synchronized void updateActiveNameServerCacheEntry(String name, //NameRecordKey recordKey, 
          Set<Integer> activeNameServers) {
    activeNameServer = new HashSet<Integer>(activeNameServers);
  }

  /**
   * ************************************************************
   * Write your own doc. ***********************************************************
   */
  public synchronized boolean isValidNameserver() {
    GNS.getLogger().fine("Active name servers in cache: " + activeNameServer);
    return activeNameServer != null && activeNameServer.size() != 0;
  }

  public synchronized int timeSinceAddressCached() {
    return (int) (System.currentTimeMillis() - timestampAddress);
  }

  public synchronized void invalidateActiveNameServer() {
    activeNameServer = null;
  }

  /**
   * ************************************************************
   * Returns a string representation of a cache entry ***********************************************************
   */
  @Override
  public synchronized String toString() {
    StringBuilder entry = new StringBuilder();

    entry.append("Name:" + getName());
    //entry.append(" Key: " + getRecordKey().getName());
    entry.append(" TTLAddress:" + timeToLive);
    // NULL MEANS THE VALUE IS INVALID
    entry.append(" Value:" + (value == null ? "INVALID" : value.toString()));
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

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

//  /**
//   * @return the recordKey
//   */
//  public NameRecordKey getRecordKey() {
//    return recordKey;
//  }
//
//  /**
//   * @param recordKey the recordKey to set
//   */
//  public void setRecordKey(NameRecordKey recordKey) {
//    this.recordKey = recordKey;
//  }
  /**
   * @return the primaryNameServer
   */
  public HashSet<Integer> getPrimaryNameServer() {
    return primaryNameServer;
  }

  /**
   * @param primaryNameServer the primaryNameServer to set
   */
  public void setPrimaryNameServer(HashSet<Integer> primaryNameServer) {
    this.primaryNameServer = primaryNameServer;
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
