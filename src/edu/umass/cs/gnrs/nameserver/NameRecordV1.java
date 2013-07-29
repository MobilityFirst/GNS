package edu.umass.cs.gnrs.nameserver;

import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.main.StartNameServer;
import edu.umass.cs.gnrs.packet.QueryResultValue;
import edu.umass.cs.gnrs.packet.UpdateOperation;
import edu.umass.cs.gnrs.util.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * THIS IS ONLY STILL HERE SO THAT OLDER CASSANDRA CODE COMPILES.
 * IT WILL GO AWAY ONCE I UPDATE THAT.
 * @author westy
 */
public class NameRecordV1 {
  /* !!!! IF YOU ADD A FIELD TO THIS YOU NEED TO UPDATE THE JSON READ AND WRITE FUNCTIONS BELOW !!!! */

  /**
   * Name (host/domain) *
   */
  private String name;
  /**
   * The key of the value key pair. For GNRS this will be EdgeRecord, CoreRecord or GroupRecord. *
   */
  private NameRecordKey recordKey;
  /**
   * TTL: time to live IN SECONDS*
   */
  private int timeToLive = 0; // 0 means TTL - 0 (no caching), -1 means the record will never expire if it gets into a cache
  /**
   * List of values of this record *
   */
  private QueryResultValue valuesList;
  /**
   * List of values from old active name servers.
   */
  private QueryResultValue oldValuesList;
  /**
   * Flag that indicates whether the name server is a primary replica of this record *
   */
  private boolean primaryReplica;
  /**
   * Set of primary nameservers *
   */
  private HashSet<Integer> primaryNameservers;
  /**
   * Set of active nameservers most recently computed.
   */
  private Set<Integer> activeNameservers;
  /**
   * Previous set of active nameservers
   */
  private Set<Integer> oldActiveNameservers;
  /**
   * Whether previous set of active name server is active
   */
  private boolean oldActiveRunning = false;
  /**
   * Whether current set of active name servers is stopped.
   */
  private boolean activeRunning = false;
  /**
   * Paxos instance ID of the old active name server set
   */
  private String oldActivePaxosID;
  /**
   * Paxos instance ID of the new active name server set.
   */
  private String activePaxosID;
  /**
   * At primary name servers, this field means that the name record is going to be removed once current set of active is deleted.
   */
  private boolean markedForRemoval = false;
  /**
   * Read write rates reported from name server.
   */
  private ConcurrentMap<Integer, StatsInfo> nameServerStatsMap;
  /**
   * Number of lookups *
   */
  private int totalLookupRequest;
  /**
   * Number of updates *
   */
  private int totalUpdateRequest;
  private int totalAggregateReadFrequency;
  private int totalAggregateWriteFrequency;
  private int previousAggregateReadFrequency;
  private int previousAggregateWriteFrequency;
  /**
   * List of name server voted as a replica for this record. <Key = NameserverID, Value=#Votes}
   */
  private ConcurrentMap<Integer, Integer> nameServerVotesMap;
  /**
   * Calculates moving average of inter-arrival time between updates *
   */
  private MovingAverage movingAverageUpdates;
  /**
   * Calculates moving average of inter-arrival time between lookups *
   */
  private MovingAverage movingAverageLookups;
  /**
   * System timestamp of the last updated received *
   */
  private long lastUpdateTimestamp;
  /**
   * System timestamp of the last lookup received *
   */
  private long lastLookupTimestamp;
  /**
   * Update Rate: average inter-arrival time (in seconds) between updates *
   */
  private double updateRate;
  /**
   * Lookup Rate: average inter-arrival time (in seconds) between lookups *
   */
  private double lookupRate;
  private MovingAverage movingAvgAggregateLookupFrequency = null;
  private MovingAverage movingAvgAggregateUpdateFrequency = null;
  private ConcurrentMap<Integer, Integer> lnsRequestsCount;

  /* !!!! IF YOU ADD A FIELD TO THIS YOU NEED TO UPDATE THE JSON READ AND WRITE FUNCTIONS BELOW !!!! */
  
  private ReentrantReadWriteLock lock;
  /**
   * ***********************************************************
   * Creates a new record for the given name and primary replicas.
   *
   * @param name The name (host/domain) //
   * @param primary List of primary replicas for this record
   * @throws Exception **********************************************************
   */
  public NameRecordV1(String name, NameRecordKey recordKey) throws Exception {
    this.recordKey = recordKey;
    this.name = name;
    //Initialize the entry in the map
    initialize(HashFunction.getPrimaryReplicas(name), null);
  }

  /**
   * ***********************************************************
   * Creates a new record for the given name, address and primary replicas.
   *
   * @param name The name (host/domain)
   * @param value List strings of name's value //
   * @param primary List of primary replicas for this record
   * @throws UnsupportedEncodingException
   * @throws NoSuchAlgorithmException
   * @throws UnknownHostException
   * @throws Exception **********************************************************
   */
  public NameRecordV1(String name, NameRecordKey recordKey, ArrayList<String> value)
          throws UnknownHostException {
    this.recordKey = recordKey;
    this.name = name;

    //Initialize the entry in the map
    initialize(HashFunction.getPrimaryReplicas(name), value);
  }

  /**
   * ***********************************************************
   * Creates a new record from a ReplicateRecordPacket (packet containing name record).
   *
   * @param packet Packet containing information about the record **********************************************************
   */
//  public NameRecordV1(ReplicateRecordPacket packet) {
//    this.recordKey = packet.getRecordKey();
//    this.name = packet.getName();
//    this.timeToLive = packet.getTimeToLive();
//
//    this.valuesList = new QueryResultValue();
//    //    this.addressList = Collections.newSetFromMap(
//    //            new ConcurrentHashMap<String, Boolean>(1, 0.75f, 5));
//    for (String addr : packet.getValuesList()) {
//      this.valuesList.add(addr);
//    }
//
//    this.primaryNameservers = packet.getPrimaryNameServers();
//
//    this.primaryReplica = primaryNameservers.contains(NameServer.nodeID);
//    this.activeNameservers = Collections.newSetFromMap(
//            new ConcurrentHashMap<Integer, Boolean>(6, 0.75f, 8));
//    this.activeNameservers.addAll(packet.getActiveNameServers());
//    //    for (Integer id : ) {
//    //      this.activeNameservers.add(id);
//    //    }
//
//    this.totalLookupRequest = 0;
//    this.totalUpdateRequest = 0;
//    this.totalAggregateReadFrequency = 0;
//    this.totalAggregateWriteFrequency = 0;
//    this.previousAggregateReadFrequency = 0;
//    this.previousAggregateWriteFrequency = 0;
//
//    this.nameServerVotesMap = (primaryReplica)
//            ? new ConcurrentHashMap<Integer, Integer>(10, 0.75f, 3) : null;
//    this.nameServerStatsMap = (primaryReplica)
//            ? new ConcurrentHashMap<Integer, StatsInfo>(10, 0.75f, 3) : null;
//
//    this.movingAverageUpdates = new MovingAverage(StartNameServer.movingAverageWindowSize);
//    this.movingAverageLookups = new MovingAverage(StartNameServer.movingAverageWindowSize);
//
//    if (primaryReplica) {
//      this.movingAvgAggregateLookupFrequency = new MovingAverage(StartNameServer.movingAverageWindowSize);
//      this.movingAvgAggregateUpdateFrequency = new MovingAverage(StartNameServer.movingAverageWindowSize);
//    }
//
//  }

  /**
   * ***********************************************************
   * This method initializes a new record.
   *
   * @param primary List of primary replicas for this record
   * @param values String representation of name's address
   * @throws UnknownHostException
   * @throws NoSuchAlgorithmException
   * @throws UnsupportedEncodingException **********************************************************
   */
  private void initialize(Set<Integer> primary, ArrayList<String> values)
          throws UnknownHostException {
    this.valuesList = new QueryResultValue();
    this.oldValuesList = new QueryResultValue();
    //    addressList = Collections.newSetFromMap(
    //            new ConcurrentHashMap<String, Boolean>(1, 0.75f, 5));
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Constructor Primaries: " + primary);
    }

    setPrimaryNameservers((HashSet<Integer>) primary);
    setPrimaryReplica(primary.contains(NameServer.nodeID));


    //Lookup the address of the name using the legacy DNS if address is not specified
    if (values == null) {
      if (GNS.USELEGACYDNS) {
        //TODO: Get multiple IP addresses of the domain name
        InetAddress addr = InetAddress.getByName(getName());
        String ip = addr.toString().split("/")[1];
        this.valuesList.add(ip);
      } else {
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("!!IGNORING LEGACY DNS USE!!");
        }
      }
    } else {
      this.valuesList.addAll(values);
      this.oldValuesList.addAll(values);
    }

    nameServerStatsMap = (isPrimaryReplica()) ? new ConcurrentHashMap<Integer, StatsInfo>(10, 0.75f, 3) : null;

//		//Initialize primary name servers as the active name servers
//		for (int id : primary) {
//			activeNameservers.add(id);
//			if (isPrimaryReplica()) {
//				nameServerStatsMap.put(id, new StatsInfo(0, 0));
//			}
//		}

    oldActiveNameservers = new HashSet<Integer>(primaryNameservers);
    activeNameservers = getInitialActives(primaryNameservers, StartNameServer.minReplica, name);
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Name Record INITIAL ACTIVES ARE: " + activeNameservers);
    }
    oldActivePaxosID = name + "-" + recordKey.getName() + "-1"; // initialized uniformly among primaries
    activePaxosID = name + "-" + recordKey.getName() + "-2";

    totalLookupRequest = 0;
    totalUpdateRequest = 0;
    totalAggregateReadFrequency = 0;
    totalAggregateWriteFrequency = 0;
    previousAggregateReadFrequency = 0;
    previousAggregateWriteFrequency = 0;
    nameServerVotesMap = (isPrimaryReplica()) ? new ConcurrentHashMap<Integer, Integer>(10, 0.75f, 3) : null;
    movingAverageUpdates = new MovingAverage(StartNameServer.movingAverageWindowSize);
    movingAverageLookups = new MovingAverage(StartNameServer.movingAverageWindowSize);

    if (isPrimaryReplica()) {
      this.movingAvgAggregateLookupFrequency = new MovingAverage(StartNameServer.movingAverageWindowSize);
      this.movingAvgAggregateUpdateFrequency = new MovingAverage(StartNameServer.movingAverageWindowSize);
    }
  }

  private static Set<Integer> getInitialActives(Set<Integer> primaryNameservers, int count, String name) {
    // choose three actives which are different from primaries
    Set<Integer> newActives = new HashSet<Integer>();
    Random r = new Random(name.hashCode());

    for (int j = 0; j < count; j++) {
      while (true) {
        int id = r.nextInt(ConfigFileInfo.getNumberOfNameServers());
        //            int active = (j + id)% ConfigFileInfo.getNumberOfNameServers();
        if (!primaryNameservers.contains(id) && !newActives.contains(id)) {
          GNS.getLogger().fine("ID " + id);
          newActives.add(id);
          break;
        }
      }
    }
    if (newActives.size() < count) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" ERROR: initial actives < " + count + " Initial Actives: " + newActives);
      }
    }
    return newActives;
  }

  /**
   * ***********************************************************
   * Returns the number of active nameservers for this record. **********************************************************
   */
  public synchronized int numActiveNameservers() { // synchronized
    if (activeNameservers == null) {
      return 0;
    }
    return activeNameservers.size();
  }

  /**
   * ***********************************************************
   * Returns true if the name record contains a primary name server with the given id. False otherwise.
   *
   * @param id Primary name server id **********************************************************
   */
  public synchronized boolean containsPrimaryNameserver(int id) { // synchronized
    return getPrimaryNameservers().contains(id);
  }

  /**
   * ***********************************************************
   * Returns true if the name record contains an active name server with the given id. False otherwise.
   *
   * @param id Active name server id **********************************************************
   */
  public synchronized boolean containsActiveNameServer(int id) { // synchronized
    if (activeNameservers == null) {
//			if (StartNameServer.debugMode) GNRS.getLogger().fine("Active name servers is null.");
      return false;
    }
//		if (StartNameServer.debugMode) GNRS.getLogger().fine("Name = "+ name + " Active name servers = " + activeNameservers + " Primary name servers = " + primaryNameservers);
    return activeNameservers.contains(id);
  }

  /**
   * ***********************************************************
   * Update address. * //
   *
   * @param values Updated address
   * @parem operation The update operation to perform
   * @return True if address list is updated, false otherwise. **********************************************************
   */
  public boolean updateValuesList(ArrayList<String> newValues, ArrayList<String> oldValues, UpdateOperation operation) { // synchronized
    switch (operation) {
      case REPLACE_ALL_OR_CREATE:
      case REPLACE_ALL:
        valuesList.clear();
        valuesList.addAll(newValues);
        return true;
      case APPEND_WITH_DUPLICATION:
        return valuesList.addAll(newValues);
      case APPEND_OR_CREATE:
      case APPEND:
        // this is ugly
        Set singles = new HashSet(valuesList);
        singles.addAll(newValues);
        valuesList.clear();
        valuesList.addAll(singles);
        return true;
      case REMOVE:
        return valuesList.removeAll(newValues);
      case REPLACESINGLETON:
        valuesList.clear();
        if (!newValues.isEmpty()) {
          valuesList.add(newValues.get(0));
        }
        return true;
      case SUBSTITUTE:
        boolean changed = false;
        for (Iterator<String> oldIter = oldValues.iterator(), newIter = newValues.iterator();
                oldIter.hasNext() && newIter.hasNext();) {
          String oldValue = oldIter.next();
          String newValue = newIter.next();
          if (Collections.replaceAll(valuesList, oldValue, newValue)) {
            changed = true;
          }
        }
        return changed;
      case CLEAR:
        valuesList.clear();
        return true;
    }
    return false;
  }

  /**
   * ***********************************************************
   * Increments the number of lookups by 1. **********************************************************
   */
  public synchronized void incrementLookupRequest() { // synchronized
    totalLookupRequest += 1;
  }

  /**
   * ***********************************************************
   * Increments the number of updates by 1. **********************************************************
   */
  public synchronized void incrementUpdateRequest() { // synchronized
    totalUpdateRequest += 1;
  }

  /**
   *
   * @param newActiveNameServers
   */
  public synchronized void replaceActiveNameServers(Set<Integer> newActiveNameServers) {
    this.activeNameservers = newActiveNameServers;
  }

  /**
   * ***********************************************************
   * Adds an active name server.
   *
   * @return Returns true if the replica is added successfully, false if the replica is already an active replica
   * **********************************************************
   */
  public synchronized boolean addActiveNameserver(int id) { //synchronized
    return activeNameservers.add(id);
  }

  /**
   * ***********************************************************
   * Removes an active name server.
   *
   * @param id Active name server id
   * @return Returns true, if the active name server id is removed, false otherwise.
   * **********************************************************
   */
  public synchronized boolean removeActiveNameserver(int id) { // synchronized
    StatsInfo info = (nameServerStatsMap != null) ? nameServerStatsMap.remove(id) : null;
    if (info != null) {
      if (previousAggregateReadFrequency != 0) {
        previousAggregateReadFrequency -= info.read;
      }
      if (previousAggregateWriteFrequency != 0) {
        previousAggregateWriteFrequency -= info.write;
      }
    }
    return activeNameservers.remove(id);
  }

  public synchronized void addNameServerStats(int id, int readFrequency, int writeFrequency) { // synchronized
    if (nameServerStatsMap != null) {
      nameServerStatsMap.put(id, new StatsInfo(readFrequency, writeFrequency));
    }
  }

  /**
   * ***********************************************************
   * Adds vote to the name server for replica selection.
   *
   * @param id Name server id receiving the vote **********************************************************
   */
  public synchronized void addReplicaSelectionVote(int id, int vote) { //synchronized
    if (nameServerVotesMap.containsKey(id)) {
      int votes = nameServerVotesMap.get(id) + vote;
      nameServerVotesMap.put(id, votes);
    } else {
      nameServerVotesMap.put(id, vote);
    }
  }

  public synchronized void addLNSRequestCount(String name, int lnsId, int vote) { //synchronized
    //      NameRecord nameRecord = recordMap.get( name );
    addLNSRequestCount(lnsId, vote);
    //      if( nameRecord != null && nameRecord.isPrimaryReplica )
    //          nameRecord.addLNSRequestCount(lnsId, vote);
  }

  /**
   * ***********************************************************
   * Returns a set of highest voted name servers ids.
   *
   * @param numReplica Number of name servers to be selected. **********************************************************
   */
  public synchronized Set<Integer> getHighestVotedReplicaID(int numReplica) { //synchronized
    Set<Integer> replicas = new HashSet<Integer>();

    for (int i = 1; i <= numReplica; i++) {
      int highestVotes = -1;
      int highestVotedReplicaID = -1;

      for (Map.Entry<Integer, Integer> entry : nameServerVotesMap.entrySet()) {
        int nameServerId = entry.getKey();
        int votes = entry.getValue();
        //Skip name server that are unreachable
        // from main branch 269
        if (ConfigFileInfo.getPingLatency(nameServerId) == -1
                || getPrimaryNameservers().contains(nameServerId)) {
          continue;
        }
        if (!replicas.contains(nameServerId)
                && votes > highestVotes) {
          highestVotes = votes;
          highestVotedReplicaID = nameServerId;
        }
      }
      //Checks whether a new replica was available to be added
      if (highestVotedReplicaID != -1) {
        replicas.add(highestVotedReplicaID);
      } else {
        break;
      }

      if (replicas.size() == nameServerVotesMap.size()) {
        break;
      }
    }
    return replicas;
  }

  /**
   * ***********************************************************
   * Returns a copy of the active name servers set. **********************************************************
   */
  public synchronized Set<Integer> copyActiveNameServers() { //synchronized
    Set<Integer> set = new HashSet<Integer>();
    for (int id : activeNameservers) {
      set.add(id);
    }
    return set;
  }

  /**
   * ***********************************************************
   * Returns a copy of all (primary and active) name servers set. **********************************************************
   */
  public synchronized Set<Integer> copyAllNameServers() { //synchronized
    Set<Integer> set = new HashSet<Integer>();
//		if (StartNameServer.debugMode) GNRS.getLogger().fine("Active");
    for (int id : activeNameservers) {
//			if (StartNameServer.debugMode) GNRS.getLogger().fine("ID " + id);
      set.add(id);
    }

//		if (StartNameServer.debugMode) GNRS.getLogger().fine("Primary: " + getPrimaryNameservers());
    for (int id : getPrimaryNameservers()) {
//			if (StartNameServer.debugMode) GNRS.getLogger().fine("ID " + id);
      set.add(id);
    }

    return set;
  }

  /**
   * ***********************************************************
   * Returns a copy of all (primary and active) name servers set. **********************************************************
   */
  public synchronized ArrayList<Integer> copyAllNameServersArrayList() { // synchronized
    ArrayList<Integer> nameservers = new ArrayList<Integer>();
    for (int id : activeNameservers) {
      nameservers.add(id);
    }
    for (int id : getPrimaryNameservers()) {
      boolean x = false;
      for (int y : nameservers) {
        if (y == id) {
          x = true;
        }
      }
      if (x == false) {
        nameservers.add(id);
      }
    }
    return nameservers;
  }

  /**
   * ***********************************************************
   * Returns a set contains primary and active name servers **********************************************************
   */
  public synchronized Set<Integer> allNameServer() { // synchronized
    Set<Integer> nameServers = new HashSet<Integer>(activeNameservers);
    for (int id : getPrimaryNameservers()) {
      nameServers.add(id);
    }
    return nameServers;
  }

  /**
   * ***********************************************************
   * Returns a total count on the number of lookup at this nameserver. **********************************************************
   */
  public synchronized int getTotalReadFrequency() { //synchronized
    return totalLookupRequest;
  }

  /**
   * ***********************************************************
   * Returns a total count on the number of updates at this nameserver **********************************************************
   */
  public synchronized int getTotalWriteFrequency() { // synchronized
    return totalUpdateRequest;
  }

  /**
   * ***********************************************************
   * Returns the total number of lookup request across all active name servers
   *
   * @return **********************************************************
   */
  public synchronized double getReadStats() { // synchronized
    totalAggregateReadFrequency = totalLookupRequest;
    for (StatsInfo info : nameServerStatsMap.values()) {
      totalAggregateReadFrequency += info.read;
    }

    int currentReadFrequency = totalAggregateReadFrequency - previousAggregateReadFrequency;

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("TotalAggregateRead: " + totalAggregateReadFrequency
              + " Previous: " + previousAggregateReadFrequency
              + " Current: " + currentReadFrequency);
    }
    previousAggregateReadFrequency += currentReadFrequency;
    movingAvgAggregateLookupFrequency.add(currentReadFrequency);

    //      if (currentReadFrequency > 0) {
    //
    //      }
    return movingAvgAggregateLookupFrequency.getAverage();
    //      return Util.round();
  }

  /**
   * ***********************************************************
   * Returns the total number of lookup request across all active name servers
   *
   * @return **********************************************************
   */
  public synchronized double getReadStats_Paxos() { // synchronized
    totalAggregateReadFrequency = 0;
    for (StatsInfo info : nameServerStatsMap.values()) {
      totalAggregateReadFrequency += info.read;
    }

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("TotalAggregateRead: " + totalAggregateReadFrequency
              + " Previous: " + previousAggregateReadFrequency
              + " Current: " + (totalAggregateReadFrequency - previousAggregateReadFrequency));
    }
    movingAvgAggregateLookupFrequency.add(totalAggregateReadFrequency - previousAggregateReadFrequency);
    previousAggregateReadFrequency = totalAggregateReadFrequency;


    //      if (currentReadFrequency > 0) {
    //
    //      }
    return movingAvgAggregateLookupFrequency.getAverage();
    //      return Util.round();
  }

  /**
   * ***********************************************************
   * Returns the total number of updates request across all active name servers
   *
   * @return **********************************************************
   */
  public synchronized double getWriteStats() { // synchronized
    totalAggregateWriteFrequency = totalUpdateRequest;
    for (StatsInfo info : nameServerStatsMap.values()) {
      totalAggregateWriteFrequency += info.write;
    }

    int currentWriteFrequency = totalAggregateWriteFrequency - previousAggregateWriteFrequency;
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("TotalAggregateWrite: " + totalAggregateWriteFrequency
              + " Previous: " + previousAggregateWriteFrequency
              + " Current: " + currentWriteFrequency);
    }

    previousAggregateWriteFrequency += currentWriteFrequency;
    movingAvgAggregateUpdateFrequency.add(currentWriteFrequency);

    //      if (currentWriteFrequency > 0) {
    //
    //      }
    return movingAvgAggregateUpdateFrequency.getAverage();
    //      return Util.round();
  }

  /**
   * ***********************************************************
   * Returns the total number of updates request across all active name servers
   *
   * @return **********************************************************
   */
  public synchronized double getWriteStats_Paxos() { // synchronized
    totalAggregateWriteFrequency = 0;
    for (StatsInfo info : nameServerStatsMap.values()) {
      totalAggregateWriteFrequency += info.write;
    }

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("TotalAggregateWrite: " + totalAggregateWriteFrequency
              + " Previous: " + previousAggregateWriteFrequency
              + " Current: " + (totalAggregateWriteFrequency - previousAggregateWriteFrequency));
    }
    movingAvgAggregateUpdateFrequency.add(totalAggregateWriteFrequency - previousAggregateWriteFrequency);
    previousAggregateWriteFrequency = totalAggregateWriteFrequency;

    return movingAvgAggregateUpdateFrequency.getAverage();
    //      return Util.round();
  }

  public synchronized int getReadAvg() { // synchronized
    return Util.round(movingAvgAggregateLookupFrequency.getAverage());
  }

  public synchronized int getWriteAvg() { // synchronized
    return Util.round(movingAvgAggregateUpdateFrequency.getAverage());
  }

  /**
   * ***********************************************************
   * Adds a new inter-arrival time between this and the last update.<br/> This method re-calculates the updateRate and ttl value
   * based on the moving average of inter-arrival time.</br>
   *
   * @param updateTimestamp System timestamp of the most recent update.
   * @return Returns the current ttl value of this name record **********************************************************
   */
  public synchronized int addInterarrivalTimeUpdate(long updateTimestamp) { // synchronized
    if (totalUpdateRequest == 1) {
      lastUpdateTimestamp = updateTimestamp;
    } else {
      int interarrivalTime = (int) (updateTimestamp - lastUpdateTimestamp);
      movingAverageUpdates.add(interarrivalTime);
      //calculate the update rate
      updateRate = movingAverageUpdates.getAverage() / 1000;
      //calculate the ttl value
      timeToLive = Util.round(updateRate * StartNameServer.ttlConstant);
      lastUpdateTimestamp = updateTimestamp;
    }

    return timeToLive;
  }

  /**
   * ***********************************************************
   * Adds a new inter-arrival time between this and the last lookup.<br/> This method re-calculates the lookupRate based on the
   * moving average of inter-arrival time.</br>
   *
   * @param lookupTimestamp System timestamp of the most recent lookup.
   * @return Returns the current lookupRate of this record **********************************************************
   */
  public synchronized double addInterarrivalTimeLookup(long lookupTimestamp) { // synchronized
    if (totalLookupRequest == 1) {
      lastLookupTimestamp = lookupTimestamp;
    } else {
      int interarrivalTime = (int) (lookupTimestamp - lastLookupTimestamp);
      movingAverageLookups.add(interarrivalTime);
      //calculate the lookup rate
      lookupRate = movingAverageLookups.getAverage() / 1000;
      lastLookupTimestamp = lookupTimestamp;
    }

    return lookupRate;
  }

  /**
   * ***********************************************************
   * Returns the primary name server with the smallest id. **********************************************************
   */
  public synchronized int getSmallestIdPrimary() { // synchronized

    int smallestId = Integer.MAX_VALUE;
    for (int id : getPrimaryNameservers()) {
      if (id < smallestId) {
        smallestId = id;
      }
    }
    return smallestId;

  }

  /**
   * Return a random, but deterministically chosen primary server for this name. This name server will handle updates, voting,
   * aggregation, replication for this name.
   *
   * @return primaryID a random, but deterministically chosen primary server for this name.
   */
  public synchronized int getMainPrimary() { // synchronized
    Random random = new Random(new Integer(name.hashCode()));
    int count = random.nextInt(getPrimaryNameservers().size());
    int i = 0;
    for (int id : getPrimaryNameservers()) {
      if (i == count) {
        return id;
      }
      i++;
    }
    return -1;

  }
  public final static String NAME = "name";
  public final static String RECORDKEY = "recordkey";
  private final static String TIMETOLIVE = "timeToLive";
  private final static String VALUESLIST = "valuesList";
  private final static String OLDVALUESLIST = "oldValuesList";
  private final static String PRIMARY_NAMESERVERS = "primary";
  private final static String ACTIVE_NAMESERVERS = "active";
  private final static String OLD_ACTIVE_NAMESERVERS = "oldactive";
  private final static String ACTIVE_NAMESERVERS_RUNNING = "activeRunning";
  private final static String OLD_ACTIVE_NAMESERVERS_RUNNING = "oldActiveRunning";
  private final static String ACTIVE_PAXOS_ID = "activePaxosID";
  private final static String OLD_ACTIVE_PAXOS_ID = "oldActivePaxosID";
  private final static String MARKED_FOR_REMOVAL = "markedForRemoval";
  private final static String PRIMARY_REPLICA = "primaryReplica";
  private final static String NAMESERVER_VOTES_MAP = "nameserverVotesMap";
  private final static String TOTALLOOKUPREQUEST = "totalLookupRequest";
  private final static String TOTALUPDATEREQUEST = "totalUpdateRequest";
  private final static String NAMESERVERSTATSMAP = "nameServerStatsMap";
  private final static String TOTALAGGREGATEREADFREQUENCY = "totalAggregateReadFrequency";
  private final static String TOTALAGGREGATEWRITEFREQUENCY = "totalAggregateWriteFrequency";
  private final static String PREVIOUSAGGREAGATEREADFREQUENCY = "previousAggregateReadFrequency";
  private final static String PREVIOUSAGGREAGATEWRITEFREQUENCY = "previousAggregateWriteFrequency";
  private final static String MOVINGAGGREGATELOOKUPFREQUENCY = "movingAvgAggregateLookupFrequency";
  private final static String MOVINGAGGREGATEUPDATEFREQUENCY = "movingAvgAggregateUpdateFrequency";
  private final static String MOVINGAVERAGELOOKUPS = "movingAverageLookups";
  private final static String MOVINGAVERAGEUPDATES = " movingAverageUpdates";
  private final static String LOOKUPRATE = "lookupRate";
  private final static String UPDATERATE = "updateRate";
  private final static String LASTLOOKUPTIMESTAMP = "lastLookupTimestamp";
  private final static String LASTUPDATETIMESTAMP = "lastUpdateTimestamp";
  private final static String LNSREQUESTSCOUNT = "lnsRequestsCount";

  public NameRecordV1(JSONObject json) throws JSONException {
    this.name = json.getString(NAME);
    this.recordKey = NameRecordKey.valueOf(json.getString(RECORDKEY));
    this.timeToLive = json.getInt(TIMETOLIVE);
    this.valuesList = new QueryResultValue(JSONUtils.JSONArrayToArrayList(json.getJSONArray(VALUESLIST)));
    this.oldValuesList = new QueryResultValue(JSONUtils.JSONArrayToArrayList(json.getJSONArray(OLDVALUESLIST)));

    this.primaryNameservers = (HashSet<Integer>) JSONUtils.JSONArrayToSetInteger(json.getJSONArray(PRIMARY_NAMESERVERS));
    this.activeNameservers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(ACTIVE_NAMESERVERS));
    this.primaryReplica = json.getBoolean(PRIMARY_REPLICA);
    // New fields
    this.oldActiveNameservers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(OLD_ACTIVE_NAMESERVERS));
    this.oldActiveRunning = json.getBoolean(OLD_ACTIVE_NAMESERVERS_RUNNING);
    this.activeRunning = json.getBoolean(ACTIVE_NAMESERVERS_RUNNING);
    this.oldActivePaxosID = json.getString(OLD_ACTIVE_PAXOS_ID);
    if (!json.has(ACTIVE_PAXOS_ID)) {
      this.activePaxosID = null;
    } else {
      this.activePaxosID = json.getString(ACTIVE_PAXOS_ID);
    }
    this.markedForRemoval = json.getBoolean(MARKED_FOR_REMOVAL);

    this.nameServerVotesMap = toIntegerMap(json.getJSONObject(NAMESERVER_VOTES_MAP));
    this.lnsRequestsCount = toIntegerMap(json.getJSONObject(LNSREQUESTSCOUNT));
    this.totalLookupRequest = json.getInt(TOTALLOOKUPREQUEST);
    this.totalUpdateRequest = json.getInt(TOTALUPDATEREQUEST);
    this.nameServerStatsMap = toStatsMap(json.getJSONObject(NAMESERVERSTATSMAP));

    this.totalAggregateReadFrequency = json.getInt(TOTALAGGREGATEREADFREQUENCY);
    this.totalAggregateWriteFrequency = json.getInt(TOTALAGGREGATEWRITEFREQUENCY);
    this.previousAggregateReadFrequency = json.getInt(PREVIOUSAGGREAGATEREADFREQUENCY);
    this.previousAggregateWriteFrequency = json.getInt(PREVIOUSAGGREAGATEWRITEFREQUENCY);

    this.lookupRate = json.getDouble(LOOKUPRATE);
    this.updateRate = json.getDouble(UPDATERATE);

    this.lastLookupTimestamp = json.getLong(LASTLOOKUPTIMESTAMP);
    this.lastUpdateTimestamp = json.getLong(LASTUPDATETIMESTAMP);

    this.movingAverageUpdates = new MovingAverage(json.getJSONArray(MOVINGAVERAGEUPDATES), StartNameServer.movingAverageWindowSize);
    this.movingAverageLookups = new MovingAverage(json.getJSONArray(MOVINGAVERAGELOOKUPS), StartNameServer.movingAverageWindowSize);

    if (json.has(MOVINGAGGREGATELOOKUPFREQUENCY)) {
      this.movingAvgAggregateLookupFrequency = new MovingAverage(json.getJSONArray(MOVINGAGGREGATELOOKUPFREQUENCY), StartNameServer.movingAverageWindowSize);
    }
    if (json.has(MOVINGAGGREGATEUPDATEFREQUENCY)) {
      this.movingAvgAggregateUpdateFrequency = new MovingAverage(json.getJSONArray(MOVINGAGGREGATEUPDATEFREQUENCY), StartNameServer.movingAverageWindowSize);
    }


  }

  public synchronized JSONObject toJSONObject() throws JSONException { // synchronized
    JSONObject json = new JSONObject();
    json.put(NAME, getName());
    json.put(RECORDKEY, getRecordKey().getName());
    json.put(TIMETOLIVE, timeToLive);
    json.put(VALUESLIST, new JSONArray(valuesList));
    json.put(OLDVALUESLIST, new JSONArray(oldValuesList));

    json.put(PRIMARY_NAMESERVERS, new JSONArray(getPrimaryNameservers()));
    json.put(ACTIVE_NAMESERVERS, new JSONArray(activeNameservers));
    json.put(PRIMARY_REPLICA, isPrimaryReplica());

    // new fields
    json.put(OLD_ACTIVE_NAMESERVERS, new JSONArray(oldActiveNameservers));
    json.put(OLD_ACTIVE_NAMESERVERS_RUNNING, oldActiveRunning);
    json.put(ACTIVE_NAMESERVERS_RUNNING, activeRunning);
    json.put(ACTIVE_PAXOS_ID, activePaxosID);
    json.put(OLD_ACTIVE_PAXOS_ID, oldActivePaxosID);
    json.put(MARKED_FOR_REMOVAL, markedForRemoval);
    //		json.put(key, value)

    json.put(NAMESERVER_VOTES_MAP, nameServerVotesMap);
    json.put(LNSREQUESTSCOUNT, lnsRequestsCount);
    json.put(TOTALLOOKUPREQUEST, totalLookupRequest);
    json.put(TOTALUPDATEREQUEST, totalUpdateRequest);
    json.put(NAMESERVERSTATSMAP, statsMapToJSONObject(nameServerStatsMap));

    json.put(TOTALAGGREGATEREADFREQUENCY, totalAggregateReadFrequency);
    json.put(TOTALAGGREGATEWRITEFREQUENCY, totalAggregateWriteFrequency);
    json.put(PREVIOUSAGGREAGATEREADFREQUENCY, previousAggregateReadFrequency);
    json.put(PREVIOUSAGGREAGATEWRITEFREQUENCY, previousAggregateWriteFrequency);

    json.put(LOOKUPRATE, lookupRate);
    json.put(UPDATERATE, updateRate);

    json.put(LASTLOOKUPTIMESTAMP, lastLookupTimestamp);
    json.put(LASTUPDATETIMESTAMP, lastUpdateTimestamp);

    if (movingAvgAggregateLookupFrequency != null) {
      json.put(MOVINGAGGREGATELOOKUPFREQUENCY, movingAvgAggregateLookupFrequency.toJSONArray());
    }
    if (movingAvgAggregateUpdateFrequency != null) {
      json.put(MOVINGAGGREGATEUPDATEFREQUENCY, movingAvgAggregateUpdateFrequency.toJSONArray());
    }

    json.put(MOVINGAVERAGELOOKUPS, movingAverageLookups.toJSONArray());
    json.put(MOVINGAVERAGEUPDATES, movingAverageUpdates.toJSONArray());

    return json;
  }

  private static ConcurrentHashMap<Integer, Integer> toIntegerMap(JSONObject json) { // synchronized
    HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
    try {
      Iterator<String> nameItr = json.keys();
      while (nameItr.hasNext()) {
        String name = nameItr.next();
        map.put(Integer.valueOf(name), json.getInt(name));
      }
    } catch (JSONException e) {
    }
    return new ConcurrentHashMap<Integer, Integer>(map);
  }

  private static ConcurrentHashMap<Integer, StatsInfo> toStatsMap(JSONObject json) { //synchronized
    HashMap<Integer, StatsInfo> map = new HashMap<Integer, StatsInfo>();
    try {
      Iterator<String> nameItr = json.keys();
      while (nameItr.hasNext()) {
        String name = nameItr.next();
        map.put(Integer.valueOf(name), new StatsInfo(json.getJSONObject(name)));
      }
    } catch (JSONException e) {
    }
    return new ConcurrentHashMap<Integer, StatsInfo>(map);
  }

  public synchronized JSONObject statsMapToJSONObject(ConcurrentMap<Integer, StatsInfo> map) { // synchronized
    JSONObject json = new JSONObject();
    try {
      if (map != null) {
        for (Map.Entry<Integer, StatsInfo> e : map.entrySet()) {
          StatsInfo value = e.getValue();
          if (value != null) {
            JSONObject jsonStats = new JSONObject();
            jsonStats.put("read", value.read);
            jsonStats.put("write", value.write);
            json.put(e.getKey().toString(), jsonStats);
          }
        }
      }
    } catch (JSONException e) {
    }
    return json;
  }

  /**
   * ***********************************************************
   * Returns a String representation of this NameRecord. **********************************************************
   */
  @Override
  public synchronized String toString() { // synchronized
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      return "Error printing NameRecord: " + e;
    }
  }

  /**
   * ********************************************************
   * Updates the number of requests from each local name server.
   *
   * @param lnsId LNS server id sending the vote **********************************************************
   */
  public synchronized void addLNSRequestCount(int lnsId, int vote) { //synchronized
    if (lnsRequestsCount.containsKey(lnsId)) {
      int votes = lnsRequestsCount.get(lnsId) + vote;
      lnsRequestsCount.put(lnsId, votes);
    } else {
      lnsRequestsCount.put(lnsId, vote);
    }
  }

  // ABHIGYAN: adding these methods after making fields private
  /**
   * Returns the total number of requests from each local name server for this name
   *
   */
  public synchronized int[] getRequestCountsFromEachLocalNameServer() { // synchronized
    // Make an array of size equal to the number of local name servers.
    int[] lnsRequests = new int[StartNameServer.numberLNS];
    for (Map.Entry<Integer, Integer> entry :
            lnsRequestsCount.entrySet()) {

      Integer lnsId = entry.getKey();
      Integer votes = entry.getValue();
      lnsRequests[lnsId] = votes;

    }

    return lnsRequests;
  }

  public synchronized String getMovingAverageLookupString() { // synchronized
    if (movingAvgAggregateLookupFrequency == null) {
      return "NULL";
    }
    return movingAvgAggregateLookupFrequency.toString();
  }

  public synchronized String getMovingAverageUpdateString() { // synchronized
    if (movingAvgAggregateUpdateFrequency == null) {
      return "NULL";
    }
    return movingAvgAggregateUpdateFrequency.toString();
  }

  public synchronized double getUpdateRate() { // synchronized
    return updateRate;
  }

  public synchronized QueryResultValue getValuesList() { // synchronized
    return valuesList;
  }

  public synchronized int getTTL() { // synchronized
    return timeToLive;
  }

  public synchronized void setTTL(int ttl) { // synchronized
    this.timeToLive = ttl;
  }

  /**
   * Some test *
   */
  public static void main(String[] args) throws Exception {
    ConfigFileInfo.setNumberOfNameServers(20);
    Set<Integer> primary = new HashSet<Integer>();
    primary.add(5);
    primary.add(10);
    primary.add(15);
    getInitialActives(primary, 3, "1");
//        NameRecord.

//        test();
  }

  public static NameRecordV1 testCreateNameRecord() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", 20);

    Set<Integer> primary = new HashSet<Integer>();
    primary.add(1);
    primary.add(3);
    primary.add(5);
    NameServer.nodeID = 1;
    NameRecordV1 n = new NameRecordV1("s", NameRecordKey.EdgeRecord, new ArrayList(Arrays.asList("dd")));
    System.out.println(n.toString());
    //Sorted id: 1 5 0 3 2 6 4
    n.nameServerVotesMap.put(0, 10);
    n.nameServerVotesMap.put(1, 15);
    n.nameServerVotesMap.put(2, 4);
    n.nameServerVotesMap.put(3, 7);
    n.nameServerVotesMap.put(4, 1);
    n.nameServerVotesMap.put(5, 11);
    n.nameServerVotesMap.put(6, 3);
    System.out.println(n.toString());
    System.out.println(n.getHighestVotedReplicaID(7));
    System.out.println(n.getHighestVotedReplicaID(9));
    System.out.println(n.getHighestVotedReplicaID(1));
    System.out.println(n.getHighestVotedReplicaID(2));
    System.out.println(n.getHighestVotedReplicaID(3));
    System.out.println(n.getHighestVotedReplicaID(4));
    System.out.println(n.getHighestVotedReplicaID(5));
    System.out.println(n.getHighestVotedReplicaID(6));

    n.setPrimaryReplica(true);
    n.addActiveNameserver(1);
    n.addActiveNameserver(2);
    n.addActiveNameserver(3);
    System.out.println("All " + n.allNameServer());
    System.out.println(n.toString());
    System.out.println(n.activeNameservers.toString());
    Set<Integer> array = n.copyActiveNameServers();
    System.out.println(array);
    n.removeActiveNameserver(2);
    return n;
  }

  public static void test() throws Exception {

    NameRecordV1 n = testCreateNameRecord();
    System.out.println(n.toString());
    System.out.println(n.activeNameservers.toString());

    System.out.println();

    Set<Integer> primary2 = new HashSet<Integer>();
    primary2.add(4);
    primary2.add(5);
    primary2.add(2);
    NameRecordV1 n2 = new NameRecordV1("s", NameRecordKey.EdgeRecord, new ArrayList(Arrays.asList("dd")));
    System.out.println(n2.getSmallestIdPrimary());
  }

  /**
   * @return the name
   */
  public synchronized String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public synchronized void setName(String name) {
    this.name = name;
  }

  /**
   * @return the recordKey
   */
  public synchronized NameRecordKey getRecordKey() {
    return recordKey;
  }

  /**
   * @param recordKey the recordKey to set
   */
  public synchronized void setRecordKey(NameRecordKey recordKey) {
    this.recordKey = recordKey;
  }

  /**
   * @return the isPrimaryReplica
   */
  public synchronized boolean isPrimaryReplica() {
    return primaryReplica;
  }

  /**
   * @param primaryReplica the isPrimaryReplica to set
   */
  public synchronized void setPrimaryReplica(boolean primaryReplica) {
    this.primaryReplica = primaryReplica;
  }

  /**
   * @return the primaryNameservers
   */
  public synchronized HashSet<Integer> getPrimaryNameservers() {
//		if (StartNameServer.debugMode) GNRS.getLogger().fine("Primaries in name record object: " + primaryNameservers.toString());
    return primaryNameservers;
  }

  /**
   * @param primaryNameservers the primaryNameservers to set
   */
  public synchronized void setPrimaryNameservers(HashSet<Integer> primaryNameservers) {
    this.primaryNameservers = primaryNameservers;
  }

  /**
   * return the set of active name servers in the network.
   *
   * @return
   */
  public synchronized Set<Integer> getOldActiveNameServers() {
    return oldActiveNameservers;
  }

  /**
   * Makes current active name servers old active name servers, and sets the new active name servers to true.
   *
   * @param newActiveNameServers
   * @param newActivePaxosID
   */
  public synchronized void updateActiveNameServers(Set<Integer> newActiveNameServers, String newActivePaxosID) {
    this.oldActiveRunning = this.activeRunning;
    this.oldActiveNameservers = this.activeNameservers;
    this.oldActivePaxosID = this.activePaxosID;
    this.activeNameservers = newActiveNameServers;
    this.activeRunning = false;
    this.activePaxosID = newActivePaxosID;
  }

  /**
   * are oldActiveNameSevers stopped now?
   *
   * @return
   */
  public synchronized boolean isOldActiveStopped(String oldPaxosID) {
    // checks whether old paxos is still running
    if (this.oldActivePaxosID.equals(oldPaxosID)) {
      // return database status
      return !oldActiveRunning;
    }
    // if oldActivePaxosID != oldPaxosID, it means this oldPaxosID had stopped.
    return true;
  }

  /**
   * are the activeNameServers running?
   *
   * @return
   */
  public synchronized boolean isActiveRunning() {
    return activeRunning;
  }

  public synchronized boolean setOldActiveStopped(String oldActiveID) {
    if (oldActiveID.equals(this.oldActivePaxosID)) {
      this.oldActiveRunning = false;
      return true;
    }
    return false;
  }

  /**
   * Set new active running.
   *
   * @param newActiveID
   * @return
   */
  public synchronized boolean setNewActiveRunning(String newActiveID) {
    if (newActiveID.equals(this.activePaxosID)) {
      this.activeRunning = true;
      return true;
    }
    return false;
  }

  /**
   * return paxos ID for the oldActiveNameSevers
   *
   * @return
   */
  public synchronized String getOldActivePaxosID() {
    return oldActivePaxosID;
  }

  /**
   * return paxos ID for new activeNameServers
   *
   * @return
   */
  public synchronized String getActivePaxosID() {
    return activePaxosID;
  }

  /**
   * checks whether paxosID is current active Paxos/oldactive paxos/neither. .
   *
   * @param paxosID
   * @return
   */
  public synchronized int getPaxosStatus(String paxosID) {
//		if (StartNameServer.debugMode) GNRS.getLogger().fine("NAME RECORD: Active Paxos = " + activePaxosID + " Old Active Paxos: " + oldActivePaxosID);
    if (activePaxosID != null && activePaxosID.equals(paxosID)) {
      return 1;
    }
    if (oldActivePaxosID != null && oldActivePaxosID.equals(paxosID)) {
      return 2;
    }
    return 3;
  }

  /**
   * ACTIVE: handles paxos instance corresponding to the current set of actives has stopped.
   *
   * @param paxosID ID of the active set stopped
   */
  public synchronized void handleCurrentActiveStop(String paxosID) {

    if (this.activePaxosID.equals(paxosID)) {
      // current set of actives has stopped.
      // copy all fields to "oldActive" variables
      // initialize "active" variables to null
      this.oldActivePaxosID = paxosID;
      this.activePaxosID = null;
      this.oldActiveNameservers = this.activeNameservers;
      this.activeNameservers = null;
      this.oldValuesList = valuesList;
      this.valuesList = null;
      this.oldActiveRunning = false; // none of them are running.
      this.activeRunning = false;
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" Updated variables after current active stopped. ");
      }
    } else {
    }
    // this stops the old active.
    // backup current state.

  }

  /**
   * When new actives are started, initialize the necessary variables.
   *
   * @param actives current set of active name servers
   * @param paxosID paxosID of the current name servers
   * @param currentValue starting value of active name servers
   */
  public synchronized void handleNewActiveStart(Set<Integer> actives, String paxosID,
          QueryResultValue currentValue) {
    this.activeNameservers = actives;
    this.activeRunning = true;
    this.activePaxosID = paxosID;
    this.valuesList = currentValue;
  }

  /**
   * Return
   *
   * @param oldPaxosID
   * @return
   */
  public synchronized QueryResultValue getOldValues(String oldPaxosID) {
    if (oldPaxosID.equals(this.oldActivePaxosID)) {
      return oldValuesList;
    }
    return null;
  }

  /**
   * whether the flag is set of not.
   *
   * @return
   */
  public synchronized boolean isMarkedForRemoval() {
    return markedForRemoval;
  }

  /**
   * sets the flag at name record stored at primary that this name record will be removed soon.
   */
  public synchronized void setMarkedForRemoval() {
    this.markedForRemoval = true;
  }

  /**
   * primary checks the current step of transition from old active name servers to new active name servers
   *
   * @return
   */
  public synchronized ACTIVE_STATE getNewActiveTransitionStage() {
    if (!activeRunning && oldActiveRunning) {
      return ACTIVE_STATE.OLD_ACTIVE_RUNNING; // new proposed but old not stop stopped
    }
    if (!activeRunning && !oldActiveRunning) {
      return ACTIVE_STATE.NO_ACTIVE_RUNNING; // both are stopped, start new active
    }
    if (activeRunning && !oldActiveRunning) {
      return ACTIVE_STATE.ACTIVE_RUNNING; // normal operation
    }
    if (StartNameServer.debugMode) {
      GNS.getLogger().severe("ERROR: BOTH OLD & NEW ACTIVE RUNNING for NameRecord. An error condition.");
    }
    return ACTIVE_STATE.BOTH_ACTIVE_RUNNING_ERROR; // both cannot be running.
  }

  public enum ACTIVE_STATE {

    ACTIVE_RUNNING,
    OLD_ACTIVE_RUNNING,
    NO_ACTIVE_RUNNING,
    BOTH_ACTIVE_RUNNING_ERROR
  };
  
  public void acquireReadLock() {
    lock.readLock().lock();
  }

  public void releaseReadLock() {
    lock.readLock().unlock();
  }

  public void acquireWriteLock() {
    lock.writeLock().lock();
  }

  public void releaseWriteLock() {
    lock.writeLock().unlock();
  }
}
