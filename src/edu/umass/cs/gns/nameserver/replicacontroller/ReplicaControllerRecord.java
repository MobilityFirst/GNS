package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.StatsInfo;
import edu.umass.cs.gns.nameserver.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nameserver.recordmap.MongoRecordMap;
import edu.umass.cs.gns.packet.UpdateOperation;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.MovingAverage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * The ReplicaControllerRecord class.
 *
 */
public class ReplicaControllerRecord {

  public final static String NAME = "rcr_name";
  public final static String PRIMARY_NAMESERVERS = "rcr_primary";
  public final static String ACTIVE_NAMESERVERS = "rcr_active";
  public final static String OLD_ACTIVE_NAMESERVERS = "rcr_oldactive";
  public final static String ACTIVE_NAMESERVERS_RUNNING = "rcr_activeRunning";
  public final static String OLD_ACTIVE_NAMESERVERS_RUNNING = "rcr_oldActiveRunning";
  public final static String ACTIVE_PAXOS_ID = "rcr_activePaxosID";
  public final static String OLD_ACTIVE_PAXOS_ID = "rcr_oldActivePaxosID";
  public final static String MARKED_FOR_REMOVAL = "rcr_markedForRemoval";
  public final static String NAMESERVER_VOTES_MAP = "rcr_nameserverVotesMap";
  public final static String NAMESERVER_STATS_MAP = "rcr_nameServerStatsMap";
  public final static String TOTALAGGREGATEREADFREQUENCY = "rcr_totalAggregateReadFrequency";
  public final static String TOTALAGGREGATEWRITEFREQUENCY = "rcr_totalAggregateWriteFrequency";
  public final static String PREVIOUSAGGREAGATEREADFREQUENCY = "rcr_previousAggregateReadFrequency";
  public final static String PREVIOUSAGGREAGATEWRITEFREQUENCY = "rcr_previousAggregateWriteFrequency";
  public final static String MOVINGAGGREGATELOOKUPFREQUENCY = "rcr_movingAvgAggregateLookupFrequency";
  public final static String MOVINGAGGREGATEUPDATEFREQUENCY = "rcr_movingAvgAggregateUpdateFrequency";
  //
  private final static int LAZYINT = -9999;
  /**
   * Name (host/domain) *
   */
  private String name;
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
  private MovingAverage movingAvgAggregateLookupFrequency = null;
  private MovingAverage movingAvgAggregateUpdateFrequency = null;
  private int totalAggregateReadFrequency;
  private int totalAggregateWriteFrequency;
  private int previousAggregateReadFrequency;
  private int previousAggregateWriteFrequency;
  /**
   * List of name server voted as a replica for this record. <Key = NameserverID, Value=#Votes}
   */
  private ConcurrentMap<Integer, Integer> nameServerVotesMap;
  /**
   * Indicates if we're using new load-as-you-go-scheme
   */
  private boolean lazyEval = false;
  /**
   * When we're loading values on demand this is the recordMap we use.
   */
  private BasicRecordMap recordMap;

  /**
   * Creates an instance of a ReplicaControllerRecord where the fields are left empty and filled in on demand from the backing
   * store.
   *
   * @param name
   * @param recordMap
   */
  public ReplicaControllerRecord(String name, BasicRecordMap recordMap) {
    if (recordMap == null) {
      throw new RuntimeException("Record map cannot be null!");
    }
    this.recordMap = recordMap;
    this.lazyEval = true;
    this.name = name;
    this.primaryNameservers = null;
    this.activeNameservers = null;
    this.oldActiveNameservers = null;
    this.oldActiveRunning = false;
    this.activeRunning = false;
    this.oldActivePaxosID = null;
    this.activePaxosID = null;
    this.markedForRemoval = false;
    this.nameServerStatsMap = null;
    this.movingAvgAggregateLookupFrequency = null;
    this.movingAvgAggregateUpdateFrequency = null;
    this.totalAggregateReadFrequency = LAZYINT;
    this.totalAggregateWriteFrequency = LAZYINT;
    this.previousAggregateReadFrequency = LAZYINT;
    this.previousAggregateWriteFrequency = LAZYINT;
    this.nameServerVotesMap = null;
  }

  /**
   * This method creates a new initialized ReplicaControllerRecord.
   */
  public ReplicaControllerRecord(String name) {
    this.name = name;
    //Initialize the entry in the map
    primaryNameservers = (HashSet<Integer>) HashFunction.getPrimaryReplicas(name);
//        if (primaryNameservers.contains(NameServer.nodeID) == false)
    if (StartNameServer.debugMode) {
      GNS.getLogger().finer("Constructor Primaries: " + primaryNameservers);
    }

    this.nameServerStatsMap = new ConcurrentHashMap<Integer, StatsInfo>(10, 0.75f, 3);

    this.oldActiveNameservers = new HashSet<Integer>(primaryNameservers);
    //this.oldActiveNameservers = new HashSet<Integer>(getPrimaryNameservers());
    this.activeNameservers = new HashSet<Integer>(primaryNameservers);
    //this.activeNameservers = getInitialActives(primaryNameservers, StartNameServer.minReplica, name);
    if (StartNameServer.debugMode) {
      GNS.getLogger().finer(" Name Record INITIAL ACTIVES ARE: " + activeNameservers);
    }
    this.oldActivePaxosID = name + "-1"; // initialized uniformly among primaries
    this.activePaxosID = name + "-2";

    this.totalAggregateReadFrequency = 0;
    this.totalAggregateWriteFrequency = 0;
    this.previousAggregateReadFrequency = 0;
    this.previousAggregateWriteFrequency = 0;
    this.nameServerVotesMap = new ConcurrentHashMap<Integer, Integer>(10, 0.75f, 3);
    this.movingAvgAggregateLookupFrequency = new MovingAverage(StartNameServer.movingAverageWindowSize);
    this.movingAvgAggregateUpdateFrequency = new MovingAverage(StartNameServer.movingAverageWindowSize);
  }

  /**
   * Creates a new ReplicaControllerRecord from a JSONObject.
   *
   * @param json
   * @throws JSONException
   */
  public ReplicaControllerRecord(JSONObject json) throws JSONException {

    this.name = json.getString(NAME);

    this.primaryNameservers = (HashSet<Integer>) JSONUtils.JSONArrayToSetInteger(json.getJSONArray(PRIMARY_NAMESERVERS));
    this.activeNameservers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(ACTIVE_NAMESERVERS));

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
    this.nameServerStatsMap = toStatsMap(json.getJSONObject(NAMESERVER_STATS_MAP));

    this.totalAggregateReadFrequency = json.getInt(TOTALAGGREGATEREADFREQUENCY);
    this.totalAggregateWriteFrequency = json.getInt(TOTALAGGREGATEWRITEFREQUENCY);
    this.previousAggregateReadFrequency = json.getInt(PREVIOUSAGGREAGATEREADFREQUENCY);
    this.previousAggregateWriteFrequency = json.getInt(PREVIOUSAGGREAGATEWRITEFREQUENCY);


    if (json.has(MOVINGAGGREGATELOOKUPFREQUENCY)) {
      this.movingAvgAggregateLookupFrequency = new MovingAverage(json.getJSONArray(MOVINGAGGREGATELOOKUPFREQUENCY), StartNameServer.movingAverageWindowSize);
    }
    if (json.has(MOVINGAGGREGATEUPDATEFREQUENCY)) {
      this.movingAvgAggregateUpdateFrequency = new MovingAverage(json.getJSONArray(MOVINGAGGREGATEUPDATEFREQUENCY), StartNameServer.movingAverageWindowSize);
    }
  }

  /**
   * Converts a ReplicaControllerRecord to a JSONObject.
   *
   * @return
   * @throws JSONException
   */
  public synchronized JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    // the reason we're putting the "user level" keys "inline" with all the other keys is
    // so that we can later access the metadata and the data using the same mechanism, namely a
    // lookup of the fields in the database. Alternatively, we could have separate storage and lookup
    // of data and metadata.

    json.put(NAME, getName());

    json.put(PRIMARY_NAMESERVERS, new JSONArray(getPrimaryNameservers()));
    json.put(ACTIVE_NAMESERVERS, new JSONArray(getActiveNameservers()));

    // new fields
    json.put(OLD_ACTIVE_NAMESERVERS, new JSONArray(getOldActiveNameservers()));
    json.put(OLD_ACTIVE_NAMESERVERS_RUNNING, isOldActiveRunning());
    json.put(ACTIVE_NAMESERVERS_RUNNING, isActiveRunning());
    json.put(ACTIVE_PAXOS_ID, getActivePaxosID());
    json.put(OLD_ACTIVE_PAXOS_ID, getOldActivePaxosID());
    json.put(MARKED_FOR_REMOVAL, isMarkedForRemoval());
    //		json.put(key, value)
    json.put(NAMESERVER_VOTES_MAP, getNameServerVotesMap());
    json.put(NAMESERVER_STATS_MAP, statsMapToJSONObject(getNameServerStatsMap()));

    json.put(TOTALAGGREGATEREADFREQUENCY, getTotalAggregateReadFrequency());
    json.put(TOTALAGGREGATEWRITEFREQUENCY, getTotalAggregateWriteFrequency());
    json.put(PREVIOUSAGGREAGATEREADFREQUENCY, getPreviousAggregateReadFrequency());
    json.put(PREVIOUSAGGREAGATEWRITEFREQUENCY, getPreviousAggregateWriteFrequency());


    if (getMovingAvgAggregateLookupFrequency() != null) {
      json.put(MOVINGAGGREGATELOOKUPFREQUENCY, getMovingAvgAggregateLookupFrequency().toJSONArray());
    }
    if (getMovingAvgAggregateUpdateFrequency() != null) {
      json.put(MOVINGAGGREGATEUPDATEFREQUENCY, getMovingAvgAggregateUpdateFrequency().toJSONArray());
    }

    return json;
  }

  /**
   * @return the name
   */
  public synchronized String getName() {
    return name;
  }

  public boolean isLazyEval() {
    return lazyEval;
  }

  //
  // lazified accessors
  // NOTE THAT ALL OF THESE (WITH EXCEPTIONS OF THE ONES THAT EXPLICITLY UPDATE AN OBJECT) READ THE VALUE FROM THE
  // DATABASE ONCE! WHICH MEANS THEY ASSUME THERE ARE NO INTERLEAVED CALLS THAT UPDATE THE DATABASE
  //
  /**
   * @return the activeNameservers
   */
  public Set<Integer> getActiveNameservers() {
    if (isLazyEval() && activeNameservers == null) {
      activeNameservers = (HashSet<Integer>) recordMap.getNameRecordFieldAsIntegerSet(name, ACTIVE_NAMESERVERS);
    }
    return activeNameservers;
  }

  /**
   * Returns the PrimaryNameservers.
   *
   * @return primaryNameservers as a set of Integers
   */
  public synchronized HashSet<Integer> getPrimaryNameservers() {
    if (isLazyEval() && primaryNameservers == null) {
      primaryNameservers = (HashSet<Integer>) recordMap.getNameRecordFieldAsIntegerSet(name, PRIMARY_NAMESERVERS);
    }
    return primaryNameservers;
  }

  /**
   * @param activeNameservers the activeNameservers to set
   */
  public void setActiveNameservers(Set<Integer> activeNameservers) {
    this.activeNameservers = activeNameservers;
    if (isLazyEval() && activeNameservers != null) {
      recordMap.updateNameRecordFieldAsIntegerSet(name, ACTIVE_NAMESERVERS, activeNameservers);
    }
  }

  /**
   * return the set of active name servers in the network.
   *
   * @return
   */
  public synchronized Set<Integer> getOldActiveNameservers() {
    if (isLazyEval() && oldActiveNameservers == null) {
      oldActiveNameservers = (HashSet<Integer>) recordMap.getNameRecordFieldAsIntegerSet(name, OLD_ACTIVE_NAMESERVERS);
    }
    return oldActiveNameservers;
  }

  /**
   * @param oldActiveNameservers the oldActiveNameservers to set
   */
  public void setOldActiveNameservers(Set<Integer> oldActiveNameservers) {
    this.oldActiveNameservers = oldActiveNameservers;
    if (isLazyEval() && oldActiveNameservers != null) {
      recordMap.updateNameRecordFieldAsIntegerSet(name, OLD_ACTIVE_NAMESERVERS, oldActiveNameservers);
    }
  }

  /**
   * @return the oldActiveRunning
   */
  public boolean isOldActiveRunning() {
    if (isLazyEval()) {
      oldActiveRunning = recordMap.getNameRecordFieldAsBoolean(name, OLD_ACTIVE_NAMESERVERS_RUNNING);
    }
    return oldActiveRunning;
  }

  /**
   * @param oldActiveRunning the oldActiveRunning to set
   */
  public void setOldActiveRunning(boolean oldActiveRunning) {
    this.oldActiveRunning = oldActiveRunning;
    if (isLazyEval()) {
      recordMap.updateNameRecordFieldAsBoolean(name, OLD_ACTIVE_NAMESERVERS_RUNNING, oldActiveRunning);
    }
  }

  /**
   * are the activeNameServers running?
   *
   * @return
   */
  public synchronized boolean isActiveRunning() {
    if (isLazyEval()) {
      activeRunning = recordMap.getNameRecordFieldAsBoolean(name, ACTIVE_NAMESERVERS_RUNNING);
    }
    return activeRunning;
  }

  /**
   * @param activeRunning the activeRunning to set
   */
  public void setActiveRunning(boolean activeRunning) {
    this.activeRunning = activeRunning;
    if (isLazyEval()) {
      recordMap.updateNameRecordFieldAsBoolean(name, ACTIVE_NAMESERVERS_RUNNING, activeRunning);
    }
  }

  /**
   * return paxos ID for the oldActiveNameSevers
   *
   * @return
   */
  public synchronized String getOldActivePaxosID() {
    if (isLazyEval() && oldActivePaxosID == null) {
      oldActivePaxosID = recordMap.getNameRecordField(name, OLD_ACTIVE_PAXOS_ID);
    }
    return oldActivePaxosID;
  }

  /**
   * @param oldActivePaxosID the oldActivePaxosID to set
   */
  public void setOldActivePaxosID(String oldActivePaxosID) {
    this.oldActivePaxosID = oldActivePaxosID;
    if (isLazyEval() && oldActivePaxosID != null) {
      recordMap.updateNameRecordField(name, OLD_ACTIVE_PAXOS_ID, oldActivePaxosID);
    }
  }

  /**
   * return paxos ID for new activeNameServers
   *
   * @return
   */
  public synchronized String getActivePaxosID() {
    if (isLazyEval() && activePaxosID == null) {
      activePaxosID = recordMap.getNameRecordField(name, ACTIVE_PAXOS_ID);
    }
    return activePaxosID;
  }

  /**
   * @param activePaxosID the activePaxosID to set
   */
  public void setActivePaxosID(String activePaxosID) {
    this.activePaxosID = activePaxosID;
    if (isLazyEval() && activePaxosID != null) {
      recordMap.updateNameRecordField(name, ACTIVE_PAXOS_ID, activePaxosID);
    }
  }

  /**
   * whether the flag is set of not.
   *
   * @return
   */
  public synchronized boolean isMarkedForRemoval() {
    if (isLazyEval()) {
      markedForRemoval = recordMap.getNameRecordFieldAsBoolean(name, MARKED_FOR_REMOVAL);
    }
    return markedForRemoval;
  }

  /**
   * @param markedForRemoval the markedForRemoval to set
   */
  public void setMarkedForRemoval(boolean markedForRemoval) {
    this.markedForRemoval = markedForRemoval;
    if (isLazyEval()) {
      recordMap.updateNameRecordFieldAsBoolean(name, MARKED_FOR_REMOVAL, markedForRemoval);
    }
  }

  /**
   * @return the nameServerStatsMap
   */
  public ConcurrentMap<Integer, StatsInfo> getNameServerStatsMap() {
    if (isLazyEval() && nameServerStatsMap == null) {
      nameServerStatsMap = recordMap.getNameRecordFieldAsStatsMap(name, NAMESERVER_STATS_MAP);
    }
    return nameServerStatsMap;
  }

  /**
   *
   * @param id
   * @param readFrequency
   * @param writeFrequency
   */
  public synchronized void addNameServerStats(int id, int readFrequency, int writeFrequency) {
    if (isLazyEval()) {
      nameServerStatsMap = getNameServerStatsMap();
    }
    if (nameServerStatsMap != null) {
      nameServerStatsMap.put(id, new StatsInfo(readFrequency, writeFrequency));
    }
    if (isLazyEval() && nameServerStatsMap != null) {
      recordMap.updateNameRecordField(name, NAMESERVER_STATS_MAP, statsMapToJSONObject(nameServerStatsMap).toString());
    }
  }

  /**
   * @return the nameServerVotesMap
   */
  public ConcurrentMap<Integer, Integer> getNameServerVotesMap() {
    if (isLazyEval() && nameServerVotesMap == null) {
      nameServerVotesMap = recordMap.getNameRecordFieldAsVotesMap(name, NAMESERVER_VOTES_MAP);
    }
    return nameServerVotesMap;
  }

  /**
   * Adds vote to the name server for replica selection.
   *
   * @param id Name server id receiving the vote
   */
  public synchronized void addReplicaSelectionVote(int id, int vote) { //synchronized
    if (isLazyEval()) {
      nameServerVotesMap = getNameServerVotesMap();
    }
    if (nameServerVotesMap.containsKey(id)) {
      int votes = nameServerVotesMap.get(id) + vote;
      nameServerVotesMap.put(id, votes);
    } else {
      nameServerVotesMap.put(id, vote);
    }
    if (isLazyEval()) {
      recordMap.updateNameRecordField(name, NAMESERVER_VOTES_MAP, new JSONObject(nameServerVotesMap).toString());
    }
  }

  /**
   * @return the movingAvgAggregateLookupFrequency
   */
  public MovingAverage getMovingAvgAggregateLookupFrequency() {
    if (isLazyEval() && movingAvgAggregateLookupFrequency == null) {
      String result = recordMap.getNameRecordField(name, MOVINGAGGREGATELOOKUPFREQUENCY);
      try {
        movingAvgAggregateLookupFrequency = new MovingAverage(new JSONArray(result), StartNameServer.movingAverageWindowSize);
      } catch (JSONException e) {
        GNS.getLogger().severe("Error parsing result movingAvgAggregateLookupFrequency for " + name + " :" + e);
      }
    }
    return movingAvgAggregateLookupFrequency;
  }

  public void updateMovingAvgAggregateLookupFrequency(int value) {
    if (isLazyEval()) {
      movingAvgAggregateLookupFrequency = getMovingAvgAggregateLookupFrequency();
    }
    movingAvgAggregateLookupFrequency.add(value);
    if (isLazyEval()) {
      recordMap.updateNameRecordField(name, MOVINGAGGREGATELOOKUPFREQUENCY,
              movingAvgAggregateLookupFrequency.toJSONArray().toString());
    }
  }

  /**
   * @return the movingAvgAggregateUpdateFrequency
   */
  public MovingAverage getMovingAvgAggregateUpdateFrequency() {
    if (isLazyEval() && movingAvgAggregateUpdateFrequency == null) {
      String result = recordMap.getNameRecordField(name, MOVINGAGGREGATEUPDATEFREQUENCY);
      try {
        movingAvgAggregateUpdateFrequency = new MovingAverage(new JSONArray(result), StartNameServer.movingAverageWindowSize);
      } catch (JSONException e) {
        GNS.getLogger().severe("Error parsing result movingAvgAggregateLookupFrequency for " + name + " :" + e);
      }
    }
    return movingAvgAggregateUpdateFrequency;
  }

  public void updateMovingAvgAggregateUpdateFrequency(int value) {
    if (isLazyEval()) {
      movingAvgAggregateUpdateFrequency = getMovingAvgAggregateUpdateFrequency();
    }
    movingAvgAggregateUpdateFrequency.add(value);
    if (isLazyEval()) {
      recordMap.updateNameRecordField(name, MOVINGAGGREGATEUPDATEFREQUENCY,
              movingAvgAggregateUpdateFrequency.toJSONArray().toString());
    }
  }

  /**
   * @return the totalAggregateReadFrequency
   */
  public int getTotalAggregateReadFrequency() {
    if (isLazyEval() && totalAggregateReadFrequency == LAZYINT) {
      totalAggregateReadFrequency = recordMap.getNameRecordFieldAsInt(name, TOTALAGGREGATEREADFREQUENCY);
    }
    return totalAggregateReadFrequency;
  }

  /**
   * @param totalAggregateReadFrequency the totalAggregateReadFrequency to set
   */
  public void setTotalAggregateReadFrequency(int totalAggregateReadFrequency) {
    this.totalAggregateReadFrequency = totalAggregateReadFrequency;
    if (isLazyEval() && totalAggregateReadFrequency != LAZYINT) {
      recordMap.updateNameRecordFieldAsInteger(name, TOTALAGGREGATEREADFREQUENCY, totalAggregateReadFrequency);
    }
  }

  /**
   * @return the totalAggregateWriteFrequency
   */
  public int getTotalAggregateWriteFrequency() {
    if (isLazyEval() && totalAggregateWriteFrequency == LAZYINT) {
      totalAggregateWriteFrequency = recordMap.getNameRecordFieldAsInt(name, TOTALAGGREGATEWRITEFREQUENCY);
    }
    return totalAggregateWriteFrequency;
  }

  /**
   * @param totalAggregateWriteFrequency the totalAggregateWriteFrequency to set
   */
  public void setTotalAggregateWriteFrequency(int totalAggregateWriteFrequency) {
    this.totalAggregateWriteFrequency = totalAggregateWriteFrequency;
    if (isLazyEval() && totalAggregateWriteFrequency != LAZYINT) {
      recordMap.updateNameRecordFieldAsInteger(name, TOTALAGGREGATEWRITEFREQUENCY, totalAggregateWriteFrequency);
    }
  }

  /**
   * @return the previousAggregateReadFrequency
   */
  public int getPreviousAggregateReadFrequency() {
    if (isLazyEval() && previousAggregateReadFrequency == LAZYINT) {
      previousAggregateReadFrequency = recordMap.getNameRecordFieldAsInt(name, PREVIOUSAGGREAGATEREADFREQUENCY);
    }
    return previousAggregateReadFrequency;
  }

  /**
   * @param previousAggregateReadFrequency the previousAggregateReadFrequency to set
   */
  public void setPreviousAggregateReadFrequency(int previousAggregateReadFrequency) {
    this.previousAggregateReadFrequency = previousAggregateReadFrequency;
    if (isLazyEval() && previousAggregateReadFrequency != LAZYINT) {
      recordMap.updateNameRecordFieldAsInteger(name, PREVIOUSAGGREAGATEREADFREQUENCY, previousAggregateReadFrequency);
    }
  }

  /**
   * @return the previousAggregateWriteFrequency
   */
  public int getPreviousAggregateWriteFrequency() {
    if (isLazyEval() && previousAggregateWriteFrequency == LAZYINT) {
      previousAggregateWriteFrequency = recordMap.getNameRecordFieldAsInt(name, PREVIOUSAGGREAGATEWRITEFREQUENCY);
    }
    return previousAggregateWriteFrequency;
  }

  /**
   * @param previousAggregateWriteFrequency the previousAggregateWriteFrequency to set
   */
  public void setPreviousAggregateWriteFrequency(int previousAggregateWriteFrequency) {
    this.previousAggregateWriteFrequency = previousAggregateWriteFrequency;
    if (isLazyEval() && previousAggregateWriteFrequency != LAZYINT) {
      recordMap.updateNameRecordFieldAsInteger(name, PREVIOUSAGGREAGATEWRITEFREQUENCY, previousAggregateWriteFrequency);
    }
  }

  //
  // Utilities that use the accessors
  //
  public enum ACTIVE_STATE {

    ACTIVE_RUNNING,
    OLD_ACTIVE_RUNNING,
    NO_ACTIVE_RUNNING,
    BOTH_ACTIVE_RUNNING_ERROR
  };

  /**
   * Returns a copy of the active name servers set.
   *
   * @return copy of ActiveNameServers
   */
  public synchronized Set<Integer> copyActiveNameServers() { //synchronized
    Set<Integer> set = new HashSet<Integer>();
    for (int id : getActiveNameservers()) {
      set.add(id);
    }
    return set;
  }

  /**
   * Returns the number of ActiveNameServers.
   *
   * @return
   */
  public synchronized int numActiveNameServers() { //synchronized
    return getActiveNameservers().size();
  }

//  private static Set<Integer> getInitialActives(Set<Integer> primaryNameservers, int count, String name) {
//    // choose three actives which are different from primaries
//    Set<Integer> newActives = new HashSet<Integer>();
//    Random r = new Random(name.hashCode());
//
//    for (int j = 0; j < count; j++) {
//      while (true) {
//        int id = r.nextInt(ConfigFileInfo.getNumberOfNameServers());
//        //            int active = (j + id)% ConfigFileInfo.getNumberOfNameServers();
//        if (!primaryNameservers.contains(id) && !newActives.contains(id)) {
//          GNS.getLogger().finer("ID " + id);
//          newActives.add(id);
//          break;
//        }
//      }
//    }
//    if (newActives.size() < count) {
//      if (StartNameServer.debugMode) {
//        GNS.getLogger().warning(" ERROR: initial actives < " + count + " Initial Actives: " + newActives);
//      }
//    }
//    return newActives;
//  }
  /**
   * Returns true if the name record contains a primary name server with the given id. False otherwise.
   *
   * @param id Primary name server id
   */
  public synchronized boolean containsPrimaryNameserver(int id) {
    return getPrimaryNameservers().contains(id);
  }

  /**
   * Returns a set of highest voted name servers ids.
   *
   * @param numReplica Number of name servers to be selected.
   */
  public synchronized Set<Integer> getHighestVotedReplicaID(int numReplica) { //synchronized
    Set<Integer> replicas = new HashSet<Integer>();

    for (int i = 1; i <= numReplica; i++) {
      int highestVotes = -1;
      int highestVotedReplicaID = -1;

      for (Map.Entry<Integer, Integer> entry : getNameServerVotesMap().entrySet()) {
        int nameServerId = entry.getKey();
        int votes = entry.getValue();
        //Skip name server that are unreachable
        // from main branch 269
        if (ConfigFileInfo.getPingLatency(nameServerId) == -1) { //|| getPrimaryNameservers().contains(nameServerId)
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

      if (replicas.size() == getNameServerVotesMap().size()) {
        break;
      }
    }
    return replicas;
  }

  /**
   * Returns the total number of lookup request across all active name servers
   *
   * @return
   */
  public synchronized double getReadStats_Paxos() {
    setTotalAggregateReadFrequency(0);
    for (StatsInfo info : getNameServerStatsMap().values()) {
      setTotalAggregateReadFrequency(getTotalAggregateReadFrequency() + info.read);
    }

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("TotalAggregateRead: " + getTotalAggregateReadFrequency()
              + " Previous: " + getPreviousAggregateReadFrequency()
              + " Current: " + (getTotalAggregateReadFrequency() - getPreviousAggregateReadFrequency()));
    }
    updateMovingAvgAggregateLookupFrequency(getTotalAggregateReadFrequency() - getPreviousAggregateReadFrequency());
    //getMovingAvgAggregateLookupFrequency().add(getTotalAggregateReadFrequency() - getPreviousAggregateReadFrequency());
    setPreviousAggregateReadFrequency(getTotalAggregateReadFrequency());

    return getMovingAvgAggregateLookupFrequency().getAverage();
  }

  /**
   * Returns the total number of updates request across all active name servers
   *
   * @return
   */
  public synchronized double getWriteStats_Paxos() {
    setTotalAggregateWriteFrequency(0);
    for (StatsInfo info : getNameServerStatsMap().values()) {
      setTotalAggregateWriteFrequency(getTotalAggregateWriteFrequency() + info.write);
    }

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("TotalAggregateWrite: " + getTotalAggregateWriteFrequency()
              + " Previous: " + getPreviousAggregateWriteFrequency()
              + " Current: " + (getTotalAggregateWriteFrequency() - getPreviousAggregateWriteFrequency()));
    }
    updateMovingAvgAggregateUpdateFrequency(getTotalAggregateWriteFrequency() - getPreviousAggregateWriteFrequency());
    //getMovingAvgAggregateUpdateFrequency().add(getTotalAggregateWriteFrequency() - getPreviousAggregateWriteFrequency());
    setPreviousAggregateWriteFrequency(getTotalAggregateWriteFrequency());

    return getMovingAvgAggregateUpdateFrequency().getAverage();
  }

  public static ConcurrentHashMap<Integer, Integer> toIntegerMap(JSONObject json) {
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

  public static ConcurrentHashMap<Integer, StatsInfo> toStatsMap(JSONObject json) { //synchronized
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

  public synchronized JSONObject statsMapToJSONObject(ConcurrentMap<Integer, StatsInfo> map) {
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
   * @return the isPrimaryReplica
   */
  public synchronized boolean isPrimaryReplica() {
    return getPrimaryNameservers().contains(NameServer.nodeID);
  }

  /**
   * Makes current active name servers old active name servers, and sets the new active name servers to true.
   *
   * @param newActiveNameServers
   * @param newActivePaxosID
   */
  public synchronized void updateActiveNameServers(Set<Integer> newActiveNameServers, String newActivePaxosID) {
    this.setOldActiveRunning(this.isActiveRunning());
    this.setOldActiveNameservers(this.getActiveNameservers());
    this.setOldActivePaxosID(this.getActivePaxosID());
    this.setActiveNameservers(newActiveNameServers);
    this.setActiveRunning(false);
    this.setActivePaxosID(newActivePaxosID);
  }

  /**
   * are oldActiveNameSevers stopped now?
   *
   * @return
   */
  public synchronized boolean isOldActiveStopped(String oldPaxosID) {
    // checks whether old paxos is still running
    if (this.getOldActivePaxosID().equals(oldPaxosID)) {
      // return database status
      return !isOldActiveRunning();
    }
    // if oldActivePaxosID != oldPaxosID, it means this oldPaxosID had stopped.
    return true;
  }

  public synchronized boolean setOldActiveStopped(String oldActiveID) {
    if (oldActiveID.equals(this.getOldActivePaxosID())) {
      this.setOldActiveRunning(false);
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
    if (newActiveID.equals(this.getActivePaxosID())) {
      this.setActiveRunning(true);
      return true;
    }
    return false;
  }

  /**
   * sets the flag at name record stored at primary that this name record will be removed soon.
   */
  public synchronized void setMarkedForRemoval() {
    this.setMarkedForRemoval(true);
  }

  /**
   * primary checks the current step of transition from old active name servers to new active name servers
   *
   * @return
   */
  public synchronized ACTIVE_STATE getNewActiveTransitionStage() {
    if (!isActiveRunning() && isOldActiveRunning()) {
      return ACTIVE_STATE.OLD_ACTIVE_RUNNING; // new proposed but old not stop stopped
    }
    if (!isActiveRunning() && !isOldActiveRunning()) {
      return ACTIVE_STATE.NO_ACTIVE_RUNNING; // both are stopped, start new active
    }
    if (isActiveRunning() && !isOldActiveRunning()) {
      return ACTIVE_STATE.ACTIVE_RUNNING; // normal operation
    }
    if (StartNameServer.debugMode) {
      GNS.getLogger().severe("ERROR: BOTH OLD & NEW ACTIVE RUNNING for NameRecord. An error condition.");
    }
    return ACTIVE_STATE.BOTH_ACTIVE_RUNNING_ERROR; // both cannot be running.
  }

  /**
   *
   * Returns a String representation of this NameRecord.
   */
  @Override
  public synchronized String toString() {
    if (isLazyEval()) {
      return "ReplicaControllerRecord{LAZY - " + "name=" + name + '}';
    } else {
      try {
        return toJSONObject().toString();
      } catch (JSONException e) {
        return "Error printing NameRecord: " + e;
      }
    }
  }

  // test code
  public static void main(String[] args) throws Exception {
    NameServer.nodeID = 4;
    StartNameServer.movingAverageWindowSize = 10;
    test();
    //System.exit(0);
  }

  // make this query:
  // http://127.0.0.1:8080/GNS/registerAccount?name=sally&publickey=dummy3
  private static void test() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    BasicRecordMap replicaController = new MongoRecordMap(MongoRecords.DBREPLICACONTROLLER);
    replicaController.reset();
    ReplicaControllerRecord record = new ReplicaControllerRecord("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24");
    NameServer.replicaController.addNameRecordPrimary(record);
    record = new ReplicaControllerRecord("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", replicaController);
    System.out.println("PRIMARY NS: " + record.getPrimaryNameservers());
    System.out.println("CONTAINS ACTIVE NS: " + record.containsPrimaryNameserver(12));
    record.addNameServerStats(10, 50, 75);
    System.out.println("READ STATS: " + record.getReadStats_Paxos());
    record.addReplicaSelectionVote(1, 5);
    record.addReplicaSelectionVote(1, 1);
    record.addReplicaSelectionVote(2, 2);
    record.addReplicaSelectionVote(3, 3);
    record.addReplicaSelectionVote(4, 4);
    System.out.println("3 HIGHEST VOTES: " + record.getHighestVotedReplicaID(3));
    record.updateMovingAvgAggregateLookupFrequency(10);
    record.updateMovingAvgAggregateLookupFrequency(30);
    record.updateMovingAvgAggregateLookupFrequency(50);
    System.out.println("MOVING AG READ: " + record.getMovingAvgAggregateLookupFrequency());
  }
}
