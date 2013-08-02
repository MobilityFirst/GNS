package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.StatsInfo;
import edu.umass.cs.gns.nameserver.recordmap.BasicRecordMap;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.MovingAverage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
  public final static String NAMESERVERSTATSMAP = "rcr_nameServerStatsMap";
  public final static String TOTALAGGREGATEREADFREQUENCY = "rcr_totalAggregateReadFrequency";
  public final static String TOTALAGGREGATEWRITEFREQUENCY = "rcr_totalAggregateWriteFrequency";
  public final static String PREVIOUSAGGREAGATEREADFREQUENCY = "rcr_previousAggregateReadFrequency";
  public final static String PREVIOUSAGGREAGATEWRITEFREQUENCY = "rcr_previousAggregateWriteFrequency";
  public final static String MOVINGAGGREGATELOOKUPFREQUENCY = "rcr_movingAvgAggregateLookupFrequency";
  public final static String MOVINGAGGREGATEUPDATEFREQUENCY = "rcr_movingAvgAggregateUpdateFrequency";
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

  public ReplicaControllerRecord(String name, BasicRecordMap recordMap) {
    if (recordMap == null) {
      throw new RuntimeException("Record map cannot be null!");
    }
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
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @param primaryNameservers the primaryNameservers to set
   */
  public void setPrimaryNameservers(HashSet<Integer> primaryNameservers) {
    this.primaryNameservers = primaryNameservers;
  }

  /**
   * @return the activeNameservers
   */
  public Set<Integer> getActiveNameservers() {
    return activeNameservers;
  }

  /**
   * Returns the PrimaryNameservers.
   *
   * @return primaryNameservers as a set of Integers
   */
  public synchronized HashSet<Integer> getPrimaryNameservers() {
    return primaryNameservers;
  }

  /**
   * @param activeNameservers the activeNameservers to set
   */
  public void setActiveNameservers(Set<Integer> activeNameservers) {
    this.activeNameservers = activeNameservers;
  }

  /**
   * return the set of active name servers in the network.
   *
   * @return
   */
  public synchronized Set<Integer> getOldActiveNameservers() {
    return oldActiveNameservers;
  }

  /**
   * @param oldActiveNameservers the oldActiveNameservers to set
   */
  public void setOldActiveNameservers(Set<Integer> oldActiveNameservers) {
    this.oldActiveNameservers = oldActiveNameservers;
  }

  /**
   * @return the oldActiveRunning
   */
  public boolean isOldActiveRunning() {
    return oldActiveRunning;
  }

  /**
   * @param oldActiveRunning the oldActiveRunning to set
   */
  public void setOldActiveRunning(boolean oldActiveRunning) {
    this.oldActiveRunning = oldActiveRunning;
  }

  /**
   * are the activeNameServers running?
   *
   * @return
   */
  public synchronized boolean isActiveRunning() {
    return activeRunning;
  }

  /**
   * @param activeRunning the activeRunning to set
   */
  public void setActiveRunning(boolean activeRunning) {
    this.activeRunning = activeRunning;
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
   * @param oldActivePaxosID the oldActivePaxosID to set
   */
  public void setOldActivePaxosID(String oldActivePaxosID) {
    this.oldActivePaxosID = oldActivePaxosID;
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
   * @param activePaxosID the activePaxosID to set
   */
  public void setActivePaxosID(String activePaxosID) {
    this.activePaxosID = activePaxosID;
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
   * @param markedForRemoval the markedForRemoval to set
   */
  public void setMarkedForRemoval(boolean markedForRemoval) {
    this.markedForRemoval = markedForRemoval;
  }

  /**
   * @return the nameServerStatsMap
   */
  public ConcurrentMap<Integer, StatsInfo> getNameServerStatsMap() {
    return nameServerStatsMap;
  }

  /**
   * @param nameServerStatsMap the nameServerStatsMap to set
   */
  public void setNameServerStatsMap(ConcurrentMap<Integer, StatsInfo> nameServerStatsMap) {
    this.nameServerStatsMap = nameServerStatsMap;
  }

  /**
   * @return the movingAvgAggregateLookupFrequency
   */
  public MovingAverage getMovingAvgAggregateLookupFrequency() {
    return movingAvgAggregateLookupFrequency;
  }

  /**
   * @param movingAvgAggregateLookupFrequency the movingAvgAggregateLookupFrequency to set
   */
  public void setMovingAvgAggregateLookupFrequency(MovingAverage movingAvgAggregateLookupFrequency) {
    this.movingAvgAggregateLookupFrequency = movingAvgAggregateLookupFrequency;
  }

  /**
   * @return the movingAvgAggregateUpdateFrequency
   */
  public MovingAverage getMovingAvgAggregateUpdateFrequency() {
    return movingAvgAggregateUpdateFrequency;
  }

  /**
   * @param movingAvgAggregateUpdateFrequency the movingAvgAggregateUpdateFrequency to set
   */
  public void setMovingAvgAggregateUpdateFrequency(MovingAverage movingAvgAggregateUpdateFrequency) {
    this.movingAvgAggregateUpdateFrequency = movingAvgAggregateUpdateFrequency;
  }

  /**
   * @return the totalAggregateReadFrequency
   */
  public int getTotalAggregateReadFrequency() {
    return totalAggregateReadFrequency;
  }

  /**
   * @param totalAggregateReadFrequency the totalAggregateReadFrequency to set
   */
  public void setTotalAggregateReadFrequency(int totalAggregateReadFrequency) {
    this.totalAggregateReadFrequency = totalAggregateReadFrequency;
  }

  /**
   * @return the totalAggregateWriteFrequency
   */
  public int getTotalAggregateWriteFrequency() {
    return totalAggregateWriteFrequency;
  }

  /**
   * @param totalAggregateWriteFrequency the totalAggregateWriteFrequency to set
   */
  public void setTotalAggregateWriteFrequency(int totalAggregateWriteFrequency) {
    this.totalAggregateWriteFrequency = totalAggregateWriteFrequency;
  }

  /**
   * @return the previousAggregateReadFrequency
   */
  public int getPreviousAggregateReadFrequency() {
    return previousAggregateReadFrequency;
  }

  /**
   * @param previousAggregateReadFrequency the previousAggregateReadFrequency to set
   */
  public void setPreviousAggregateReadFrequency(int previousAggregateReadFrequency) {
    this.previousAggregateReadFrequency = previousAggregateReadFrequency;
  }

  /**
   * @return the previousAggregateWriteFrequency
   */
  public int getPreviousAggregateWriteFrequency() {
    return previousAggregateWriteFrequency;
  }

  /**
   * @param previousAggregateWriteFrequency the previousAggregateWriteFrequency to set
   */
  public void setPreviousAggregateWriteFrequency(int previousAggregateWriteFrequency) {
    this.previousAggregateWriteFrequency = previousAggregateWriteFrequency;
  }

  /**
   * @return the nameServerVotesMap
   */
  public ConcurrentMap<Integer, Integer> getNameServerVotesMap() {
    return nameServerVotesMap;
  }

  /**
   * @param nameServerVotesMap the nameServerVotesMap to set
   */
  public void setNameServerVotesMap(ConcurrentMap<Integer, Integer> nameServerVotesMap) {
    this.nameServerVotesMap = nameServerVotesMap;
  }

  public enum ACTIVE_STATE {

    ACTIVE_RUNNING,
    OLD_ACTIVE_RUNNING,
    NO_ACTIVE_RUNNING,
    BOTH_ACTIVE_RUNNING_ERROR
  };

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
    this.nameServerStatsMap = toStatsMap(json.getJSONObject(NAMESERVERSTATSMAP));

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
    json.put(NAMESERVERSTATSMAP, statsMapToJSONObject(getNameServerStatsMap()));

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

  private static Set<Integer> getInitialActives(Set<Integer> primaryNameservers, int count, String name) {
    // choose three actives which are different from primaries
    Set<Integer> newActives = new HashSet<Integer>();
    Random r = new Random(name.hashCode());

    for (int j = 0; j < count; j++) {
      while (true) {
        int id = r.nextInt(ConfigFileInfo.getNumberOfNameServers());
        //            int active = (j + id)% ConfigFileInfo.getNumberOfNameServers();
        if (!primaryNameservers.contains(id) && !newActives.contains(id)) {
          GNS.getLogger().finer("ID " + id);
          newActives.add(id);
          break;
        }
      }
    }
    if (newActives.size() < count) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().warning(" ERROR: initial actives < " + count + " Initial Actives: " + newActives);
      }
    }
    return newActives;
  }

  /**
   * Returns true if the name record contains a primary name server with the given id. False otherwise.
   *
   * @param id Primary name server id
   */
  public synchronized boolean containsPrimaryNameserver(int id) {
    return getPrimaryNameservers().contains(id);
  }

  /**
   *
   * @param id
   * @param readFrequency
   * @param writeFrequency
   */
  public synchronized void addNameServerStats(int id, int readFrequency, int writeFrequency) {
    if (getNameServerStatsMap() != null) {
      getNameServerStatsMap().put(id, new StatsInfo(readFrequency, writeFrequency));
    }
  }

  /**
   * Adds vote to the name server for replica selection.
   *
   * @param id Name server id receiving the vote
   */
  public synchronized void addReplicaSelectionVote(int id, int vote) { //synchronized
    if (getNameServerVotesMap().containsKey(id)) {
      int votes = getNameServerVotesMap().get(id) + vote;
      getNameServerVotesMap().put(id, votes);
    } else {
      getNameServerVotesMap().put(id, vote);
    }
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

      for (Map.Entry<Integer, Integer> entry : nameServerVotesMap.entrySet()) {
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
    getMovingAvgAggregateLookupFrequency().add(getTotalAggregateReadFrequency() - getPreviousAggregateReadFrequency());
    setPreviousAggregateReadFrequency(getTotalAggregateReadFrequency());


    //      if (currentReadFrequency > 0) {
    //
    //      }
    return getMovingAvgAggregateLookupFrequency().getAverage();
    //      return Util.round();
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
    getMovingAvgAggregateUpdateFrequency().add(getTotalAggregateWriteFrequency() - getPreviousAggregateWriteFrequency());
    setPreviousAggregateWriteFrequency(getTotalAggregateWriteFrequency());

    return getMovingAvgAggregateUpdateFrequency().getAverage();
    //      return Util.round();
  }

  private static ConcurrentHashMap<Integer, Integer> toIntegerMap(JSONObject json) {
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
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      return "Error printing NameRecord: " + e;
    }
  }
}
