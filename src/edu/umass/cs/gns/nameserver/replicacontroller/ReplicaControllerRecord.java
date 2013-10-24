package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.StatsInfo;
import edu.umass.cs.gns.database.Field;
import edu.umass.cs.gns.database.FieldType;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nameserver.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nameserver.recordmap.MongoRecordMap;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.MovingAverage;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ReplicaControllerRecord class.
 *
 */
public class ReplicaControllerRecord {

//  public final static Field NAME = new Field("rcr_name", FieldType.STRING);
//
//  public final static Field PRIMARY_NAMESERVERS = new Field("rcr_primary", FieldType.SET_INTEGER);
//  public final static Field ACTIVE_NAMESERVERS = new Field("rcr_active", FieldType.SET_INTEGER);
//  public final static Field OLD_ACTIVE_NAMESERVERS = new Field("rcr_oldactive", FieldType.SET_INTEGER);
//
//  public final static Field ACTIVE_NAMESERVERS_RUNNING = new Field("rcr_activeRunning", FieldType.BOOLEAN);
//  public final static Field OLD_ACTIVE_NAMESERVERS_RUNNING = new Field("rcr_oldActiveRunning", FieldType.BOOLEAN);
//
//  public final static Field ACTIVE_PAXOS_ID = new Field("rcr_activePaxosID", FieldType.STRING);
//  public final static Field OLD_ACTIVE_PAXOS_ID = new Field("rcr_oldActivePaxosID", FieldType.STRING);
//
//  public final static Field MARKED_FOR_REMOVAL = new Field("rcr_markedForRemoval", FieldType.INTEGER);
//
//  public final static Field VOTES_MAP = new Field("rcr_votesMap", FieldType.VOTES_MAP);
//  public final static Field STATS_MAP = new Field("rcr_statsMap", FieldType.STATS_MAP);
//
//  public final static Field PREV_TOTAL_READ = new Field("rcr_prevTotalRead", FieldType.INTEGER);
//  public final static Field PREV_TOTAL_WRITE = new Field("rcr_prevTotalWrite", FieldType.INTEGER);
//  public final static Field MOV_AVG_READ = new Field("rcr_movAvgRead", FieldType.LIST_INTEGER);
//  public final static Field MOV_AVG_WRITE = new Field("rcr_movAvgWrite", FieldType.LIST_INTEGER);
//
//  public final static Field KEEP_ALIVE_TIME = new Field("rcr_keepAlive", FieldType.INTEGER);
  public final static Field NAME = new Field("rcr_name", FieldType.STRING);
  public final static Field PRIMARY_NAMESERVERS = (StartNameServer.experimentMode == false) ? new Field("rcr_primary", FieldType.SET_INTEGER) : new Field("rc1", FieldType.SET_INTEGER);
  public final static Field ACTIVE_NAMESERVERS = (StartNameServer.experimentMode == false) ? new Field("rcr_active", FieldType.SET_INTEGER) : new Field("rc2", FieldType.SET_INTEGER);
  public final static Field OLD_ACTIVE_NAMESERVERS = (StartNameServer.experimentMode == false) ? new Field("rcr_oldactive", FieldType.SET_INTEGER) : new Field("rc3", FieldType.SET_INTEGER);
  public final static Field ACTIVE_NAMESERVERS_RUNNING = (StartNameServer.experimentMode == false) ? new Field("rcr_activeRunning", FieldType.BOOLEAN) : new Field("rc4", FieldType.BOOLEAN);
  public final static Field OLD_ACTIVE_NAMESERVERS_RUNNING = (StartNameServer.experimentMode == false) ? new Field("rcr_oldActiveRunning", FieldType.BOOLEAN) : new Field("rc5", FieldType.BOOLEAN);
  public final static Field ACTIVE_PAXOS_ID = (StartNameServer.experimentMode == false) ? new Field("rcr_activePaxosID", FieldType.STRING) : new Field("rc6", FieldType.STRING);
  public final static Field OLD_ACTIVE_PAXOS_ID = (StartNameServer.experimentMode == false) ? new Field("rcr_oldActivePaxosID", FieldType.STRING) : new Field("rc7", FieldType.STRING);
  public final static Field MARKED_FOR_REMOVAL = (StartNameServer.experimentMode == false) ? new Field("rcr_markedForRemoval", FieldType.INTEGER) : new Field("rc8", FieldType.INTEGER);
  public final static Field VOTES_MAP = (StartNameServer.experimentMode == false) ? new Field("rcr_votesMap", FieldType.VOTES_MAP) : new Field("rc9", FieldType.VOTES_MAP);
  public final static Field STATS_MAP = (StartNameServer.experimentMode == false) ? new Field("rcr_statsMap", FieldType.STATS_MAP) : new Field("rc10", FieldType.STATS_MAP);
  public final static Field PREV_TOTAL_READ = (StartNameServer.experimentMode == false) ? new Field("rcr_prevTotalRead", FieldType.INTEGER) : new Field("rc11", FieldType.INTEGER);
  public final static Field PREV_TOTAL_WRITE = (StartNameServer.experimentMode == false) ? new Field("rcr_prevTotalWrite", FieldType.INTEGER) : new Field("rc12", FieldType.INTEGER);
  public final static Field MOV_AVG_READ = (StartNameServer.experimentMode == false) ? new Field("rcr_movAvgRead", FieldType.LIST_INTEGER) : new Field("rc13", FieldType.LIST_INTEGER);
  public final static Field MOV_AVG_WRITE = (StartNameServer.experimentMode == false) ? new Field("rcr_movAvgWrite", FieldType.LIST_INTEGER) : new Field("rc14", FieldType.LIST_INTEGER);
  public final static Field KEEP_ALIVE_TIME = (StartNameServer.experimentMode == false) ? new Field("rcr_keepAlive", FieldType.INTEGER) : new Field("rc15", FieldType.INTEGER);
  private HashMap<Field, Object> hashMap = new HashMap<Field, Object>();

  /********************************************
   * CONSTRUCTORS
   * ******************************************/
  /**
   * This method creates a new initialized ReplicaControllerRecord. by filling in all the fields.
   * If false, this constructor is the same as <code>public ReplicaControllerRecord(String name)</code>.
   */
  public ReplicaControllerRecord(String name, boolean initialize) {

    hashMap = new HashMap<Field, Object>();
    hashMap.put(NAME, name);

    if (initialize == false) {
      return;
    }
    hashMap.put(PRIMARY_NAMESERVERS, HashFunction.getPrimaryReplicas(name));
    hashMap.put(ACTIVE_NAMESERVERS, HashFunction.getPrimaryReplicas(name));
    hashMap.put(OLD_ACTIVE_NAMESERVERS, HashFunction.getPrimaryReplicas(name));

    hashMap.put(OLD_ACTIVE_NAMESERVERS_RUNNING, false);
    hashMap.put(ACTIVE_NAMESERVERS_RUNNING, true);

    hashMap.put(ACTIVE_PAXOS_ID, name + "-1");
    hashMap.put(OLD_ACTIVE_PAXOS_ID, name + "-2");

    hashMap.put(MARKED_FOR_REMOVAL, -1);

    hashMap.put(VOTES_MAP, new ConcurrentHashMap<Integer, Integer>());
    hashMap.put(STATS_MAP, new ConcurrentHashMap<Integer, StatsInfo>());

    hashMap.put(PREV_TOTAL_READ, 0);
    hashMap.put(PREV_TOTAL_WRITE, 0);
    hashMap.put(MOV_AVG_READ, new ArrayList<Integer>());
    hashMap.put(MOV_AVG_WRITE, new ArrayList<Integer>());

    hashMap.put(KEEP_ALIVE_TIME, 0);

  }


  /**
   * ONLY FOR RUNNING EXPERIMENTS!!
   * This method creates a new initialized ReplicaControllerRecord. by filling in all the fields.
   * If false, this constructor is the same as <code>public ReplicaControllerRecord(String name)</code>.
   */
  public ReplicaControllerRecord(String name, Set<Integer> actives, boolean initialize) {

    hashMap = new HashMap<Field, Object>();
    hashMap.put(NAME,name);

    if (initialize == false) return;
    hashMap.put(PRIMARY_NAMESERVERS, HashFunction.getPrimaryReplicas(name));
    hashMap.put(ACTIVE_NAMESERVERS, actives);
    hashMap.put(OLD_ACTIVE_NAMESERVERS, actives);

    hashMap.put(OLD_ACTIVE_NAMESERVERS_RUNNING, false);
    hashMap.put(ACTIVE_NAMESERVERS_RUNNING, true);

    hashMap.put(ACTIVE_PAXOS_ID, name + "-1");
    hashMap.put(OLD_ACTIVE_PAXOS_ID, name + "-2");

    hashMap.put(MARKED_FOR_REMOVAL, -1);

    hashMap.put(VOTES_MAP, new ConcurrentHashMap<Integer,Integer>());
    hashMap.put(STATS_MAP, new ConcurrentHashMap<Integer,StatsInfo>());

    hashMap.put(PREV_TOTAL_READ, 0);
    hashMap.put(PREV_TOTAL_WRITE, 0);
    hashMap.put(MOV_AVG_READ, new ArrayList<Integer>());
    hashMap.put(MOV_AVG_WRITE, new ArrayList<Integer>());

    hashMap.put(KEEP_ALIVE_TIME, 0);

  }

  /**
   * creates an empty ReplicaControllerRecord object
   * @param name
   */
  public ReplicaControllerRecord(String name) {
    hashMap = new HashMap<Field, Object>();
    hashMap.put(NAME, name);

  }

  /**
   * Creates a new ReplicaControllerRecord from a JSONObject.
   *
   * @param json
   * @throws JSONException
   */
  public ReplicaControllerRecord(JSONObject json) throws JSONException {
    hashMap = new HashMap<Field, Object>();

    if (json.has(NAME.getName())) {
      hashMap.put(NAME, json.getString(NAME.getName()));
    }

    if (json.has(PRIMARY_NAMESERVERS.getName())) {
      hashMap.put(PRIMARY_NAMESERVERS, JSONUtils.getObject(PRIMARY_NAMESERVERS, json));
    }
    if (json.has(ACTIVE_NAMESERVERS.getName())) {
      hashMap.put(ACTIVE_NAMESERVERS, JSONUtils.getObject(ACTIVE_NAMESERVERS, json));
    }
    if (json.has(OLD_ACTIVE_NAMESERVERS.getName())) {
      hashMap.put(OLD_ACTIVE_NAMESERVERS, JSONUtils.getObject(OLD_ACTIVE_NAMESERVERS, json));
    }

    if (json.has(OLD_ACTIVE_NAMESERVERS_RUNNING.getName())) {
      hashMap.put(OLD_ACTIVE_NAMESERVERS_RUNNING, JSONUtils.getObject(OLD_ACTIVE_NAMESERVERS_RUNNING, json));
    }
    if (json.has(ACTIVE_NAMESERVERS_RUNNING.getName())) {
      hashMap.put(ACTIVE_NAMESERVERS_RUNNING, JSONUtils.getObject(ACTIVE_NAMESERVERS_RUNNING, json));
    }

    if (json.has(ACTIVE_PAXOS_ID.getName())) {
      hashMap.put(ACTIVE_PAXOS_ID, JSONUtils.getObject(ACTIVE_PAXOS_ID, json));
    }
    if (json.has(OLD_ACTIVE_PAXOS_ID.getName())) {
      hashMap.put(OLD_ACTIVE_PAXOS_ID, JSONUtils.getObject(OLD_ACTIVE_PAXOS_ID, json));
    }

    if (json.has(MARKED_FOR_REMOVAL.getName())) {
      hashMap.put(MARKED_FOR_REMOVAL, JSONUtils.getObject(MARKED_FOR_REMOVAL, json));
    }

    if (json.has(VOTES_MAP.getName())) {
      hashMap.put(VOTES_MAP, JSONUtils.getObject(VOTES_MAP, json));
    }
    if (json.has(STATS_MAP.getName())) {
      hashMap.put(STATS_MAP, JSONUtils.getObject(STATS_MAP, json));
    }


    if (json.has(PREV_TOTAL_READ.getName())) {
      hashMap.put(PREV_TOTAL_READ, JSONUtils.getObject(PREV_TOTAL_READ, json));
    }
    if (json.has(PREV_TOTAL_WRITE.getName())) {
      hashMap.put(PREV_TOTAL_WRITE, JSONUtils.getObject(PREV_TOTAL_WRITE, json));
    }
    if (json.has(MOV_AVG_READ.getName())) {
      hashMap.put(MOV_AVG_READ, JSONUtils.getObject(MOV_AVG_READ, json));
    }
    if (json.has(MOV_AVG_WRITE.getName())) {
      hashMap.put(MOV_AVG_WRITE, JSONUtils.getObject(MOV_AVG_WRITE, json));
    }

    if (json.has(KEEP_ALIVE_TIME.getName())) {
      hashMap.put(KEEP_ALIVE_TIME, JSONUtils.getObject(KEEP_ALIVE_TIME, json));
    }
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    GNS.getLogger().fine("hash map --> " + hashMap);
    for (Field f : hashMap.keySet()) {

      JSONUtils.putFieldInJsonObject(f, hashMap.get(f), jsonObject);
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return null;
  }

  /**
   * Constructor used by the initialize values read from database
   * @param allValues
   */
  public ReplicaControllerRecord(HashMap<Field, Object> allValues) {
    this.hashMap = allValues;
  }

  /********************************************
   * GETTER methods for each field in replica controller record
   * ******************************************/
  /**
   * @return the name
   */
  public String getName() throws FieldNotFoundException {
    if (hashMap.containsKey(NAME)) {
      return (String) hashMap.get(NAME);
    }
    throw new FieldNotFoundException(NAME);
  }

  /**
   * Returns the PrimaryNameservers.
   * @return primaryNameservers as a set of Integers
   */
  public HashSet<Integer> getPrimaryNameservers() throws FieldNotFoundException {
    if (hashMap.containsKey(PRIMARY_NAMESERVERS)) {
      return (HashSet<Integer>) hashMap.get(PRIMARY_NAMESERVERS);
    }
    throw new FieldNotFoundException(PRIMARY_NAMESERVERS);
  }

  /**
   * @return the activeNameservers
   */
  public Set<Integer> getActiveNameservers() throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_NAMESERVERS)) {
      return (Set<Integer>) hashMap.get(ACTIVE_NAMESERVERS);
    }
    throw new FieldNotFoundException(ACTIVE_NAMESERVERS);
  }

  /**
   * return the set of active name servers in the network.
   *
   * @return
   */
  public Set<Integer> getOldActiveNameservers() throws FieldNotFoundException {
    if (hashMap.containsKey(OLD_ACTIVE_NAMESERVERS)) {
      return (Set<Integer>) hashMap.get(OLD_ACTIVE_NAMESERVERS);
    }
    throw new FieldNotFoundException(OLD_ACTIVE_NAMESERVERS);
  }

  /**
   * are the activeNameServers running?
   *
   * @return
   */
  public boolean isActiveRunning() throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_NAMESERVERS_RUNNING)) {
      return (Boolean) hashMap.get(ACTIVE_NAMESERVERS_RUNNING);
    }
    throw new FieldNotFoundException(ACTIVE_NAMESERVERS_RUNNING);
  }

  /**
   * @return the oldActiveRunning
   */
  public boolean isOldActiveRunning() throws FieldNotFoundException {
    if (hashMap.containsKey(OLD_ACTIVE_NAMESERVERS_RUNNING)) {
      return (Boolean) hashMap.get(OLD_ACTIVE_NAMESERVERS_RUNNING);
    }
    throw new FieldNotFoundException(OLD_ACTIVE_NAMESERVERS_RUNNING);
  }

  /**
   * return paxos ID for new activeNameServers
   * @return
   */
  public String getActivePaxosID() throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_PAXOS_ID)) {
      return (String) hashMap.get(ACTIVE_PAXOS_ID);
    }
    throw new FieldNotFoundException(ACTIVE_PAXOS_ID);
  }

  /**
   * return paxos ID for the oldActiveNameSevers
   *
   * @return
   */
  public String getOldActivePaxosID() throws FieldNotFoundException {
    if (hashMap.containsKey(OLD_ACTIVE_PAXOS_ID)) {
      return (String) hashMap.get(OLD_ACTIVE_PAXOS_ID);
    }
    throw new FieldNotFoundException(OLD_ACTIVE_PAXOS_ID);
  }

  public int getMarkedForRemoval() throws FieldNotFoundException {
    if (hashMap.containsKey(MARKED_FOR_REMOVAL)) {
      return (Integer) hashMap.get(MARKED_FOR_REMOVAL);
    }
    throw new FieldNotFoundException(MARKED_FOR_REMOVAL);
  }

  /**
   * @return the nameServerVotesMap
   */
  public ConcurrentMap<Integer, Integer> getNameServerVotesMap() throws FieldNotFoundException {
    if (hashMap.containsKey(VOTES_MAP)) {
      return (ConcurrentMap<Integer, Integer>) hashMap.get(VOTES_MAP);
    }
    throw new FieldNotFoundException(VOTES_MAP);
  }

  /**
   * @return the nameServerStatsMap
   */
  public ConcurrentMap<Integer, StatsInfo> getNameServerStatsMap() throws FieldNotFoundException {
    if (hashMap.containsKey(STATS_MAP)) {
      return (ConcurrentMap<Integer, StatsInfo>) hashMap.get(STATS_MAP);
    }
    throw new FieldNotFoundException(STATS_MAP);
  }

  /**
   * @return the previousAggregateReadFrequency
   */
  public int getPreviousAggregateReadFrequency() throws FieldNotFoundException {
    if (hashMap.containsKey(PREV_TOTAL_READ)) {
      return (Integer) hashMap.get(PREV_TOTAL_READ);
    }
    throw new FieldNotFoundException(PREV_TOTAL_READ);
  }

  /**
   * @return the previousAggregateWriteFrequency
   */
  public int getPreviousAggregateWriteFrequency() throws FieldNotFoundException {
    if (hashMap.containsKey(PREV_TOTAL_WRITE)) {
      return (Integer) hashMap.get(PREV_TOTAL_WRITE);
    }
    throw new FieldNotFoundException(PREV_TOTAL_WRITE);
  }

  /**
   * @return the movingAvgAggregateLookupFrequency
   */
  public ArrayList<Integer> getMovingAvgAggregateLookupFrequency() throws FieldNotFoundException {
    if (hashMap.containsKey(MOV_AVG_READ)) {
      return (ArrayList<Integer>) hashMap.get(MOV_AVG_READ);
    }
    throw new FieldNotFoundException(MOV_AVG_READ);
  }

  /**
   * @return the movingAvgAggregateUpdateFrequency
   */
  public ArrayList<Integer> getMovingAvgAggregateUpdateFrequency() throws FieldNotFoundException {
    if (hashMap.containsKey(MOV_AVG_WRITE)) {
      return (ArrayList<Integer>) hashMap.get(MOV_AVG_WRITE);
    }
    throw new FieldNotFoundException(MOV_AVG_WRITE);
  }

  /**
   * return the keep alive time value stored in record
   * @return
   */
  public int getKeepAliveTime() throws FieldNotFoundException {
    if (hashMap.containsKey(KEEP_ALIVE_TIME)) {
      return (Integer) hashMap.get(KEEP_ALIVE_TIME);
    }
    throw new FieldNotFoundException(KEEP_ALIVE_TIME);
  }

  /********************************************
   * READ methods: these methods only read one or more fields. they use the above GETTER methods to access the values of fields.
   * ******************************************/
  public boolean isAdded() throws FieldNotFoundException {
    // TODO what semantics do you want? == 0 or >= 0
    return getMarkedForRemoval() == 0;
  }

  /**
   * whether the flag is set of not.
   * @return
   */
  public boolean isMarkedForRemoval() throws FieldNotFoundException {
    return getMarkedForRemoval() > 0;
  }

  /**
   * whether the flag is set of not.
   *
   * @return
   */
  public boolean isRemoved() throws FieldNotFoundException {
    return getMarkedForRemoval() == 2;
  }

  /**
   * Returns true if the name record contains a primary name server with the given id. False otherwise.
   *
   * @param id Primary name server id
   */
  public boolean containsPrimaryNameserver(int id) throws FieldNotFoundException {
    Set<Integer> primaries = getPrimaryNameservers();
    if (primaries != null) {
      return primaries.contains(id);
    }
    return false;
  }

  /**
   * Returns a set of highest voted name servers ids.
   *
   * @param numReplica Number of name servers to be selected.
   */
  public Set<Integer> getHighestVotedReplicaID(int numReplica) throws FieldNotFoundException { //
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

  public enum ACTIVE_STATE {

    ACTIVE_RUNNING,
    OLD_ACTIVE_RUNNING,
    NO_ACTIVE_RUNNING,
    BOTH_ACTIVE_RUNNING_ERROR
  };

  /**
   * primary checks the current step of transition from old active name servers to new active name servers
   *
   * @return
   */
  public ACTIVE_STATE getNewActiveTransitionStage() throws FieldNotFoundException {
    boolean active = isActiveRunning();
    boolean oldActive = isOldActiveRunning();
    if (!active && oldActive) {
      return ACTIVE_STATE.OLD_ACTIVE_RUNNING; // new proposed but old not stop stopped
    }
    if (!active && !oldActive) {
      return ACTIVE_STATE.NO_ACTIVE_RUNNING; // both are stopped, start new active
    }
    if (active && !oldActive) {
      return ACTIVE_STATE.ACTIVE_RUNNING; // normal operation
    }
    if (StartNameServer.debugMode) {
      GNS.getLogger().severe("ERROR: Error  BOTH OLD & NEW ACTIVE RUNNING for NameRecord. An error condition.");
    }
    return ACTIVE_STATE.BOTH_ACTIVE_RUNNING_ERROR; // both cannot be running.
  }

  /**
   * Are oldActiveNameSevers stopped now?
   *
   * @return
   */
  public boolean isOldActiveStopped(String oldPaxosID) throws FieldNotFoundException {
    String paxosID = getOldActivePaxosID();
    if (paxosID.equals(oldPaxosID)) {
      return !isOldActiveRunning();
    }
    return true;
  }
  /********************************************
   * WRITE methods: these methods write one or more fields. they may read values of some fields using the above GETTER methods.
   * ******************************************/
  private static ArrayList<Field> setMarkedForRemoval = new ArrayList<Field>();

  private static ArrayList<Field> getSetMarkedForRemoval() {
    synchronized (setMarkedForRemoval) {
      if (setMarkedForRemoval.size() > 0) {
        return setMarkedForRemoval;
      }
      setMarkedForRemoval.add(MARKED_FOR_REMOVAL);
      return setMarkedForRemoval;
    }
  }

  /**
   * set marked for removal as
   */
  public void setMarkedForRemoval() throws FieldNotFoundException {
    int markedForRemoval = getMarkedForRemoval();
    if (markedForRemoval == 0) {
      ArrayList<Field> fields = getSetMarkedForRemoval();

      ArrayList<Object> values = new ArrayList<Object>();
      values.add(1);

      NameServer.replicaController.update(getName(), NAME, fields, values);
      hashMap.put(MARKED_FOR_REMOVAL, 1);
    }
  }

  public void setRemoved() throws FieldNotFoundException {

    ArrayList<Field> fields = getSetMarkedForRemoval();

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(2);

    NameServer.replicaController.update(getName(), NAME, fields, values);
    hashMap.put(MARKED_FOR_REMOVAL, 2);

  }

  public void setAdded() throws FieldNotFoundException {

    ArrayList<Field> updateFields = getSetMarkedForRemoval();

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(0);

    NameServer.replicaController.update(getName(), NAME, updateFields, values);

    hashMap.put(MARKED_FOR_REMOVAL, 0);

    // TODO think if checking this is necessary
//    if (isLazyEval()) {
//      markedForRemoval = recordMap.getNameRecordFieldAsInt(name, MARKED_FOR_REMOVAL);
//    }
//
//    if (markedForRemoval > 0) {
//      GNS.getLogger().severe(" Exception: Trying to add a deleted record " + getName());
////      System.exit(2);
//    }
//
//    if (markedForRemoval == -1) {
//      this.markedForRemoval = 0;
//      if (isLazyEval()) {
//        recordMap.updateNameRecordFieldAsInteger(name, MARKED_FOR_REMOVAL, markedForRemoval);
//      }
//    }
  }
  private static ArrayList<Field> updateActiveNameServerFields = new ArrayList<Field>();

  private static ArrayList<Field> getUpdateActiveNameServerFields() {
    synchronized (updateActiveNameServerFields) {
      if (updateActiveNameServerFields.size() > 0) {
        return updateActiveNameServerFields;
      }
      updateActiveNameServerFields.add(OLD_ACTIVE_NAMESERVERS_RUNNING);
      updateActiveNameServerFields.add(ACTIVE_NAMESERVERS_RUNNING);
      updateActiveNameServerFields.add(OLD_ACTIVE_NAMESERVERS);
      updateActiveNameServerFields.add(ACTIVE_NAMESERVERS);
      updateActiveNameServerFields.add(OLD_ACTIVE_PAXOS_ID);
      updateActiveNameServerFields.add(ACTIVE_PAXOS_ID);
      return updateActiveNameServerFields;
    }
  }

  /**
   * Makes current active name servers old active name servers, and sets the new active name servers to true.
   *
   * @param newActiveNameServers
   * @param newActivePaxosID
   */
  public void updateActiveNameServers(Set<Integer> newActiveNameServers, String newActivePaxosID) throws FieldNotFoundException {

    boolean activeRunning = isActiveRunning();
    Set<Integer> actives = getActiveNameservers();
    String activePaxosID = getActivePaxosID();

    ArrayList<Field> updateFields = getUpdateActiveNameServerFields();

    ArrayList<Object> updateValues = new ArrayList<Object>();

    updateValues.add(activeRunning);
    updateValues.add(false);
    updateValues.add(actives);
    updateValues.add(newActiveNameServers);
    updateValues.add(activePaxosID);
    updateValues.add(newActivePaxosID);

    NameServer.replicaController.update(getName(), NAME, updateFields, updateValues);

    hashMap.put(OLD_ACTIVE_NAMESERVERS_RUNNING, activeRunning);
    hashMap.put(ACTIVE_NAMESERVERS_RUNNING, false);
    hashMap.put(OLD_ACTIVE_NAMESERVERS, actives);
    hashMap.put(ACTIVE_NAMESERVERS, newActiveNameServers);
    hashMap.put(OLD_ACTIVE_PAXOS_ID, activePaxosID);
    hashMap.put(ACTIVE_PAXOS_ID, newActivePaxosID);
  }
  private static ArrayList<Field> setOldActiveStoppedFields = new ArrayList<Field>();

  private static ArrayList<Field> getSetOldActiveStoppedFields() {
    synchronized (setOldActiveStoppedFields) {
      if (setOldActiveStoppedFields.size() > 0) {
        return setOldActiveStoppedFields;
      }
      setOldActiveStoppedFields.add(OLD_ACTIVE_NAMESERVERS_RUNNING);
      return setOldActiveStoppedFields;
    }
  }

  public boolean setOldActiveStopped(String oldActiveID) throws FieldNotFoundException {

    if (oldActiveID.equals(this.getOldActivePaxosID())) {

      ArrayList<Field> updateFields = getSetOldActiveStoppedFields();

      ArrayList<Object> values = new ArrayList<Object>();
      values.add(false);

      NameServer.replicaController.update(getName(), NAME, updateFields, values);
      hashMap.put(OLD_ACTIVE_NAMESERVERS_RUNNING, false);
      return true;
    }
    return false;
  }
  private static ArrayList<Field> setNewActiveRunningFields = new ArrayList<Field>();

  private static ArrayList<Field> getSetNewActiveRunningFields() {
    synchronized (setNewActiveRunningFields) {
      if (setNewActiveRunningFields.size() > 0) {
        return setNewActiveRunningFields;
      }
      setNewActiveRunningFields.add(ACTIVE_NAMESERVERS_RUNNING);
      setNewActiveRunningFields.add(OLD_ACTIVE_NAMESERVERS_RUNNING);
      return setNewActiveRunningFields;
    }
  }

  /**
   * Set new active running.
   *
   * @param newActiveID
   * @return
   */
  public boolean setNewActiveRunning(String newActiveID) throws FieldNotFoundException {
    if (newActiveID.equals(this.getActivePaxosID())) {

      ArrayList<Field> updateFields = getSetNewActiveRunningFields();

      ArrayList<Object> values = new ArrayList<Object>();
      values.add(true);
      values.add(false);

      NameServer.replicaController.update(getName(),NAME,updateFields,values);
      hashMap.put(ACTIVE_NAMESERVERS_RUNNING, true);
      hashMap.put(OLD_ACTIVE_NAMESERVERS_RUNNING, false);

      return true;
    }
    return false;
  }

  /**
   * Returns the total number of lookup request across all active name servers
   *
   * @return
   */
  public double[] recomputeAverageReadWriteRate() throws FieldNotFoundException {
    int previousTotalReads = getPreviousAggregateReadFrequency();
    int previousTotalWrites = getPreviousAggregateWriteFrequency();
    MovingAverage lookups = new MovingAverage(getMovingAvgAggregateLookupFrequency(), StartNameServer.movingAverageWindowSize);
    MovingAverage updates = new MovingAverage(getMovingAvgAggregateUpdateFrequency(), StartNameServer.movingAverageWindowSize);

    lookups.add(previousTotalReads);
    updates.add(previousTotalWrites);

    double[] readWriteRate = new double[2];
    readWriteRate[0] = lookups.getAverage();
    readWriteRate[1] = updates.getAverage();


    ArrayList<Field> updateFields = new ArrayList<Field>();
    updateFields.add(PREV_TOTAL_READ);
    updateFields.add(PREV_TOTAL_WRITE);
    updateFields.add(MOV_AVG_READ);
    updateFields.add(MOV_AVG_WRITE);

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(0);
    values.add(0);
    values.add(lookups.toArrayList());
    values.add(updates.toArrayList());

    NameServer.replicaController.update(getName(), NAME, updateFields, values);

    hashMap.put(PREV_TOTAL_READ, 0);
    hashMap.put(PREV_TOTAL_WRITE, 0);
    hashMap.put(MOV_AVG_READ, lookups.toArrayList());
    hashMap.put(MOV_AVG_WRITE, updates.toArrayList());
    return readWriteRate;

  }

  /**
   * Adds vote to the name server for replica selection.
   * and increment lookup count and update count.
   *
   * @param id Name server id receiving the vote
   */
  public void addReplicaSelectionVote(int id, int vote, int update) throws FieldNotFoundException { //
    NameServer.replicaController.increment(getName(),
            Field.keys(PREV_TOTAL_READ, PREV_TOTAL_WRITE),
            //incrementFields, 
            Field.values(vote, update),
            //incrementValues, 
            VOTES_MAP,
            Field.keys(new Field(Integer.toString(id), FieldType.INTEGER)),
            //votesMapKeys,
            Field.values(vote) //votesMapValues
            );
  }

  /**
   *
   * @param id
   * @param readFrequency
   * @param writeFrequency
   */
  public void addNameServerStats(int id, int readFrequency, int writeFrequency) throws FieldNotFoundException {
    ConcurrentMap<Integer, StatsInfo> statsMap = getNameServerStatsMap();

    if (statsMap != null) {
      statsMap.put(id, new StatsInfo(readFrequency, writeFrequency));
      ArrayList<Field> updateFields = new ArrayList<Field>();
      updateFields.add(STATS_MAP);

      ArrayList<Object> values = new ArrayList<Object>();
      values.add(statsMap);

      NameServer.replicaController.update(getName(), NAME, updateFields, values);
      // StatsMap is already updated in hashMap.
    }
  }

  /********************************************
   * SETTER methods, these methods write to database one field in the name record.
   * ******************************************/
  /**
   * @param activeNameservers the activeNameservers to set
   */
  public void setActiveNameservers(Set<Integer> activeNameservers) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param oldActiveNameservers the oldActiveNameservers to set
   */
  public void setOldActiveNameservers(Set<Integer> oldActiveNameservers) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param oldActiveRunning the oldActiveRunning to set
   */
  public void setOldActiveRunning(boolean oldActiveRunning) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param activeRunning the activeRunning to set
   */
  public void setActiveRunning(boolean activeRunning) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param oldActivePaxosID the oldActivePaxosID to set
   */
  public void setOldActivePaxosID(String oldActivePaxosID) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param activePaxosID the activePaxosID to set
   */
  public void setActivePaxosID(String activePaxosID) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param previousAggregateReadFrequency the previousAggregateReadFrequency to set
   */
  public void setPreviousAggregateReadFrequency(int previousAggregateReadFrequency) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param previousAggregateWriteFrequency the previousAggregateWriteFrequency to set
   */
  public void setPreviousAggregateWriteFrequency(int previousAggregateWriteFrequency) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param keepAliveTime set keep alive time
   */
  public void setKeepAliveTime(long keepAliveTime) throws FieldNotFoundException {
    ArrayList<Field> fields = new ArrayList<Field>();
    fields.add(KEEP_ALIVE_TIME);

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(keepAliveTime);

    NameServer.replicaController.update(getName(), NAME, fields, values);

  }

  // test code
  public static void main(String[] args) throws FieldNotFoundException, Exception {
    NameServer.nodeID = 4;
    StartNameServer.movingAverageWindowSize = 10;
    test();
    //System.exit(0);
  }

  // make this query:
  // http://127.0.0.1:8080/GNS/registerAccount?name=sally&publickey=dummy3
  private static void test() throws FieldNotFoundException, Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    BasicRecordMap replicaController = new MongoRecordMap(MongoRecords.DBREPLICACONTROLLER);
    replicaController.reset();
    ReplicaControllerRecord record = new ReplicaControllerRecord("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24");
    record.addReplicaSelectionVote(1, 5, 4);
    record.addReplicaSelectionVote(1, 1, 4);
    record.addReplicaSelectionVote(2, 2, 4);
    record.addReplicaSelectionVote(3, 3, 4);
    record.addReplicaSelectionVote(4, 4, 4);
    record.addNameServerStats(1, 50, 75);
    record.addNameServerStats(2, 50, 75);
    System.out.println(record.toJSONObject().toString());
    try {
      replicaController.addNameRecordPrimary(record);
    } catch (RecordExistsException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    // create the lazy record
    record = new ReplicaControllerRecord("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", true);
    System.out.println("PRIMARY NS: " + record.getPrimaryNameservers());
    System.out.println("CONTAINS ACTIVE NS: " + record.containsPrimaryNameserver(12));
    record.addNameServerStats(10, 50, 75);
    System.out.println("READ STATS: " + record.recomputeAverageReadWriteRate());

    record.addReplicaSelectionVote(11, 5, 0);
    record.addReplicaSelectionVote(11, 1, 0);
    record.addReplicaSelectionVote(21, 2, 0);
    record.addReplicaSelectionVote(13, 3, 0);
    record.addReplicaSelectionVote(14, 4, 0);
    record.addNameServerStats(11, 50, 75);
    record.addNameServerStats(12, 50, 75);
    System.out.println("3 HIGHEST VOTES: " + record.getHighestVotedReplicaID(3));
//    record.updateMovingAvgAggregateLookupFrequency(10);
//    record.updateMovingAvgAggregateLookupFrequency(30);
//    record.updateMovingAvgAggregateLookupFrequency(50);
    System.out.println("MOVING AG READ: " + record.getMovingAvgAggregateLookupFrequency());

    MongoRecords instance = MongoRecords.getInstance();
    instance.printAllEntries(MongoRecords.DBREPLICACONTROLLER);

    try {
      record = replicaController.getNameRecordPrimary("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24");
    } catch (RecordNotFoundException e) {

      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    System.out.println(record.toJSONObject().toString());

  }
}
