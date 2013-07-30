package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nameserver.recordmap.MongoRecordMap;
import edu.umass.cs.gns.nameserver.recordmap.RecordMapInterface;
import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.packet.UpdateOperation;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Created with IntelliJ IDEA. User: abhigyan Date: 7/26/13 Time: 12:46 PM To change this template use File | Settings | File
 * Templates.
 */
public class NameRecord implements Comparable<NameRecord> {

  private final static String USER_KEYS = "nr_user_keys";
  private final static String OLDVALUESMAP = "nr_oldValuesMap";
  //
  public final static String NAME = "nr_name";
  public final static String KEY = "nr_key"; // legacy use
  public final static String TIMETOLIVE = "nr_timeToLive";
  public final static String PRIMARY_NAMESERVERS = "nr_primary";
  public final static String ACTIVE_NAMESERVERS = "nr_active";
  public final static String ACTIVE_PAXOS_ID = "nr_activePaxosID";
  public final static String OLD_ACTIVE_PAXOS_ID = "nr_oldActivePaxosID";
  public final static String TOTALLOOKUPREQUEST = "nr_totalLookupRequest";
  public final static String TOTALUPDATEREQUEST = "nr_totalUpdateRequest";
  //
  private final static int LAZYINT = -9999;
  /**
   * Name (host/domain) *
   */
  private String name;
  /**
   * Map of values of this record *
   */
  private ValuesMap valuesMap;
  /**
   * Map of values from old active name servers *
   */
  private ValuesMap oldValuesMap;
  /**
   * TTL: time to live IN SECONDS*
   */
  private int timeToLive = 0; // 0 means TTL - 0 (no caching), -1 means the record will never expire if it gets into a cache
  /**
   * Set of primary nameservers *
   */
  private HashSet<Integer> primaryNameservers;
  /**
   * Set of active nameservers most recently computed.
   */
  private Set<Integer> activeNameservers;
  /**
   * Paxos instance ID of the new active name server set.
   */
  private String activePaxosID;
  /**
   * Paxos instance ID of the old active name server set
   */
  private String oldActivePaxosID;
  /**
   * Number of lookups *
   */
  private int totalLookupRequest;
  /**
   * Number of updates *
   */
  private int totalUpdateRequest;
  /**
   * Indicates if we're using new load-as-you-go-scheme
   */
  private boolean lazyEval = false;
  private BasicRecordMap recordMap;

  public NameRecord(String name, BasicRecordMap recordMap) {
    if (recordMap == null) {
      throw new RuntimeException("Record map cannot be null!");
    }

    this.recordMap = recordMap;
    this.lazyEval = true;
    this.name = name;
    this.valuesMap = new ValuesMap(); // set this to a default value - it's handled specially
    // set these to sentinel values which tells us the should be loaded on demand
    this.oldValuesMap = null;
    this.timeToLive = LAZYINT;
    this.primaryNameservers = null;
    this.activeNameservers = null;
    this.activePaxosID = null;
    this.oldActivePaxosID = null;
    this.totalLookupRequest = LAZYINT;
    this.totalUpdateRequest = LAZYINT;
  }

  public NameRecord(String name) {
    this.name = name;
    //Initialize the entry in the map
    primaryNameservers = (HashSet<Integer>) HashFunction.getPrimaryReplicas(name);
    this.valuesMap = new ValuesMap();
    this.oldValuesMap = new ValuesMap();
    if (StartNameServer.debugMode) {
      GNS.getLogger().finer("Constructor Primaries: " + primaryNameservers);
    }

    this.activeNameservers = initializeInitialActives(primaryNameservers, StartNameServer.minReplica, name);
    if (StartNameServer.debugMode) {
      GNS.getLogger().finer(" Name Record INITIAL ACTIVES ARE: " + activeNameservers);
    }
    this.oldActivePaxosID = name + "-1"; // initialized uniformly among primaries
    this.activePaxosID = name + "-2";

    this.totalLookupRequest = 0;
    this.totalUpdateRequest = 0;
  }

  public NameRecord(JSONObject json) throws JSONException {
    this.valuesMap = new ValuesMap();
    // extract the user keys out of the json object
    for (String key : JSONUtils.JSONArrayToArrayList(json.getJSONArray(USER_KEYS))) {
      this.valuesMap.put(key, new QueryResultValue(JSONUtils.JSONArrayToArrayList(json.getJSONArray(key))));
    }
    this.oldValuesMap = new ValuesMap(json.getJSONObject(OLDVALUESMAP));

    this.name = json.getString(NAME);
    this.timeToLive = json.getInt(TIMETOLIVE);

    this.primaryNameservers = (HashSet<Integer>) JSONUtils.JSONArrayToSetInteger(json.getJSONArray(PRIMARY_NAMESERVERS));
    this.activeNameservers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(ACTIVE_NAMESERVERS));

    this.oldActivePaxosID = json.getString(OLD_ACTIVE_PAXOS_ID);
    if (!json.has(ACTIVE_PAXOS_ID)) {
      this.activePaxosID = null;
    } else {
      this.activePaxosID = json.getString(ACTIVE_PAXOS_ID);
    }
    this.totalLookupRequest = json.getInt(TOTALLOOKUPREQUEST);
    this.totalUpdateRequest = json.getInt(TOTALUPDATEREQUEST);
  }

  /**
   * Only use for testing.
   *
   * @param name
   * @param nameRecordKey
   * @param values
   */
  public NameRecord(String name, NameRecordKey nameRecordKey, ArrayList<String> values) {
    this(name);
    this.valuesMap.put(nameRecordKey.getName(), new QueryResultValue(values));
    this.oldValuesMap.put(nameRecordKey.getName(), new QueryResultValue(values));
  }

  public synchronized JSONObject toJSONObject() throws JSONException {
    if (isLazyEval()) {
      throw new RuntimeException("Attempting to convert a lazy NameRecord to JSON");
    }
    JSONObject json = new JSONObject();
    // the reason we're putting the "user level" keys "inline" with all the other keys is
    // so that we can later access the metadata and the data using the same mechanism, namely a
    // lookup of the fields in the database. Alternatively, we could have separate storage and lookup
    // of data and metadata.
    valuesMap.addToJSONObject(json);
    json.put(USER_KEYS, new JSONArray(valuesMap.keySet()));

    json.put(OLDVALUESMAP, getOldValuesMap().toJSONObject());
    //
    json.put(NAME, getName());
    json.put(TIMETOLIVE, getTimeToLive());
    json.put(PRIMARY_NAMESERVERS, new JSONArray(getPrimaryNameservers()));
    json.put(ACTIVE_NAMESERVERS, new JSONArray(getActiveNameservers()));

    // new fields
    json.put(ACTIVE_PAXOS_ID, getActiveNameservers());
    json.put(OLD_ACTIVE_PAXOS_ID, getOldActivePaxosID());

    json.put(TOTALLOOKUPREQUEST, getTotalLookupRequest());
    json.put(TOTALUPDATEREQUEST, getTotalUpdateRequest());

    return json;
  }

  private static Set<Integer> initializeInitialActives(Set<Integer> primaryNameservers, int count, String name) {
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
   * Return the name.
   *
   * @return the name
   */
  public synchronized String getName() {
    return name;
  }

  /**
   * @return the lazyEval
   */
  public boolean isLazyEval() {
    return lazyEval;
  }

  /**
   * Implements the updating of values in the namerecord for a given key.
   *
   * Note: If the key doesn't exist the underlying calls will create it.
   *
   * That notwithstanding, code that calls this should still check for the key existing in the name record so that we can return
   * error values to the client for non-upsert situations.
   *
   * @param key
   * @param newValues - a list of the new values
   * @param oldValues - a list of the old values, can be null
   * @param operation
   * @return
   */
  public synchronized boolean updateField(String key, ArrayList<String> newValues,
          ArrayList<String> oldValues, UpdateOperation operation) {
    if (isLazyEval()) {
      //get a fresh copy of this value in case it changed elsewhere
      ArrayList<String> freshValue = recordMap.getNameRecordFieldAsArrayList(name, key);
      if (freshValue != null) {
        this.valuesMap.put(key, new QueryResultValue(freshValue));
      }
    }
    // butter our bread here - actually do the update
    boolean updated = UpdateOperation.updateValuesMap(valuesMap, key, newValues, oldValues, operation);
    //
    if (updated) {
      if (isLazyEval()) {
        recordMap.updateNameRecordListValue(name, key, new ArrayList<String>(this.valuesMap.get(key)));
        // make sure the key is in the user keys
        ArrayList<String> keys = recordMap.getNameRecordFieldAsArrayList(name, USER_KEYS);
        if (!keys.contains(key)) {
          keys.add(key);
          recordMap.updateNameRecordListValue(name, USER_KEYS, keys);
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public synchronized QueryResultValue get(String key) {
    if (isLazyEval()) {
      ArrayList<String> result = recordMap.getNameRecordFieldAsArrayList(name, key);
      if (result != null) {
        QueryResultValue value = new QueryResultValue(result);
        this.valuesMap.put(key, value);
        return value;
      } else {
        return null;
      }
    } else {
      return getValuesMap().get(key);
    }
  }

  public synchronized boolean containsKey(String key) {
    if (isLazyEval()) {
      ArrayList<String> keys = recordMap.getNameRecordFieldAsArrayList(name, USER_KEYS);
      return keys.contains(key);
    } else {
      return getValuesMap().containsKey(key);
    }
  }

  // these might be special
  public synchronized ValuesMap getValuesMap() {
    if (isLazyEval()) {
      // extract the user keys out of the json object
      ArrayList<String> keys = recordMap.getNameRecordFieldAsArrayList(name, USER_KEYS);
      for (String key : keys) {
        this.valuesMap.put(key, new QueryResultValue(recordMap.getNameRecordFieldAsArrayList(name, key)));
      }
    }
    return valuesMap;
  }

  /**
   * @param valuesMap the valuesMap to set
   */
  public void setValuesMap(ValuesMap valuesMap) {
    this.valuesMap = valuesMap;
    if (isLazyEval()) {
      for (Map.Entry<String, QueryResultValue> entry : this.valuesMap.entrySet()) {
        recordMap.updateNameRecordListValue(name, entry.getKey(), new ArrayList<String>(entry.getValue()));
      }
      recordMap.updateNameRecordListValue(name, USER_KEYS, new ArrayList<String>(valuesMap.keySet()));
    }
  }

  // lazified accessors
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
   * @param activeNameservers the activeNameservers to set
   */
  public void setActiveNameservers(Set<Integer> activeNameservers) {
    this.activeNameservers = activeNameservers;
    if (isLazyEval() && activeNameservers != null) {
      recordMap.updateNameRecordFieldAsIntegerSet(name, ACTIVE_NAMESERVERS, activeNameservers);
    }
  }

  public HashSet<Integer> getPrimaryNameservers() {
    if (isLazyEval() && primaryNameservers == null) {
      primaryNameservers = (HashSet<Integer>) recordMap.getNameRecordFieldAsIntegerSet(name, PRIMARY_NAMESERVERS);
    }
    return primaryNameservers;
  }

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
   * @return the timeToLive
   */
  public synchronized int getTimeToLive() {
    if (isLazyEval() && timeToLive == LAZYINT) {
      timeToLive = recordMap.getNameRecordFieldAsInt(name, TIMETOLIVE);
    }
    return timeToLive;
  }

  /**
   * @return the oldActivePaxosID
   */
  public String getOldActivePaxosID() {
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
   * Returns a total count on the number of lookup at this nameserver.
   */
  public synchronized int getTotalLookupRequest() { //synchronized
    if (isLazyEval() && totalLookupRequest == LAZYINT) {
      totalLookupRequest = recordMap.getNameRecordFieldAsInt(name, TOTALLOOKUPREQUEST);
    }
    return totalLookupRequest;
  }

  /**
   * @param totalLookupRequest the totalLookupRequest to set
   */
  public void setTotalLookupRequest(int totalLookupRequest) {
    this.totalLookupRequest = totalLookupRequest;
    if (isLazyEval() && totalLookupRequest != LAZYINT) {
      recordMap.updateNameRecordField(name, TOTALLOOKUPREQUEST, Integer.toString(totalLookupRequest));
    }
  }

  /**
   * Returns a total count on the number of updates at this nameserver.
   */
  public synchronized int getTotalUpdateRequest() {
    if (isLazyEval() && totalUpdateRequest == LAZYINT) {
      totalUpdateRequest = recordMap.getNameRecordFieldAsInt(name, TOTALUPDATEREQUEST);
    }
    return totalUpdateRequest;
  }

  /**
   * @param totalUpdateRequest the totalUpdateRequest to set
   */
  public void setTotalUpdateRequest(int totalUpdateRequest) {
    this.totalUpdateRequest = totalUpdateRequest;
    if (isLazyEval() && totalUpdateRequest != LAZYINT) {
      recordMap.updateNameRecordField(name, TOTALUPDATEREQUEST, Integer.toString(totalUpdateRequest));
    }
  }

  /**
   * @return the oldValuesMap
   */
  public ValuesMap getOldValuesMap() {
    if (isLazyEval() && oldValuesMap == null) {
      oldValuesMap = recordMap.getNameRecordFieldAsValuesMap(name, OLDVALUESMAP);
    }
    return oldValuesMap;
  }

  /**
   * @param oldValuesMap the oldValuesMap to set
   */
  public void setOldValuesMap(ValuesMap oldValuesMap) {
    this.oldValuesMap = oldValuesMap;
    if (isLazyEval() && oldValuesMap != null) {
      try {
        recordMap.updateNameRecordField(name, OLDVALUESMAP, oldValuesMap.toJSONObject().toString());
      } catch (JSONException e) {
        GNS.getLogger().severe("ERROR: problem convert oldValuesMap to JSONObject: " + e);
      }
    }
  }

  /**
   * Returns a copy of the active name servers set.
   */
  public synchronized Set<Integer> copyActiveNameServers() { //synchronized
    Set<Integer> set = new HashSet<Integer>();
    for (int id : getActiveNameservers()) {
      set.add(id);
    }
    return set;
  }

  /**
   * Returns true if the name record contains an active name server with the given id. False otherwise.
   *
   * @param id Active name server id
   */
  public synchronized boolean containsActiveNameServer(int id) {
    if (getActiveNameservers() == null) {
      return false;
    }
    return getActiveNameservers().contains(id);
  }

  /**
   *
   * Increments the number of lookups by 1.
   */
  public synchronized void incrementLookupRequest() {
    setTotalLookupRequest(getTotalLookupRequest() + 1);
  }

  /**
   *
   * Increments the number of updates by 1.
   */
  public synchronized void incrementUpdateRequest() {
    setTotalUpdateRequest(getTotalUpdateRequest() + 1);
  }

  /**
   * ACTIVE: checks whether paxosID is current active Paxos/oldactive paxos/neither. .
   *
   * @param paxosID
   * @return
   */
  public synchronized int getPaxosStatus(String paxosID) {
    if (getActivePaxosID() != null && getActivePaxosID().equals(paxosID)) {
      return 1; // CONSIDER TURNING THESE INTS INTO ENUMERATED VALUES!
    }
    if (getOldActivePaxosID() != null && getOldActivePaxosID().equals(paxosID)) {
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

    if (getActivePaxosID().equals(paxosID)) {
      // current set of actives has stopped.
      // copy all fields to "oldActive" variables
      // initialize "active" variables to null
      setOldActivePaxosID(paxosID);
      setActivePaxosID(null);
      setActiveNameservers(null);
      setOldValuesMap(valuesMap);
      setValuesMap(new ValuesMap());
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" Updated variables after current active stopped. ");
      }
    } else {
    }
    // this stops the old active.
    // backup current state.
  }

  /**
   * ACTIVE: When new actives are started, initialize the necessary variables.
   *
   * @param actives current set of active name servers
   * @param paxosID paxosID of the current name servers
   * @param currentValue starting value of active name servers
   */
  public synchronized void handleNewActiveStart(Set<Integer> actives, String paxosID, ValuesMap currentValue) {
    setActiveNameservers(actives);
//        this.activeRunning = true;
    setActivePaxosID(paxosID);
    setValuesMap(currentValue);
  }

  /**
   * Return
   *
   * @param oldPaxosID
   * @return
   */
  public synchronized ValuesMap getOldValues(String oldPaxosID) {
    if (oldPaxosID.equals(getOldActivePaxosID())) {
      //return oldValuesList;
      return getOldValuesMap();
    }
    return null;
  }

  /**
   *
   * Returns a String representation of this NameRecord.
   */
  @Override
  public synchronized String toString() {
    if (isLazyEval()) {
      return "NameRecord{LAZY - " + "name=" + name + '}';
    } else {
      try {
        return toJSONObject().toString();
      } catch (JSONException e) {
        return "Error printing NameRecord: " + e;
      }
    }
  }

  @Override
  public int compareTo(NameRecord d) {
    int result = (this.getName()).compareTo(d.getName());
    return result;
  }

  // test code
  public static void main(String[] args) throws Exception {
    NameServer.nodeID = 2;
    test();
    //System.exit(0);
  }

  // make this query:
  // http://127.0.0.1:8080/GNS/registerAccount?name=sally&publickey=dummy3
  private static void test() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    BasicRecordMap recordMap = new MongoRecordMap();
    NameRecord record = new NameRecord("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", recordMap);
    System.out.println("PRIMARY NS: " + record.getPrimaryNameservers());
    record.updateField("COLOR", new ArrayList<String>(Arrays.asList("Red", "Green")), null, UpdateOperation.CREATE);
    System.out.println("COLOR: " + record.get("COLOR"));
    System.out.println("CONTAINS KEY: " + record.containsKey("COLOR"));
    System.out.println("CONTAINS ACTIVE NS: " + record.containsActiveNameServer(14));
    System.out.println("TOTAL LOOKUP: " + record.getTotalLookupRequest());
    record.incrementLookupRequest();
    System.out.println("TOTAL LOOKUP: " + record.getTotalLookupRequest());
    record.setTotalLookupRequest(0);
    System.out.println("CONTAINS KEY: " + record.getTotalLookupRequest());
    System.out.println("OLD VALUES MAP: " + record.getOldValuesMap());
    record.setOldValuesMap(record.getValuesMap());
    System.out.println("OLD VALUES MAP: " + record.getOldValuesMap());
  }
}
