package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * 
 * PLEASE DO NOT DELETE THE implements Comparable<NameRecord> BELOW. IT IS NECESSARY!!!! - Westy
 */
public class NameRecord implements Comparable<NameRecord> {

  public final static ColumnField NAME = new ColumnField("nr_name", ColumnFieldType.STRING);

  public final static ColumnField ACTIVE_NAMESERVERS = new ColumnField("nr_active", ColumnFieldType.SET_INTEGER);
  public final static ColumnField PRIMARY_NAMESERVERS = new ColumnField("nr_primary", ColumnFieldType.SET_INTEGER);
  public final static ColumnField ACTIVE_PAXOS_ID = new ColumnField("nr_paxosID", ColumnFieldType.STRING);
  public final static ColumnField OLD_ACTIVE_PAXOS_ID = new ColumnField("nr_oldPaxosID", ColumnFieldType.STRING);
  public final static ColumnField TIME_TO_LIVE = new ColumnField("nr_ttl", ColumnFieldType.INTEGER);
  public final static ColumnField VALUES_MAP = new ColumnField("nr_valuesMap", ColumnFieldType.VALUES_MAP);
  public final static ColumnField OLD_VALUES_MAP = new ColumnField("nr_oldValuesMap", ColumnFieldType.VALUES_MAP);
  public final static ColumnField TOTAL_UPDATE_REQUEST = new ColumnField("nr_totalUpdate", ColumnFieldType.INTEGER);
  public final static ColumnField TOTAL_LOOKUP_REQUEST = new ColumnField("nr_totalLookup", ColumnFieldType.INTEGER);
  /**
   * This HashMap stores all the (field,value) tuples that are read from the database for this name record.
   */
  private HashMap<ColumnField, Object> hashMap;

  /********************************************
   * CONSTRUCTORS
   * ******************************************/
  /**
   * Creates a <code>NameRecord</code> object initialized with given fields. The record is in memory and not written to DB.
   * @param name
   * @param activeNameServers
   * @param activePaxosID
   * @param values
   * @return
   */
  public NameRecord(String name, Set<Integer> activeNameServers, String activePaxosID,
          ValuesMap values, int ttl) {
    hashMap = new HashMap<ColumnField, Object>();
    hashMap.put(NAME, name);
    hashMap.put(ACTIVE_NAMESERVERS, activeNameServers);
    hashMap.put(PRIMARY_NAMESERVERS, HashFunction.getPrimaryReplicas(name));
    hashMap.put(ACTIVE_PAXOS_ID, activePaxosID);
    hashMap.put(OLD_ACTIVE_PAXOS_ID, name + "-0");
    hashMap.put(TIME_TO_LIVE, ttl);
    hashMap.put(VALUES_MAP, values);
    hashMap.put(OLD_VALUES_MAP, new ValuesMap());
    hashMap.put(TOTAL_LOOKUP_REQUEST, 0);
    hashMap.put(TOTAL_UPDATE_REQUEST, 0);
  }

  /**
   * Creates a <code>NameRecord</code> object by reading fields from the JSONObject.
   * @param jsonObject
   * @return
   * @throws JSONException
   */
  public NameRecord(JSONObject jsonObject) throws JSONException {

    hashMap = new HashMap<ColumnField, Object>();
    if (jsonObject.has(NAME.getName())) {
      hashMap.put(NAME, JSONUtils.getObject(NAME, jsonObject));
    }

    if (jsonObject.has(ACTIVE_NAMESERVERS.getName())) {
      hashMap.put(ACTIVE_NAMESERVERS, JSONUtils.getObject(ACTIVE_NAMESERVERS, jsonObject));
    }

    if (jsonObject.has(PRIMARY_NAMESERVERS.getName())) {
      hashMap.put(PRIMARY_NAMESERVERS, JSONUtils.getObject(PRIMARY_NAMESERVERS, jsonObject));
    }

    if (jsonObject.has(ACTIVE_PAXOS_ID.getName())) {
      hashMap.put(ACTIVE_PAXOS_ID, JSONUtils.getObject(ACTIVE_PAXOS_ID, jsonObject));
    }

    if (jsonObject.has(OLD_ACTIVE_PAXOS_ID.getName())) {
      hashMap.put(OLD_ACTIVE_PAXOS_ID, JSONUtils.getObject(OLD_ACTIVE_PAXOS_ID, jsonObject));
    }

    if (jsonObject.has(TIME_TO_LIVE.getName())) {
      hashMap.put(TIME_TO_LIVE, JSONUtils.getObject(TIME_TO_LIVE, jsonObject));
    }

    if (jsonObject.has(VALUES_MAP.getName())) {
      hashMap.put(VALUES_MAP, JSONUtils.getObject(VALUES_MAP, jsonObject));
    }

    if (jsonObject.has(OLD_VALUES_MAP.getName())) {
      hashMap.put(VALUES_MAP, JSONUtils.getObject(VALUES_MAP, jsonObject));
    }

    if (jsonObject.has(TOTAL_LOOKUP_REQUEST.getName())) {
      hashMap.put(TOTAL_LOOKUP_REQUEST, JSONUtils.getObject(TOTAL_LOOKUP_REQUEST, jsonObject));
    }

    if (jsonObject.has(TOTAL_UPDATE_REQUEST.getName())) {
      hashMap.put(TOTAL_UPDATE_REQUEST, JSONUtils.getObject(TOTAL_UPDATE_REQUEST, jsonObject));
    }
  }

  /**
   * Constructor used by the initialize values read from database
   * @param allValues
   */
  public NameRecord(HashMap<ColumnField, Object> allValues) {
    this.hashMap = allValues;
  }

  /**
   * Creates an empty name record without checking if name exists in database.
   * @param name
   */
  public NameRecord(String name) {
    hashMap = new HashMap<ColumnField, Object>();
    hashMap.put(NAME, name);
  }

  @Override
  public String toString() {
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      e.printStackTrace();  
    }
    return null;
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    for (ColumnField f : hashMap.keySet()) {
      JSONUtils.putFieldInJsonObject(f, hashMap.get(f), jsonObject);
    }
    return jsonObject;
  }

  /********************************************
   * GETTER methods for each field in name record
   * ******************************************/
  public String getName() throws FieldNotFoundException {
    if (hashMap.containsKey(NAME)) {
      return (String) hashMap.get(NAME);
    }
    throw new FieldNotFoundException(NAME);
  }

  public Set<Integer> getActiveNameServers() throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_NAMESERVERS)) {
      return (Set<Integer>) hashMap.get(ACTIVE_NAMESERVERS);
    }
    throw new FieldNotFoundException(ACTIVE_NAMESERVERS);
  }

  public Set<Integer> getPrimaryNameservers() throws FieldNotFoundException {
    if (hashMap.containsKey(PRIMARY_NAMESERVERS)) {
      return (Set<Integer>) hashMap.get(PRIMARY_NAMESERVERS);
    }
    throw new FieldNotFoundException(PRIMARY_NAMESERVERS);
  }

  public String getActivePaxosID() throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_PAXOS_ID)) {
      return (String) hashMap.get(ACTIVE_PAXOS_ID);
    }
    throw new FieldNotFoundException(ACTIVE_PAXOS_ID);
  }

  public String getOldActivePaxosID() throws FieldNotFoundException {
    if (hashMap.containsKey(OLD_ACTIVE_PAXOS_ID)) {
      return (String) hashMap.get(OLD_ACTIVE_PAXOS_ID);
    }
    throw new FieldNotFoundException(OLD_ACTIVE_PAXOS_ID);
  }

  public int getTimeToLive() throws FieldNotFoundException {
    if (hashMap.containsKey(TIME_TO_LIVE)) {
      return (Integer) hashMap.get(TIME_TO_LIVE);
    }
    throw new FieldNotFoundException(TIME_TO_LIVE);
  }

  public ValuesMap getValuesMap() throws FieldNotFoundException {
    if (hashMap.containsKey(VALUES_MAP)) {
      return (ValuesMap) hashMap.get(VALUES_MAP);
    }
    throw new FieldNotFoundException(VALUES_MAP);
  }

  public ValuesMap getOldValuesMap() throws FieldNotFoundException {
    if (hashMap.containsKey(OLD_VALUES_MAP)) {
      return (ValuesMap) hashMap.get(OLD_VALUES_MAP);
    }
    throw new FieldNotFoundException(OLD_VALUES_MAP);
  }

  public int getTotalLookupRequest() throws FieldNotFoundException {
    if (hashMap.containsKey(TOTAL_LOOKUP_REQUEST)) {
      return (Integer) hashMap.get(TOTAL_LOOKUP_REQUEST);
    }
    throw new FieldNotFoundException(TOTAL_LOOKUP_REQUEST);
  }

  public int getTotalUpdateRequest() throws FieldNotFoundException {
    if (hashMap.containsKey(TOTAL_UPDATE_REQUEST)) {
      return (Integer) hashMap.get(TOTAL_UPDATE_REQUEST);
    }
    throw new FieldNotFoundException(TOTAL_UPDATE_REQUEST);
  }

  /********************************************
   * READ methods
   * ******************************************/
  /**
   * Checks whether the key exists in the values map for this name record.
   *
   * Call this method after reading this key from the database. If the key is not found, then name record does not exist.
   * @param key
   * @return
   * @throws FieldNotFoundException
   */
  public boolean containsKey(String key) throws FieldNotFoundException {
    if (hashMap.containsKey(VALUES_MAP)) {
      ValuesMap valuesMap = (ValuesMap) hashMap.get(VALUES_MAP);
      return valuesMap.containsKey(key);
    }
    throw new FieldNotFoundException(VALUES_MAP);
  }

  /**
   * Returns the list of values for this key, if the key exists and the key is read from database already.
   * Throws FieldNotFoundException if (1) key has not been read from database
   * or (2) key does not exist for this name record.
   *
   * Call this method only if <code>containsKey</code> returns true, otherwise return false.
   *
   * @param key
   * @return
   * @throws FieldNotFoundException
   */
  public ResultValue getKey(String key) throws FieldNotFoundException {
    if (hashMap.containsKey(VALUES_MAP)) {
      ValuesMap valuesMap = (ValuesMap) hashMap.get(VALUES_MAP);
      if (valuesMap.containsKey(key)) {
        return valuesMap.get(key);
      }
    }
    throw new FieldNotFoundException(VALUES_MAP);
  }

  public boolean containsActiveNameServer(int id) throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_NAMESERVERS)) {
      return ((Set<Integer>) hashMap.get(ACTIVE_NAMESERVERS)).contains(id);
    }
    throw new FieldNotFoundException(ACTIVE_NAMESERVERS);
  }

  /**
   * ACTIVE: checks whether paxosID is current active Paxos/oldactive paxos/neither. .
   * @param paxosID
   * @return
   * @throws FieldNotFoundException
   */
  public int getPaxosStatus(String paxosID) throws FieldNotFoundException {
    String activePaxosID = getActivePaxosID();
    String oldActivePaxosID = getOldActivePaxosID();
    if (activePaxosID != null && activePaxosID.equals(paxosID)) {
      return 1; // CONSIDER TURNING THESE INTS INTO ENUMERATED VALUES!
    }
    if (oldActivePaxosID != null && oldActivePaxosID.equals(paxosID)) {
      return 2;
    }
    return 3;

  }

  /**
   *
   * @param oldPaxosID
   * @return
   * @throws FieldNotFoundException
   */
  public ValuesMap getOldValuesOnPaxosIDMatch(String oldPaxosID) throws FieldNotFoundException {
    if (oldPaxosID.equals(getOldActivePaxosID())) {
      //return oldValuesList;
      return getOldValuesMap();
    }
    return null;
  }

  /********************************************
   * WRITE methods, these methods change one or more fields in database.
   * ******************************************/
  public void incrementLookupRequest() throws FieldNotFoundException {
    ArrayList<ColumnField> incrementFields = new ArrayList<ColumnField>();
    incrementFields.add(TOTAL_LOOKUP_REQUEST);

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(1);

    NameServer.recordMap.increment(getName(), incrementFields, values);
    // TODO implement batching
  }

  public void incrementUpdateRequest() throws FieldNotFoundException {
    ArrayList<ColumnField> incrementFields = new ArrayList<ColumnField>();
    incrementFields.add(TOTAL_UPDATE_REQUEST);

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(1);

    NameServer.recordMap.increment(getName(), incrementFields, values);
  }

  /**
   * Updates the value of the field associated with the key.
   * 
   * @param key
   * @param newValues
   * @param oldValues
   * @param operation
   * @return True if the update does anything, false otherwise.
   * @throws FieldNotFoundException 
   */
  public boolean updateKey(String key, ResultValue newValues, ResultValue oldValues,
          UpdateOperation operation) throws FieldNotFoundException {

    if (operation.equals(UpdateOperation.REMOVE_FIELD)) { // remove the field with name = key from values map.

      ArrayList<ColumnField> keys = new ArrayList<ColumnField>();
      keys.add(new ColumnField(key,ColumnFieldType.LIST_STRING));
      NameServer.recordMap.removeMapKeys(getName(), VALUES_MAP, keys);
      return true;

    }

    /* 
     * Some update operations e.g., SUBSTITUTE, require that record is first read from DB, modified, and then written. 
     * That is 1 DB read + 1 DB write. REPLACE_ALL does not require record to be read, but we can directly do a write. 
     * This saves us a database read.
     * 
     * To implement this, we require some changes to both ClientRequestWorker.updateAdddressNS and NameRecord.updateKey. 
     * Abhighyan had made both these changes but unknowingly commented out the change in ClientRequestWorker.updateAdddressNS.
     * I will uncomment it, so that a REPLACE_ALL can proceed without doing a database read.
     * There could be other operations like REPLACE_ALL which could proceed without DB read, 
     * and should be handled similar to REPLACE_ALL. In my experiments, I was using REPLACE_ALL so I have
     * included it as a special case for it.
     * 
     * Westy - I will be augmenting the UpdateOperation class with some notion of operations that don't require a read before
     * the write and then use that to redo the "if (operation.equals(UpdateOperation.REPLACE_ALL))" clause.
     */
    ValuesMap valuesMap;
    if (operation.equals(UpdateOperation.REPLACE_ALL)) {
      valuesMap = new ValuesMap();
      hashMap.put(VALUES_MAP, valuesMap);
    } else {
      valuesMap = getValuesMap(); // this will throw an exception if field is not read.
    }
    boolean updated = UpdateOperation.updateValuesMap(valuesMap, key, newValues, oldValues, operation); //this updates the values map as well
    if (updated) {
      // commit update to database
      ArrayList<ColumnField> updatedFields = new ArrayList<ColumnField>();
      updatedFields.add(new ColumnField(key, ColumnFieldType.LIST_STRING));
      ArrayList<Object> updatedValues = new ArrayList<Object>();
      updatedValues.add(valuesMap.get(key));

      NameServer.recordMap.update(getName(), NAME, null, null, VALUES_MAP, updatedFields, updatedValues);
//      valuesMap.get();
    }
    return updated;
  }

  private static ArrayList<ColumnField> currentActiveStopFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getCurrentActiveStopFields() {
    synchronized (currentActiveStopFields) {
      if (currentActiveStopFields.size() > 0) {
        return currentActiveStopFields;
      }
      currentActiveStopFields.add(OLD_ACTIVE_PAXOS_ID);
      currentActiveStopFields.add(ACTIVE_PAXOS_ID);
      currentActiveStopFields.add(ACTIVE_NAMESERVERS);
      currentActiveStopFields.add(OLD_VALUES_MAP);
      currentActiveStopFields.add(VALUES_MAP);
      return currentActiveStopFields;
    }
  }

  public void handleCurrentActiveStop(String paxosID) throws FieldNotFoundException {

    ValuesMap valuesMap = getValuesMap();

//    if (currentPaxosID != null && currentPaxosID.equals(paxosID)) {
      ArrayList<ColumnField> updateFields = getCurrentActiveStopFields();


      ArrayList<Object> updateValues = new ArrayList<Object>();
      updateValues.add(paxosID);
      updateValues.add("NULL");
      updateValues.add(new HashSet<Integer>());
      updateValues.add(valuesMap);
      updateValues.add(new ValuesMap());

      NameServer.recordMap.updateConditional(getName(), NAME, ACTIVE_PAXOS_ID, paxosID, updateFields, updateValues,
              null,null,null);

      hashMap.put(OLD_ACTIVE_PAXOS_ID, paxosID);
      hashMap.put(ACTIVE_PAXOS_ID, "NULL");
      hashMap.put(ACTIVE_NAMESERVERS, new HashSet<Integer>());
      hashMap.put(OLD_VALUES_MAP, valuesMap);
      hashMap.put(VALUES_MAP, new ValuesMap());
//    }
  }
  private static ArrayList<ColumnField> newActiveStartFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getNewActiveStartFields() {
    synchronized (newActiveStartFields) {
      if (newActiveStartFields.size() > 0) {
        return newActiveStartFields;
      }
      newActiveStartFields.add(ACTIVE_NAMESERVERS);
      newActiveStartFields.add(ACTIVE_PAXOS_ID);
      newActiveStartFields.add(VALUES_MAP);
      return newActiveStartFields;
    }
  }

  public void handleNewActiveStart(Set<Integer> actives, String paxosID, ValuesMap currentValue)
          throws FieldNotFoundException {

    ArrayList<ColumnField> updateFields = getNewActiveStartFields();

    ArrayList<Object> updateValues = new ArrayList<Object>();
    updateValues.add(actives);
    updateValues.add(paxosID);
    updateValues.add(currentValue);

    NameServer.recordMap.update(getName(), NAME, updateFields, updateValues);

    hashMap.put(ACTIVE_NAMESERVERS, actives);
    hashMap.put(ACTIVE_PAXOS_ID, paxosID);
    hashMap.put(VALUES_MAP, currentValue);

  }

  /********************************************
   * SETTER methods, these methods write to database one field in the name record.
   * ******************************************/
  public void setValuesMap(ValuesMap valuesMap) {

    throw new UnsupportedOperationException();
  }

  public void setActiveNameServers(Set<Integer> activeNameServers1) {
    throw new UnsupportedOperationException();
  }

  public void setActiveNameservers(Set<Integer> activeNameservers) {
    throw new UnsupportedOperationException();
  }

  public void setActivePaxosID(String activePaxosID) {
    throw new UnsupportedOperationException();
  }

  public void setOldActivePaxosID(String oldActivePaxosID) {
    throw new UnsupportedOperationException();
  }

  public void setTotalLookupRequest(int totalLookupRequest) {
    throw new UnsupportedOperationException();
  }

  public void setTotalUpdateRequest(int totalUpdateRequest) {
    throw new UnsupportedOperationException();
  }

  public void setOldValuesMap(ValuesMap oldValuesMap) {
    throw new UnsupportedOperationException();
  }

  /**
   * PLEASE DO NOT DELETE THE implements Comparable<NameRecord> BELOW. IT IS NECESSARY!!!! - Westy
   * 
   * @param d
   * @return 
   */
  @Override
  public int compareTo(NameRecord d) {
    int result;
    try {
      result = (this.getName()).compareTo(d.getName());
    } catch (FieldNotFoundException ex) {
      result = 0;
    }
    return result;
  }
}
