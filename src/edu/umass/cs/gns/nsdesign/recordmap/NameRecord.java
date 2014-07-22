package edu.umass.cs.gns.nsdesign.recordmap;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.database.AbstractRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.util.ConsistentHashing;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * 
 * PLEASE DO NOT DELETE THE implements Comparable<NameRecord> BELOW. IT IS NECESSARY!!!! - Westy
 * @author abhigyan
 */
public class NameRecord implements Comparable<NameRecord> {

  /** null value for the ACTIVE_VERSION and OLD_ACTIVE_VERSION field. */
  public static final int NULL_VALUE_ACTIVE_VERSION = 0;

  public final static ColumnField NAME = new ColumnField("nr_name", ColumnFieldType.STRING);
//  public final static ColumnField ACTIVE_NAMESERVERS = new ColumnField("nr_active", ColumnFieldType.SET_INTEGER);
  public final static ColumnField PRIMARY_NAMESERVERS = new ColumnField("nr_primary", ColumnFieldType.SET_INTEGER);
  public final static ColumnField ACTIVE_VERSION = new ColumnField("nr_version", ColumnFieldType.INTEGER);
  public final static ColumnField OLD_ACTIVE_VERSION = new ColumnField("nr_oldVersion", ColumnFieldType.INTEGER);
  public final static ColumnField TIME_TO_LIVE = new ColumnField("nr_ttl", ColumnFieldType.INTEGER);
  public final static ColumnField VALUES_MAP = new ColumnField("nr_valuesMap", ColumnFieldType.VALUES_MAP);
  public final static ColumnField OLD_VALUES_MAP = new ColumnField("nr_oldValuesMap", ColumnFieldType.VALUES_MAP);
  public final static ColumnField TOTAL_UPDATE_REQUEST = new ColumnField("nr_totalUpdate", ColumnFieldType.INTEGER);
  public final static ColumnField TOTAL_LOOKUP_REQUEST = new ColumnField("nr_totalLookup", ColumnFieldType.INTEGER);


  /**
   * This HashMap stores all the (field,value) tuples that are read from the database for this name record.
   */
  private HashMap<ColumnField, Object> hashMap;

  private BasicRecordMap recordMap;
  /********************************************
   * CONSTRUCTORS
   * ******************************************/
  /**
   * Creates a <code>NameRecord</code> object initialized with given fields. The record is in memory and not written to DB.
   * @param name
   * @param activeVersion
   * @param values
   * @return
   */
  public NameRecord(BasicRecordMap recordMap, String name, int activeVersion, ValuesMap values, int ttl) {
    this.recordMap = recordMap;

    hashMap = new HashMap<ColumnField, Object>();
    hashMap.put(NAME, name);
    hashMap.put(PRIMARY_NAMESERVERS, ConsistentHashing.getReplicaControllerSet(name));
    hashMap.put(ACTIVE_VERSION, activeVersion);
    hashMap.put(OLD_ACTIVE_VERSION, 0);
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
   * @throws org.json.JSONException
   */
  public NameRecord(BasicRecordMap recordMap, JSONObject jsonObject) throws JSONException {
    this.recordMap = recordMap;

    hashMap = new HashMap<ColumnField, Object>();
    if (jsonObject.has(NAME.getName())) {
      hashMap.put(NAME, JSONUtils.getObject(NAME, jsonObject));
    }

    if (jsonObject.has(PRIMARY_NAMESERVERS.getName())) {
      hashMap.put(PRIMARY_NAMESERVERS, JSONUtils.getObject(PRIMARY_NAMESERVERS, jsonObject));
    }

    if (jsonObject.has(ACTIVE_VERSION.getName())) {
      hashMap.put(ACTIVE_VERSION, JSONUtils.getObject(ACTIVE_VERSION, jsonObject));
    }

    if (jsonObject.has(OLD_ACTIVE_VERSION.getName())) {
      hashMap.put(OLD_ACTIVE_VERSION, JSONUtils.getObject(OLD_ACTIVE_VERSION, jsonObject));
    }

    if (jsonObject.has(TIME_TO_LIVE.getName())) {
      hashMap.put(TIME_TO_LIVE, JSONUtils.getObject(TIME_TO_LIVE, jsonObject));
    }

    if (jsonObject.has(VALUES_MAP.getName())) {
      hashMap.put(VALUES_MAP, JSONUtils.getObject(VALUES_MAP, jsonObject));
    }

    // is this correct or is it a typo and should be OLD_VALUES_MAP in the other two places?
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
  public NameRecord(BasicRecordMap recordMap, HashMap<ColumnField, Object> allValues) {
    this.recordMap = recordMap;
    this.hashMap = allValues;
  }

  /**
   * Creates an empty name record without checking if name exists in database.
   * @param name
   */
  public NameRecord(BasicRecordMap recordMap, String name) {
    this.recordMap = recordMap;
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

//  public Set<Integer> getActiveNameServers() throws FieldNotFoundException {
//    if (hashMap.containsKey(ACTIVE_NAMESERVERS)) {
//      return (Set<Integer>) hashMap.get(ACTIVE_NAMESERVERS);
//    }
//    throw new FieldNotFoundException(ACTIVE_NAMESERVERS);
//  }

  public Set<Integer> getPrimaryNameservers() throws FieldNotFoundException {
    if (hashMap.containsKey(PRIMARY_NAMESERVERS)) {
      return (Set<Integer>) hashMap.get(PRIMARY_NAMESERVERS);
    }
    throw new FieldNotFoundException(PRIMARY_NAMESERVERS);
  }

  public int getActiveVersion() throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_VERSION)) {
      return (Integer) hashMap.get(ACTIVE_VERSION);
    }
    throw new FieldNotFoundException(ACTIVE_VERSION);
  }

  public int getOldActiveVersion() throws FieldNotFoundException {
    if (hashMap.containsKey(OLD_ACTIVE_VERSION)) {
      return (Integer) hashMap.get(OLD_ACTIVE_VERSION);
    }
    throw new FieldNotFoundException(OLD_ACTIVE_VERSION);
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
   * @throws edu.umass.cs.gns.exceptions.FieldNotFoundException
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
   * @throws edu.umass.cs.gns.exceptions.FieldNotFoundException
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

//  public boolean containsActiveNameServer(int id) throws FieldNotFoundException {
//    if (hashMap.containsKey(ACTIVE_NAMESERVERS)) {
//      return ((Set<Integer>) hashMap.get(ACTIVE_NAMESERVERS)).contains(id);
//    }
//    throw new FieldNotFoundException(ACTIVE_NAMESERVERS);
//  }

  /**
   * ACTIVE: checks whether version is current active version/oldactive version/neither. .
   * @param version
   * @return
   * @throws edu.umass.cs.gns.exceptions.FieldNotFoundException
   */
  public int getVersionStatus(int version) throws FieldNotFoundException {
    int activeVersion = getActiveVersion();
    int oldActiveVersion = getOldActiveVersion();
    if (activeVersion == version) {
      return 1; // CONSIDER TURNING THESE INTS INTO ENUMERATED VALUES!
    }
    if (oldActiveVersion == version) {
      return 2;
    }
    return 3;

  }

  /**
   *
   * @param oldVersion
   * @return
   * @throws edu.umass.cs.gns.exceptions.FieldNotFoundException
   */
  public ValuesMap getOldValuesOnVersionMatch(int oldVersion) throws FieldNotFoundException {
    if (oldVersion == getOldActiveVersion()) {
      //return oldValuesList;
      return getOldValuesMap();
    }
    return null;
  }

  /********************************************
   * WRITE methods, these methods change one or more fields in database.
   * ******************************************/
  public void incrementLookupRequest() throws FieldNotFoundException, FailedDBOperationException {
    ArrayList<ColumnField> incrementFields = new ArrayList<ColumnField>();
    incrementFields.add(TOTAL_LOOKUP_REQUEST);

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(1);

    recordMap.increment(getName(), incrementFields, values);
    // TODO implement batching
  }

  public void incrementUpdateRequest() throws FieldNotFoundException, FailedDBOperationException {
    ArrayList<ColumnField> incrementFields = new ArrayList<ColumnField>();
    incrementFields.add(TOTAL_UPDATE_REQUEST);

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(1);

    recordMap.increment(getName(), incrementFields, values);
  }

  /**
   * Updates the value of the field associated with the key.
   *
   * @param key
   * @param newValues
   * @param oldValues
   * @param operation
   * @return True if the update does anything, false otherwise.
   * @throws edu.umass.cs.gns.exceptions.FieldNotFoundException
   */
  public boolean updateKey(String key, ResultValue newValues, ResultValue oldValues, int argument,
          UpdateOperation operation) throws FieldNotFoundException, FailedDBOperationException {

    // handle special case for REMOVE_FIELD operation
    
    if (operation.equals(UpdateOperation.REMOVE_FIELD)) { // remove the field with name = key from values map.

      ArrayList<ColumnField> keys = new ArrayList<ColumnField>();
      keys.add(new ColumnField(key,ColumnFieldType.LIST_STRING));
      recordMap.removeMapKeys(getName(), VALUES_MAP, keys);
      return true;

    }

    /*
     * Some update operations e.g., SUBSTITUTE, require that record is first read from DB, modified, and then written.
     * That is 1 DB read + 1 DB write. REPLACE_ALL does not require record to be read, but we can directly do a write.
     * This saves us a database read.
     *
     * To implement this, we require some changes to both ClientRequestWorker.updateAdddressNS and NameRecord.updateKey.
     * Abhigyan had made both these changes but unknowingly commented out the change in ClientRequestWorker.updateAdddressNS.
     * I will uncomment it, so that a REPLACE_ALL can proceed without doing a database read.
     * There could be other operations like REPLACE_ALL which could proceed without DB read,
     * and should be handled similar to REPLACE_ALL. In my experiments, I was using REPLACE_ALL so I have
     * included it as a special case for it.
     *
     * We should augment the UpdateOperation class with some notion of operations that don't require a read before
     * the write and then use that to redo the "if (operation.equals(UpdateOperation.REPLACE_ALL))" clause.
     */
    ValuesMap valuesMap;
    if (operation.equals(UpdateOperation.REPLACE_ALL)) {
      valuesMap = new ValuesMap();
      hashMap.put(VALUES_MAP, valuesMap);
    } else {
      valuesMap = getValuesMap(); // this will throw an exception if field is not read.
    }
    boolean updated = UpdateOperation.updateValuesMap(valuesMap, key, newValues, oldValues, argument, operation); //this updates the values map as well
    if (updated) {
      // commit update to database
      ArrayList<ColumnField> updatedFields = new ArrayList<ColumnField>();
      updatedFields.add(new ColumnField(key, ColumnFieldType.LIST_STRING));
      ArrayList<Object> updatedValues = new ArrayList<Object>();
      updatedValues.add(valuesMap.get(key));

      recordMap.update(getName(), NAME, null, null, VALUES_MAP, updatedFields, updatedValues);
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
      currentActiveStopFields.add(OLD_ACTIVE_VERSION);
      currentActiveStopFields.add(ACTIVE_VERSION);
//      currentActiveStopFields.add(ACTIVE_NAMESERVERS);
      currentActiveStopFields.add(OLD_VALUES_MAP);
      currentActiveStopFields.add(VALUES_MAP);
      return currentActiveStopFields;
    }
  }

  /**
   * @throws FieldNotFoundException
   */
  public void handleCurrentActiveStop() throws FieldNotFoundException, FailedDBOperationException {

    ValuesMap valuesMap = getValuesMap();

    ArrayList<ColumnField> updateFields = getCurrentActiveStopFields();

    ArrayList<Object> updateValues = new ArrayList<Object>();
    updateValues.add(getActiveVersion());
    updateValues.add(NULL_VALUE_ACTIVE_VERSION);
    updateValues.add(valuesMap);
    updateValues.add(new ValuesMap());

    recordMap.updateConditional(getName(), NAME, ACTIVE_VERSION, getActiveVersion(), updateFields, updateValues,
            null,null,null);
    // todo this is a conditional update: it may not be applied. therefore, fields below may not be valid.
    hashMap.put(OLD_ACTIVE_VERSION, getActiveVersion());
    hashMap.put(ACTIVE_VERSION, NULL_VALUE_ACTIVE_VERSION);
    hashMap.put(OLD_VALUES_MAP, valuesMap);
    hashMap.put(VALUES_MAP, new ValuesMap());
  }


  /**
   * @throws FieldNotFoundException
   */
  public void deleteOldState(int oldVersion) throws FieldNotFoundException, FailedDBOperationException {

    ArrayList<ColumnField> updateFields  = new ArrayList<ColumnField>();
    updateFields.add(OLD_ACTIVE_VERSION);
    updateFields.add(OLD_VALUES_MAP);

    ArrayList<Object> updateValues = new ArrayList<Object>();
    updateValues.add(NULL_VALUE_ACTIVE_VERSION);
    updateValues.add(new ValuesMap());

    recordMap.updateConditional(getName(), NAME, OLD_ACTIVE_VERSION, oldVersion, updateFields, updateValues,
            null,null,null);
    hashMap.put(OLD_ACTIVE_VERSION, NULL_VALUE_ACTIVE_VERSION);
    hashMap.put(OLD_VALUES_MAP, new ValuesMap());
  }


  private static ArrayList<ColumnField> newActiveStartFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getNewActiveStartFields() {
    synchronized (newActiveStartFields) {
      if (newActiveStartFields.size() > 0) {
        return newActiveStartFields;
      }
//      newActiveStartFields.add(ACTIVE_NAMESERVERS);
      newActiveStartFields.add(ACTIVE_VERSION);
      newActiveStartFields.add(VALUES_MAP);
      newActiveStartFields.add(TIME_TO_LIVE);
      return newActiveStartFields;
    }
  }

  public void handleNewActiveStart(int version, ValuesMap currentValue, int ttl)
          throws FieldNotFoundException, FailedDBOperationException {

    ArrayList<ColumnField> updateFields = getNewActiveStartFields();

    ArrayList<Object> updateValues = new ArrayList<Object>();
//    updateValues.add(actives);
    updateValues.add(version);
    updateValues.add(currentValue);
    updateValues.add(ttl);

    recordMap.update(getName(), NAME, updateFields, updateValues);

//    hashMap.put(ACTIVE_NAMESERVERS, actives);
    hashMap.put(ACTIVE_VERSION, version);
    hashMap.put(VALUES_MAP, currentValue);
    hashMap.put(TIME_TO_LIVE, ttl);

  }

  public void updateState(ValuesMap currentValue, int ttl) throws FieldNotFoundException, FailedDBOperationException {

    ArrayList<ColumnField> updateFields = new ArrayList<ColumnField>();
    updateFields.add(VALUES_MAP);
    updateFields.add(TIME_TO_LIVE);
    ArrayList<Object> updateValues = new ArrayList<Object>();
    updateValues.add(currentValue);
    updateValues.add(ttl);

    recordMap.update(getName(), NAME, updateFields, updateValues);

    hashMap.put(VALUES_MAP, currentValue);
    hashMap.put(TIME_TO_LIVE, ttl);

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

  public void setActiveVersion(int activeVersion) {
    throw new UnsupportedOperationException();
  }

  public void setOldActiveVersion(int oldActiveVersion) {
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
   BEGIN: static methods for reading/writing to database and iterating over records
   */

  /**
   * Load a name record from the backing database and retrieve all the fields.
   * @param name
   * @return
   * @throws edu.umass.cs.gns.exceptions.RecordNotFoundException
   */
  public static NameRecord getNameRecord(BasicRecordMap recordMap, String name) throws RecordNotFoundException, FailedDBOperationException {
    return recordMap.getNameRecord(name);
  }

  /**
   * Load a name record from the backing database and retrieve certain fields as well.
   *
   * @param name
   * @param systemFields - a list of Field structures representing "system" fields to retrieve
   * @return
   * @throws edu.umass.cs.gns.exceptions.RecordNotFoundException
   */
  public static NameRecord getNameRecordMultiField(BasicRecordMap recordMap, String name, ArrayList<ColumnField> systemFields)
          throws RecordNotFoundException, FailedDBOperationException {
    return new NameRecord(recordMap, recordMap.lookup(name, NameRecord.NAME, systemFields, NameRecord.VALUES_MAP, null));
  }

  /**
   * Load a name record from the backing database and retrieve certain fields as well.
   *
   * @param name
   * @param systemFields - a list of Field structures representing "system" fields to retrieve
   * @param userFields - a list of Field structures representing user fields to retrieve
   * @return
   * @throws edu.umass.cs.gns.exceptions.RecordNotFoundException
   */
  public static NameRecord getNameRecordMultiField(BasicRecordMap recordMap, String name, ArrayList<ColumnField> systemFields, ArrayList<ColumnField> userFields)
          throws RecordNotFoundException, FailedDBOperationException {
    return new NameRecord(recordMap, recordMap.lookup(name, NameRecord.NAME, systemFields, NameRecord.VALUES_MAP, userFields));
  }

  /**
   * Load a name record from the backing database and retrieve certain fields as well.
   *
   * @param name
   * @param systemFields
   * @param userFieldNames - strings which name the user fields to return
   * @return
   * @throws edu.umass.cs.gns.exceptions.RecordNotFoundException
   */
  public static NameRecord getNameRecordMultiField(BasicRecordMap recordMap, String name, ArrayList<ColumnField> systemFields, String... userFieldNames)
          throws RecordNotFoundException, FailedDBOperationException {
    return new NameRecord(recordMap, recordMap.lookup(name, NameRecord.NAME, systemFields, NameRecord.VALUES_MAP, userFieldList(userFieldNames)));
  }

  private static ArrayList<ColumnField> userFieldList(String... fieldNames) {
    ArrayList<ColumnField> result = new ArrayList<ColumnField>();
    for (String name : fieldNames) {
      result.add(new ColumnField(name, ColumnFieldType.LIST_STRING));
    }
    return result;
  }

  /**
   * Add this name record to DB
   * @param record
   * @throws edu.umass.cs.gns.exceptions.RecordExistsException
   */
  public static void addNameRecord(BasicRecordMap recordMap, NameRecord record) throws FailedDBOperationException, RecordExistsException {
    recordMap.addNameRecord(record);
  }

  /**
   * Replace the name record in DB with this copy of name record
   * @param record
   */
  public static void updateNameRecord(BasicRecordMap recordMap, NameRecord record) throws FailedDBOperationException {
    recordMap.updateNameRecord(record);
  }

  /**
   * Remove name record from DB
   * @param name
   */
  public static void removeNameRecord(BasicRecordMap recordMap, String name) throws FailedDBOperationException {
    recordMap.removeNameRecord(name);
  }

  /**
   * Returns an iterator for all the rows in the collection with all fields filled in.
   *
   * @return
   */
  public static AbstractRecordCursor getAllRowsIterator(BasicRecordMap recordMap) throws FailedDBOperationException {
    return recordMap.getAllRowsIterator();
  }

  /**
   * Given a key and a value return all the records as a AbstractRecordCursor that have a *user* key with that value.
   * @param key
   * @param value
   * @return
   */
  public static AbstractRecordCursor selectRecords(BasicRecordMap recordMap, String key, Object value) throws FailedDBOperationException {
    return recordMap.selectRecords(NameRecord.VALUES_MAP, key, value);
  }

  /**
   * If key is a GeoSpatial field return all fields that are within value which is a bounding box specified as a nested JSONArray
 string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]] The returned value is a AbstractRecordCursor.
   *
   * @param key
   * @param value - a string that looks like this: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return
   */
  public static AbstractRecordCursor selectRecordsWithin(BasicRecordMap recordMap, String key, String value) throws FailedDBOperationException {
    return recordMap.selectRecordsWithin(NameRecord.VALUES_MAP, key, value);
  }

  /**
   * If key is a GeoSpatial field return all fields that are near value which is a point specified as a JSONArray string tuple:
   * [LONG, LAT]. maxDistance is in meters. The returned value is a AbstractRecordCursor.
   *
   * @param key
   * @param value - a string that looks like this: [LONG, LAT]
   * @param maxDistance - the distance in meters
   * @return
   */
  public static AbstractRecordCursor selectRecordsNear(BasicRecordMap recordMap, String key, String value, Double maxDistance) throws FailedDBOperationException {
    return recordMap.selectRecordsNear(NameRecord.VALUES_MAP, key, value, maxDistance);
  }

  /**
   * Returns all fields that match the query.
   *
   * @param query
   * @return
   */
  public static AbstractRecordCursor selectRecordsQuery(BasicRecordMap recordMap, String query) throws FailedDBOperationException {
    return recordMap.selectRecordsQuery(NameRecord.VALUES_MAP, query);
  }

  /******************************
   * End of name record methods
   ******************************/

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
