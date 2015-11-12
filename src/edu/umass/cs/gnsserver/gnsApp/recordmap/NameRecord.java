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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.recordmap;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.exceptions.RecordExistsException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * The name record. 
 * It specifies what is store in the database for system and user fields.
 * This is also the in memory version of what is store in the database.
 * 
 *
 * @author abhigyan
 */
// FIXME: All this needs to be cleaned up. There are many unused system fields and
// why is this not just JSON anyway? - Westy
public class NameRecord implements Comparable<NameRecord> {

  /**
   * null value for the ACTIVE_VERSION and OLD_ACTIVE_VERSION field.
   */
  public static final int NULL_VALUE_ACTIVE_VERSION = -1;

  /**
   * NAME
   */
  public final static ColumnField NAME = new ColumnField("nr_name", ColumnFieldType.STRING);
  /**
   * PRIMARY_NAMESERVERS
   */
  public final static ColumnField PRIMARY_NAMESERVERS = new ColumnField("nr_primary", ColumnFieldType.SET_NODE_ID_STRING);
  /**
   * ACTIVE_VERSION
   */
  public final static ColumnField ACTIVE_VERSION = new ColumnField("nr_version", ColumnFieldType.INTEGER);
  /**
   * OLD_ACTIVE_VERSION
   */
  public final static ColumnField OLD_ACTIVE_VERSION = new ColumnField("nr_oldVersion", ColumnFieldType.INTEGER);
  /**
   * TIME_TO_LIVE
   */
  public final static ColumnField TIME_TO_LIVE = new ColumnField("nr_ttl", ColumnFieldType.INTEGER);
  /**
   * VALUES_MAP
   */
  public final static ColumnField VALUES_MAP = new ColumnField("nr_valuesMap", ColumnFieldType.VALUES_MAP);
  /**
   * OLD_VALUES_MAP
   */
  public final static ColumnField OLD_VALUES_MAP = new ColumnField("nr_oldValuesMap", ColumnFieldType.VALUES_MAP);
  /**
   * TOTAL_UPDATE_REQUEST
   */
  public final static ColumnField TOTAL_UPDATE_REQUEST = new ColumnField("nr_totalUpdate", ColumnFieldType.INTEGER);
  /**
   * TOTAL_LOOKUP_REQUEST
   */
  public final static ColumnField TOTAL_LOOKUP_REQUEST = new ColumnField("nr_totalLookup", ColumnFieldType.INTEGER);
  /**
   * LOOKUP_TIME - instrumentation
   */
  public final static ColumnField LOOKUP_TIME = new ColumnField("lookup_time", ColumnFieldType.INTEGER);

  /**
   * This HashMap stores all the (field,value) tuples that are read from the database for this name record.
   */
  private final HashMap<ColumnField, Object> hashMap;
  /**
   * This is the interface into the underlying database (see for example {@link MongoRecordMap}).
   */
  private final BasicRecordMap recordMap;

  /**
   * Creates a <code>NameRecord</code> object initialized with given fields. The record is in memory and not written to DB.
   *
   * @param recordMap
   * @param name
   * @param activeVersion
   * @param values
   * @param ttl
   * @param replicaControllers
   */
  public NameRecord(BasicRecordMap recordMap, String name, int activeVersion, ValuesMap values, int ttl,
          Set<String> replicaControllers) {
    this.recordMap = recordMap;

    hashMap = new HashMap<ColumnField, Object>();
    hashMap.put(NAME, name);
    hashMap.put(PRIMARY_NAMESERVERS, replicaControllers);
    hashMap.put(ACTIVE_VERSION, activeVersion);
    hashMap.put(OLD_ACTIVE_VERSION, 0);
    hashMap.put(TIME_TO_LIVE, ttl);
    hashMap.put(VALUES_MAP, values);
    hashMap.put(OLD_VALUES_MAP, new ValuesMap());
    hashMap.put(TOTAL_LOOKUP_REQUEST, 0);
    hashMap.put(TOTAL_UPDATE_REQUEST, 0);
    hashMap.put(LOOKUP_TIME, -1);

  }

  /**
   * Creates a <code>NameRecord</code> object by reading fields from the JSONObject.
   *
   * @param recordMap
   * @param jsonObject
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

    //FIXME: is this correct or is it a typo and should be OLD_VALUES_MAP in the other two places?
    if (jsonObject.has(OLD_VALUES_MAP.getName())) {
      hashMap.put(VALUES_MAP, JSONUtils.getObject(VALUES_MAP, jsonObject));
    }

    if (jsonObject.has(TOTAL_LOOKUP_REQUEST.getName())) {
      hashMap.put(TOTAL_LOOKUP_REQUEST, JSONUtils.getObject(TOTAL_LOOKUP_REQUEST, jsonObject));
    }

    if (jsonObject.has(TOTAL_UPDATE_REQUEST.getName())) {
      hashMap.put(TOTAL_UPDATE_REQUEST, JSONUtils.getObject(TOTAL_UPDATE_REQUEST, jsonObject));
    }

    if (jsonObject.has(LOOKUP_TIME.getName())) {
      hashMap.put(LOOKUP_TIME, JSONUtils.getObject(LOOKUP_TIME, jsonObject));
    }
  }

  /**
   * Constructor used by the reInitialize values read from database
   *
   * @param recordMap
   * @param allValues
   */
  public NameRecord(BasicRecordMap recordMap, HashMap<ColumnField, Object> allValues) {
    this.recordMap = recordMap;
    this.hashMap = allValues;
  }

  /**
   * Creates an empty name record without checking if name exists in database.
   *
   * @param recordMap
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
  
  /**
   * A version of toString for logging that limits the output length.
   * Obviously this can't be used to read things back in.
   * 
   * @return a string
   */
  public String toReasonableString() {
    try {
      return toJSONObject().toReasonableString();
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return null;
  }

  
  /**
   * Convert the name record to a JSON Object.
   * 
   * @return a JSONObject
   * @throws JSONException
   */
  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    for (ColumnField f : hashMap.keySet()) {
      JSONUtils.putFieldInJsonObject(f, hashMap.get(f), jsonObject);
    }
    return jsonObject;
  }

  /**
   * ******************************************
   * GETTER methods for each field in name record
   * *****************************************
   * @return 
   * @throws edu.umass.cs.gnsserver.exceptions.FieldNotFoundException 
   */
  public String getName() throws FieldNotFoundException {
    if (hashMap.containsKey(NAME)) {
      return (String) hashMap.get(NAME);
    }
    throw new FieldNotFoundException(NAME);
  }

  /**
   * Returns the primary name server.
   * 
   * @return the primary name server
   * @throws FieldNotFoundException
   */
  public Set getPrimaryNameservers() throws FieldNotFoundException {
    if (hashMap.containsKey(PRIMARY_NAMESERVERS)) {
      return (Set) hashMap.get(PRIMARY_NAMESERVERS);
    }
    throw new FieldNotFoundException(PRIMARY_NAMESERVERS);
  }

  /**
   * Returns the active version.
   * 
   * @return the active version
   * @throws FieldNotFoundException
   */
  public int getActiveVersion() throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_VERSION)) {
      return (Integer) hashMap.get(ACTIVE_VERSION);
    }
    throw new FieldNotFoundException(ACTIVE_VERSION);
  }

  /**
   * Returns the old active version.
   * 
   * @return the old active version
   * @throws FieldNotFoundException
   */
  public int getOldActiveVersion() throws FieldNotFoundException {
    if (hashMap.containsKey(OLD_ACTIVE_VERSION)) {
      return (Integer) hashMap.get(OLD_ACTIVE_VERSION);
    }
    throw new FieldNotFoundException(OLD_ACTIVE_VERSION);
  }

  /**
   * Return the TTL.
   * 
   * @return the TTL
   * @throws FieldNotFoundException
   */
  public int getTimeToLive() throws FieldNotFoundException {
    if (hashMap.containsKey(TIME_TO_LIVE)) {
      return (Integer) hashMap.get(TIME_TO_LIVE);
    }
    throw new FieldNotFoundException(TIME_TO_LIVE);
  }

  /**
   * Return the values map.
   * 
   * @return the values map
   * @throws FieldNotFoundException
   */
  public ValuesMap getValuesMap() throws FieldNotFoundException {
    if (hashMap.containsKey(VALUES_MAP)) {
      return (ValuesMap) hashMap.get(VALUES_MAP);
    }
    throw new FieldNotFoundException(VALUES_MAP);
  }

  /**
   * Return the old values map.
   * 
   * @return the old values map
   * @throws FieldNotFoundException
   */
  public ValuesMap getOldValuesMap() throws FieldNotFoundException {
    if (hashMap.containsKey(OLD_VALUES_MAP)) {
      return (ValuesMap) hashMap.get(OLD_VALUES_MAP);
    }
    throw new FieldNotFoundException(OLD_VALUES_MAP);
  }

  /**
   * Return the total lookup request instrumentation.
   * 
   * @return the total lookup request
   * @throws FieldNotFoundException
   */
  public int getTotalLookupRequest() throws FieldNotFoundException {
    if (hashMap.containsKey(TOTAL_LOOKUP_REQUEST)) {
      return (Integer) hashMap.get(TOTAL_LOOKUP_REQUEST);
    }
    throw new FieldNotFoundException(TOTAL_LOOKUP_REQUEST);
  }

  /**
   * Return the total update request instrumentation.
   * 
   * @return the total update request
   * @throws FieldNotFoundException
   */
  public int getTotalUpdateRequest() throws FieldNotFoundException {
    if (hashMap.containsKey(TOTAL_UPDATE_REQUEST)) {
      return (Integer) hashMap.get(TOTAL_UPDATE_REQUEST);
    }
    throw new FieldNotFoundException(TOTAL_UPDATE_REQUEST);
  }

  /**
   * Return the total lookup time instrumentation.
   * 
   * @return the total lookup time
   */
  public int getLookupTime() {
    try {
      if (hashMap.containsKey(LOOKUP_TIME)) {
        return (Integer) hashMap.get(LOOKUP_TIME);
      }
      throw new FieldNotFoundException(LOOKUP_TIME);
    } catch (FieldNotFoundException e) {
      GNS.getLogger().warning("Unable to read LOOKUP_TIME field: " + e);
    }
    return -1;
  }

  /**
   * ******************************************
   * READ methods
   * *****************************************
   */
  /**
   * Checks whether the key exists in the values map for this name record.
   *
   * Call this method after reading this key from the database. If the key is not found, then name record does not exist.
   *
   * @param key
   * @return true if the key exists in the values map
   * @throws edu.umass.cs.gnsserver.exceptions.FieldNotFoundException
   */
  public boolean containsKey(String key) throws FieldNotFoundException {
    if (hashMap.containsKey(VALUES_MAP)) {
      ValuesMap valuesMap = (ValuesMap) hashMap.get(VALUES_MAP);
      return valuesMap.has(key);
    }
    throw new FieldNotFoundException(VALUES_MAP);
  }

  /**
   * Returns the list of values for this key, if the key exists and the key is read from database already.
   * Throws FieldNotFoundException if (1) key has not been read from database
   * or (2) key does not exist for this name record.
   *
   * Call this method only if <code>has</code> returns true, otherwise return false.
   *
   * @param key
   * @return the list of values as a {@link ResultValue}
   * @throws edu.umass.cs.gnsserver.exceptions.FieldNotFoundException
   */
  public ResultValue getKeyAsArray(String key) throws FieldNotFoundException {
    if (hashMap.containsKey(VALUES_MAP)) {
      ValuesMap valuesMap = (ValuesMap) hashMap.get(VALUES_MAP);
      if (valuesMap.has(key)) {
        return valuesMap.getAsArray(key);
      }
    }
    throw new FieldNotFoundException(VALUES_MAP);
  }

  /**
   * Updates the value of the field associated with the key.
   *
   * @param recordKey
   * @param newValues
   * @param oldValues
   * @param argument
   * @param operation
   * @param userJSON
   * @return True if the update does anything, false otherwise.
   * @throws edu.umass.cs.gnsserver.exceptions.FieldNotFoundException
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public boolean updateNameRecord(String recordKey, ResultValue newValues, ResultValue oldValues, int argument,
          ValuesMap userJSON, UpdateOperation operation) throws FieldNotFoundException, FailedDBOperationException {

    // Handle special case for SINGLE_FIELD_REMOVE_FIELD operation
    // whose purpose is to remove the field with name = key from values map.
    if (operation.equals(UpdateOperation.SINGLE_FIELD_REMOVE_FIELD)) {

      ArrayList<ColumnField> keys = new ArrayList<ColumnField>();
      keys.add(new ColumnField(recordKey, ColumnFieldType.LIST_STRING));
      recordMap.removeMapKeys(getName(), VALUES_MAP, keys);
      return true;
    }

    /*
     * Some update operations require that record is first read from DB, modified, and then written.
     * That is 1 DB read + 1 DB write. Others do not require record to be read, but we can directly do a write.
     * This saves us a database read.
     *
     * To implement this, we changed both ClientRequestWorker.updateAdddressNS and NameRecord.updateNameRecord.
     * There could be other operations like SINGLE_FIELD_REPLACE_ALL which could proceed without DB read,
     * and should be handled similar to SINGLE_FIELD_REPLACE_ALL. In my experiments, I was using SINGLE_FIELD_REPLACE_ALL so I have
     * included it as a special case for it.
     */
    ValuesMap valuesMap;
    if (operation.isAbleToSkipRead()) {
      valuesMap = new ValuesMap();
      hashMap.put(VALUES_MAP, valuesMap);
    } else {
      valuesMap = getValuesMap(); // this will throw an exception if field is not read.
    }
    // FIXME: might want to handle this without a special case at some point
    boolean updated = UpdateOperation.USER_JSON_REPLACE.equals(operation) 
            || UpdateOperation.USER_JSON_REPLACE_OR_CREATE.equals(operation)
            ? true : 
            UpdateOperation.updateValuesMap(valuesMap, recordKey, newValues, oldValues, argument, userJSON, operation);
    if (updated) {
      // commit update to database
      ArrayList<ColumnField> updatedFields = new ArrayList<ColumnField>();
      ArrayList<Object> updatedValues = new ArrayList<Object>();
      if (userJSON != null) {
        // full userJSON (new style) update
        Iterator<?> keyIter = userJSON.keys();
        while (keyIter.hasNext()) {
          String key = (String) keyIter.next();
          try {
            updatedFields.add(new ColumnField(key, ColumnFieldType.USER_JSON));
            updatedValues.add(userJSON.get(key));
          } catch (JSONException e) {
            GNS.getLogger().severe("Unable to get " + key + " from userJSON:" + e);
          }
        }
      } else {
        // single field update
        updatedFields.add(new ColumnField(recordKey, ColumnFieldType.LIST_STRING));
        updatedValues.add(valuesMap.getAsArray(recordKey));
      }

      recordMap.update(getName(), NAME, null, null, VALUES_MAP, updatedFields, updatedValues);
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
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
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
            null, null, null);
    // todo this is a conditional update: it may not be applied. therefore, fields below may not be valid.
    hashMap.put(OLD_ACTIVE_VERSION, getActiveVersion());
    hashMap.put(ACTIVE_VERSION, NULL_VALUE_ACTIVE_VERSION);
    hashMap.put(OLD_VALUES_MAP, valuesMap);
    hashMap.put(VALUES_MAP, new ValuesMap());
  }

  /**
   * @param oldVersion
   * @throws FieldNotFoundException
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public void deleteOldState(int oldVersion) throws FieldNotFoundException, FailedDBOperationException {

    ArrayList<ColumnField> updateFields = new ArrayList<ColumnField>();
    updateFields.add(OLD_ACTIVE_VERSION);
    updateFields.add(OLD_VALUES_MAP);

    ArrayList<Object> updateValues = new ArrayList<Object>();
    updateValues.add(NULL_VALUE_ACTIVE_VERSION);
    updateValues.add(new ValuesMap());

    recordMap.updateConditional(getName(), NAME, OLD_ACTIVE_VERSION, oldVersion, updateFields, updateValues,
            null, null, null);
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

  /**
   *
   * @param version
   * @param currentValue
   * @param ttl
   * @throws FieldNotFoundException
   * @throws FailedDBOperationException
   */
  public void handleNewActiveStart(int version, ValuesMap currentValue, int ttl)
          throws FieldNotFoundException, FailedDBOperationException {

    ArrayList<ColumnField> updateFields = getNewActiveStartFields();

    ArrayList<Object> updateValues = new ArrayList<Object>();
//    updateValues.add(actives);
    updateValues.add(version);
    updateValues.add(currentValue);
    updateValues.add(ttl);

    recordMap.update(getName(), NAME, updateFields, updateValues);

//    hashMap.putAsArray(ACTIVE_NAMESERVERS, actives);
    hashMap.put(ACTIVE_VERSION, version);
    hashMap.put(VALUES_MAP, currentValue);
    hashMap.put(TIME_TO_LIVE, ttl);

  }

  /**
   *
   * @param currentValue
   * @param ttl
   * @throws FieldNotFoundException
   * @throws FailedDBOperationException
   */
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

  /**
   * ******************************************
   * SETTER methods, these methods write to database one field in the name record.
   * *****************************************
   * @param valuesMap
   */
  public void setValuesMap(ValuesMap valuesMap) {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * @param activeNameServers1
   */
  public void setActiveNameServers(Set<Integer> activeNameServers1) {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * @param activeNameservers
   */
  public void setActiveNameservers(Set<Integer> activeNameservers) {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * @param activeVersion
   */
  public void setActiveVersion(int activeVersion) {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * @param oldActiveVersion
   */
  public void setOldActiveVersion(int oldActiveVersion) {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * @param totalLookupRequest
   */
  public void setTotalLookupRequest(int totalLookupRequest) {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * @param totalUpdateRequest
   */
  public void setTotalUpdateRequest(int totalUpdateRequest) {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * @param oldValuesMap
   */
  public void setOldValuesMap(ValuesMap oldValuesMap) {
    throw new UnsupportedOperationException();
  }

  /**
   * BEGIN: static methods for reading/writing to database and iterating over records
   */
  /**
   * Load a name record from the backing database and retrieve all the fields.
   *
   * @param recordMap
   * @param name
   * @return a NameRecord
   * @throws edu.umass.cs.gnsserver.exceptions.RecordNotFoundException
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static NameRecord getNameRecord(BasicRecordMap recordMap, String name) throws RecordNotFoundException, FailedDBOperationException {
    return recordMap.getNameRecord(name);
  }

  /**
   * Load a name record from the backing database and retrieve certain fields as well.
   *
   * @param recordMap
   * @param name
   * @param systemFields - a list of Field structures representing "system" fields to retrieve
   * @return a NameRecord
   * @throws edu.umass.cs.gnsserver.exceptions.RecordNotFoundException
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static NameRecord getNameRecordMultiField(BasicRecordMap recordMap, String name, ArrayList<ColumnField> systemFields)
          throws RecordNotFoundException, FailedDBOperationException {
    return new NameRecord(recordMap, recordMap.lookupMultipleSystemAndUserFields(name, NameRecord.NAME, systemFields, NameRecord.VALUES_MAP, null));
  }

  /**
   * Load a name record from the backing database and retrieve certain fields as well.
   *
   * @param recordMap
   * @param name
   * @param systemFields - a list of Field structures representing "system" fields to retrieve
   * @param userFields - a list of Field structures representing user fields to retrieve
   * @return a NameRecord
   * @throws edu.umass.cs.gnsserver.exceptions.RecordNotFoundException
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static NameRecord getNameRecordMultiField(BasicRecordMap recordMap, String name, ArrayList<ColumnField> systemFields, ArrayList<ColumnField> userFields)
          throws RecordNotFoundException, FailedDBOperationException {
    return new NameRecord(recordMap, recordMap.lookupMultipleSystemAndUserFields(name, NameRecord.NAME, systemFields, NameRecord.VALUES_MAP, userFields));
  }

  /**
   * Load a name record from the backing database and retrieve certain fields as well.
   *
   * @param recordMap
   * @param name
   * @param systemFields
   * @param returnType - the format which the returned data should be in
   * @param userFieldNames - strings which name the user fields to return
   * @return a NameRecord
   * @throws edu.umass.cs.gnsserver.exceptions.RecordNotFoundException
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static NameRecord getNameRecordMultiField(BasicRecordMap recordMap, String name, ArrayList<ColumnField> systemFields,
          ColumnFieldType returnType, String... userFieldNames)
          throws RecordNotFoundException, FailedDBOperationException {
    return new NameRecord(recordMap, recordMap.lookupMultipleSystemAndUserFields(name, NameRecord.NAME, systemFields, NameRecord.VALUES_MAP,
            userFieldList(returnType, userFieldNames)));
  }

  private static ArrayList<ColumnField> userFieldList(ColumnFieldType returnType, String... fieldNames) {
    ArrayList<ColumnField> result = new ArrayList<ColumnField>();
    for (String fieldName : fieldNames) {
      result.add(new ColumnField(fieldName, returnType));
    }
    return result;
  }

  /**
   * Add this name record to DB
   *
   * @param recordMap
   * @param record
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   * @throws edu.umass.cs.gnsserver.exceptions.RecordExistsException
   */
  public static void addNameRecord(BasicRecordMap recordMap, NameRecord record) throws FailedDBOperationException, RecordExistsException {
    recordMap.addNameRecord(record);
  }

  /**
   * Replace the name record in DB with this copy of name record
   *
   * @param recordMap
   * @param record
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static void updateNameRecord(BasicRecordMap recordMap, NameRecord record) throws FailedDBOperationException {
    recordMap.updateNameRecord(record);
  }

  /**
   * Remove name record from DB
   *
   * @param recordMap
   * @param name
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static void removeNameRecord(BasicRecordMap recordMap, String name) throws FailedDBOperationException {
    recordMap.removeNameRecord(name);
  }

  /**
   * Returns an iterator for all the rows in the collection with all fields filled in.
   *
   * @param recordMap
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static AbstractRecordCursor getAllRowsIterator(BasicRecordMap recordMap) throws FailedDBOperationException {
    return recordMap.getAllRowsIterator();
  }

  /**
   * Given a key and a value return all the records as a AbstractRecordCursor that have a *user* key with that value.
   *
   * @param recordMap
   * @param key
   * @param value
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static AbstractRecordCursor selectRecords(BasicRecordMap recordMap, String key, Object value) throws FailedDBOperationException {
    return recordMap.selectRecords(NameRecord.VALUES_MAP, key, value);
  }

  /**
   * If key is a GeoSpatial field return all fields that are within value which is a bounding box specified as a nested JSONArray
   * string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]] The returned value is a AbstractRecordCursor.
   *
   * @param recordMap
   * @param key
   * @param value - a string that looks like this: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static AbstractRecordCursor selectRecordsWithin(BasicRecordMap recordMap, String key, String value) throws FailedDBOperationException {
    return recordMap.selectRecordsWithin(NameRecord.VALUES_MAP, key, value);
  }

  /**
   * If key is a GeoSpatial field return all fields that are near value which is a point specified as a JSONArray string tuple:
   * [LONG, LAT]. maxDistance is in meters. The returned value is a AbstractRecordCursor.
   *
   * @param recordMap
   * @param key
   * @param value - a string that looks like this: [LONG, LAT]
   * @param maxDistance - the distance in meters
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static AbstractRecordCursor selectRecordsNear(BasicRecordMap recordMap, String key, String value, Double maxDistance) throws FailedDBOperationException {
    return recordMap.selectRecordsNear(NameRecord.VALUES_MAP, key, value, maxDistance);
  }

  /**
   * Returns all fields that match the query.
   *
   * @param recordMap
   * @param query
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static AbstractRecordCursor selectRecordsQuery(BasicRecordMap recordMap, String query) throws FailedDBOperationException {
    return recordMap.selectRecordsQuery(NameRecord.VALUES_MAP, query);
  }

  /**
   * PLEASE DO NOT DELETE THE THIS. IT IS NECESSARY!!!! - Westy
   *
   * @param d
   * @return a positive integer if this record follows the argument record; zero if the strings are equal
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
