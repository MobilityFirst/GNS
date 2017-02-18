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
package edu.umass.cs.gnsserver.gnsapp.recordmap;

import edu.umass.cs.gigapaxos.interfaces.Summarizable;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;

/**
 * The name record.
 * It specifies what is store in the database for system and user fields.
 * This is also the in memory version of what is store in the database.
 *
 * Probably could just be a JSON Object.
 *
 *
 * @author abhigyan
 */
public class NameRecord implements Comparable<NameRecord>, Summarizable {

  /**
   * NAME
   */
  public final static ColumnField NAME = new ColumnField("nr_name", ColumnFieldType.STRING);
  /**
   * VALUES_MAP
   */
  public final static ColumnField VALUES_MAP = new ColumnField("nr_valuesMap", ColumnFieldType.VALUES_MAP);
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
   * @param values
   */
  public NameRecord(BasicRecordMap recordMap, String name, ValuesMap values) {
    this.recordMap = recordMap;

    hashMap = new HashMap<>();
    hashMap.put(NAME, name);
    hashMap.put(VALUES_MAP, values);

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
    hashMap = new HashMap<>();
    if (jsonObject.has(NAME.getName())) {
      hashMap.put(NAME, jsonObject.getString(NAME.getName()));
    }
    if (jsonObject.has(VALUES_MAP.getName())) {
      hashMap.put(VALUES_MAP, new ValuesMap(jsonObject.getJSONObject(VALUES_MAP.getName())));
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
    hashMap = new HashMap<>();
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
      jsonObject.put(f.getName(), hashMap.get(f));
    }
    return jsonObject;
  }

  /**
   * Return the name.
   *
   * @return the name
   * @throws edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException
   */
  public String getName() throws FieldNotFoundException {
    if (hashMap.containsKey(NAME)) {
      return (String) hashMap.get(NAME);
    }
    throw new FieldNotFoundException(NAME);
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
   * ******************************************
   * READ methods
   * *****************************************
   */
  /**
   * Checks whether the key exists in the values map for this name record.
   *
   * Call this method after reading this key from the database.
   * If the key is not found, then field does not exist.
   *
   * @param key
   * @return true if the key exists in the values map
   * @throws edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException
   */
  public boolean containsUserKey(String key) throws FieldNotFoundException {
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
   * @throws edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException
   */
  public ResultValue getUserKeyAsArray(String key) throws FieldNotFoundException {
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
   * @return True if the updateEntireValuesMap does anything, false otherwise.
   * @throws edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public boolean updateNameRecord(String recordKey, ResultValue newValues, ResultValue oldValues, int argument,
          ValuesMap userJSON, UpdateOperation operation) throws FieldNotFoundException, FailedDBOperationException {

    // Handle special case for SINGLE_FIELD_REMOVE_FIELD operation
    // whose purpose is to remove the field with name = key from values map.
    if (operation.equals(UpdateOperation.SINGLE_FIELD_REMOVE_FIELD)) {
      
      ArrayList<ColumnField> keys = new ArrayList<>();
      keys.add(new ColumnField(recordKey, ColumnFieldType.LIST_STRING));
       GNSConfig.getLogger().log(Level.FINE,
                    "<============>REMOVE {0} from {1}<============>", new Object[]{recordKey, getName()});
      recordMap.removeMapKeys(getName(), VALUES_MAP, keys);
      return true;
    }

    /*
     * Some updateEntireValuesMap operations require that record is first read from DB, modified, and then written.
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
            ? true
            : UpdateOperation.updateValuesMap(valuesMap, recordKey, newValues, oldValues, argument, userJSON, operation);
    if (updated) {
      // commit updateEntireValuesMap to database
      ArrayList<ColumnField> updatedFields = new ArrayList<>();
      ArrayList<Object> updatedValues = new ArrayList<>();
      if (userJSON != null) {
        // full userJSON (new style) updateEntireValuesMap
        Iterator<?> keyIter = userJSON.keys();
        while (keyIter.hasNext()) {
          String key = (String) keyIter.next();
          try {
            updatedFields.add(new ColumnField(key, ColumnFieldType.USER_JSON));
            updatedValues.add(userJSON.get(key));
          } catch (JSONException e) {
            GNSConfig.getLogger().log(Level.SEVERE,
                    "Unable to get {0} from userJSON:{1}", new Object[]{key, e});
          }
        }
      } else {
        // single field updateEntireValuesMap
        updatedFields.add(new ColumnField(recordKey, ColumnFieldType.LIST_STRING));
        updatedValues.add(valuesMap.getAsArray(recordKey));
      }

      recordMap.updateIndividualFields(getName(), updatedFields, updatedValues);
    }
    return updated;
  }

  /**
   *
   * @param valuesMap
   * @throws FieldNotFoundException
   * @throws FailedDBOperationException
   */
  public void updateState(ValuesMap valuesMap) throws FieldNotFoundException, FailedDBOperationException {
    recordMap.updateEntireValuesMap(getName(), valuesMap);
    hashMap.put(VALUES_MAP, valuesMap);
  }

//BEGIN: static methods for reading/writing to database and iterating over records
//
  /**
   * Returns true if the database contains a name record with the given name.
   *
   * @param recordMap
   * @param name
   * @return true if the database contains a name record
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static boolean containsRecord(BasicRecordMap recordMap, String name)
          throws FailedDBOperationException {
    return recordMap.containsName(name);
  }

  /**
   * Load a name record from the backing database and retrieve all the fields.
   *
   * @param recordMap
   * @param name
   * @return a NameRecord
   * @throws edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static NameRecord getNameRecord(BasicRecordMap recordMap, String name)
          throws RecordNotFoundException, FailedDBOperationException {
    try {
      JSONObject json = recordMap.lookupEntireRecord(name);
      if (json != null) {
        return new NameRecord(recordMap, json);
      }
    } catch (JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE, "Error getting name record {0}: {1}", new Object[]{name, e});
    }
    return null;

  }

  /**
   * Load a name record from the backing database and retrieve certain fields as well.
   *
   * @param recordMap
   * @param name
   * @param returnType - the format which the returned data should be in
   * @param userFieldNames - strings which name the user fields to return
   * @return a NameRecord
   * @throws edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static NameRecord getNameRecordMultiUserFields(BasicRecordMap recordMap, String name,
          ColumnFieldType returnType, String... userFieldNames)
          throws RecordNotFoundException, FailedDBOperationException {
    return new NameRecord(recordMap, recordMap.lookupUserFields(name,
            NameRecord.NAME, NameRecord.VALUES_MAP,
            userFieldList(returnType, userFieldNames)));
  }

  private static ArrayList<ColumnField> userFieldList(ColumnFieldType returnType, String... fieldNames) {
    ArrayList<ColumnField> result = new ArrayList<>();
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
   * @throws org.json.JSONException
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   * @throws edu.umass.cs.gnscommon.exceptions.server.RecordExistsException
   */
  public static void addNameRecord(BasicRecordMap recordMap, NameRecord record) throws JSONException, FailedDBOperationException, RecordExistsException {
    recordMap.addRecord(record.toJSONObject());
  }

  /**
   * Remove name record from DB
   *
   * @param recordMap
   * @param name
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static void removeNameRecord(BasicRecordMap recordMap, String name) throws FailedDBOperationException {
    recordMap.removeRecord(name);
  }

  /**
   * Returns an iterator for all the rows in the collection with all fields filled in.
   *
   * @param recordMap
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
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
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
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
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
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
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
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
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
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

  /**
   *
   * @return the summary object
   */
  @Override
  public Object getSummary() {
    return Util.truncate(NameRecord.this, 64, 64);
  }

}
