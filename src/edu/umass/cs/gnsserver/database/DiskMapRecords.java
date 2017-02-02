/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.database;

import com.mongodb.util.JSON;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnscommon.utils.JSONDotNotation;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DiskMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Uses a diskmap as the primary database with mongo as the
 * backup for when we need more NoSQL databasey features.
 *
 * See DiskMapCollection for more details.
 *
 *
 * @author westy, arun
 */
public class DiskMapRecords implements NoSQLRecords {

  private static final Logger LOGGER = Logger.getLogger(DiskMapRecords.class.getName());

  private Map<String, DiskMapCollection> collections;
  private String mongoNodeID;
  private int mongoPort;

  private DiskMapCollection getCollection(String name) {
    DiskMapCollection collection = collections.get(name);
    if (collection == null) {
      collections.put(name, collection = new DiskMapCollection(mongoNodeID, mongoPort, name));
    }
    return collection;
  }

  /**
   *
   * @param name
   * @return a disk map
   */
  public DiskMap<String, JSONObject> getMap(String name) {
    return getCollection(name).getMap();
  }

  /**
   *
   * @param name
   * @return the mongo records
   */
  public MongoRecords getMongoRecords(String name) {
    return getCollection(name).getMongoRecords();
  }

  /**
   *
   * @param nodeID
   */
  public DiskMapRecords(String nodeID) {
    this(nodeID, -1);
  }

  /**
   *
   * @param nodeID
   * @param port
   */
  public DiskMapRecords(String nodeID, int port) {
    this.collections = new ConcurrentHashMap<>();
    this.mongoNodeID = nodeID;
    this.mongoPort = port;
  }

  @Override
  public void insert(String collection, String name, JSONObject value)
          throws FailedDBOperationException, RecordExistsException {
    getMap(collection).put(name, value);
  }

  @Override
  public JSONObject lookupEntireRecord(String collection, String name)
          throws FailedDBOperationException, RecordNotFoundException {
    JSONObject record;
    if ((record = getMap(collection).get(name)) == null) {
      throw new RecordNotFoundException(name);
    }
    try {
      // Make a new object to make sure there aren't any DBObjects lurking in here
      return recursiveCopyJSONObject(record); //copyJsonObject(record);
    } catch (JSONException e) {
      throw new FailedDBOperationException(collection, name, "Unable to parse json record");
    }
  }

  /**
   * The methods below copy a JSONObject recursively without stringification while
   * converting BasicDBObject and BasicDBList as needed. As in any JSONObject, it is assumed
   * that there are no cyclic pointers.
   *
   * @param record
   * @return a JSON Object
   * @throws org.json.JSONException
   */
  private static JSONObject recursiveCopyJSONObject(JSONObject record)
          throws JSONException {
    String[] keys = JSONObject.getNames(record);
    JSONObject copy = new JSONObject();
    if (keys != null) { // oddly, empty returns null
      for (String key : JSONObject.getNames(record)) {
        copy.put(key, recursiveCopyObject(record.get(key)));
      }
    }
    return copy;
  }

  private static JSONArray recursiveCopyJSONArray(JSONArray jarray)
          throws JSONException {
    JSONArray copy = new JSONArray();
    for (int i = 0; i < jarray.length(); i++) {
      copy.put(recursiveCopyObject(jarray.get(i)));
    }
    return copy;
  }

  private static JSONArray recursiveCopyCollection(Collection<?> collection)
          throws JSONException {
    JSONArray copy = new JSONArray();
    for (Object value : collection) {
      copy.put(recursiveCopyObject(value));
    }
    return copy;
  }

  // Used in MongoRecords
  static JSONObject recursiveCopyMap(Map<String, ?> map)
          throws JSONException {
    JSONObject copy = new JSONObject();
    for (String key : map.keySet()) {
      copy.put(key, recursiveCopyObject(map.get(key)));
    }
    return copy;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Object recursiveCopyObject(Object value)
          throws JSONException {
    if (value instanceof JSONObject) {
      value = recursiveCopyJSONObject((JSONObject) value);
    } else if (value instanceof JSONArray) {
      value = recursiveCopyJSONArray((JSONArray) value);
    } else if (value instanceof Map) {
      value = recursiveCopyMap((Map<String, ?>) value);
    } else if (value instanceof Collection) {
      value = recursiveCopyCollection((Collection) value);
    }
    return value;
  }

  // for debugging
  @SuppressWarnings("unused")
  private void print(JSONObject json) throws JSONException {
    for (String key : JSONObject.getNames(json)) {
      Object obj = json.get(key);
      System.out.print(key + " : ");
      if (obj instanceof JSONObject) {
        print((JSONObject) obj);
      } else {
        System.out.print(
                obj.getClass());
      }
      System.out.println("");
    }
  }

  @Override
  public HashMap<ColumnField, Object> lookupSomeFields(String collection, String name,
          ColumnField nameField, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys)
          throws RecordNotFoundException, FailedDBOperationException {

    JSONObject record = lookupEntireRecord(collection, name);
//    LOGGER.log(Level.FINE, "Full record " + record.toString());
    HashMap<ColumnField, Object> hashMap = new HashMap<>();
    hashMap.put(nameField, name);
    if (valuesMapField != null && valuesMapKeys != null) {
      try {
        JSONObject readValuesMap = record.getJSONObject(valuesMapField.getName());
//        LOGGER.log(Level.FINE, "Read valuesMap " + readValuesMap.toString());
        ValuesMap valuesMapOut = new ValuesMap();
        for (int i = 0; i < valuesMapKeys.size(); i++) {
          String userKey = valuesMapKeys.get(i).getName();
          if (JSONDotNotation.containsFieldDotNotation(userKey, readValuesMap) == false) {
//            LOGGER.fine("valuesMap doesn't contain " + userKey);
            continue;
          }
          try {
            switch (valuesMapKeys.get(i).type()) {
              case USER_JSON:
                Object value = JSONDotNotation.getWithDotNotation(userKey, readValuesMap);
                LOGGER.log(Level.FINE,
                        "Object is {0}", new Object[]{value.toString()});
                valuesMapOut.put(userKey, value);
                break;
              case LIST_STRING:
                valuesMapOut.putAsArray(userKey,
                        JSONUtils.JSONArrayToResultValue(
                                new JSONArray(JSONDotNotation.getWithDotNotation(userKey,
                                        readValuesMap).toString())));
                break;
              default:
                LOGGER.log(Level.SEVERE,
                        "ERROR: Error: User keys field {0} is not a known type:{1}",
                        new Object[]{userKey, valuesMapKeys.get(i).type()});
                break;
            }
          } catch (JSONException e) {
            LOGGER.log(Level.SEVERE, "Error parsing json: {0}", e.getMessage());
          }
        }
        hashMap.put(valuesMapField, valuesMapOut);
      } catch (JSONException e) {
        LOGGER.log(Level.SEVERE, "Problem getting values map: {0}", e.getMessage());
      }
    }
    return hashMap;
  }

  @Override
  public boolean contains(String collection, String name) throws FailedDBOperationException {
    return getMap(collection).containsKey(name);
  }

  @Override
  public void removeEntireRecord(String collection, String name) throws FailedDBOperationException {
    LOGGER.log(Level.FINE, "Remove: {0}", name);
    getMap(collection).remove(name);
  }

  @Override
  public void updateEntireRecord(String collection, String name, ValuesMap valuesMap) throws FailedDBOperationException {
    LOGGER.log(Level.FINE, "Update record {0}/{1}", new Object[]{name, valuesMap});
    JSONObject json = new JSONObject();
    try {
      json.put(NameRecord.NAME.getName(), name);
      json.put(NameRecord.VALUES_MAP.getName(), valuesMap);
      getMap(collection).put(name, json);
    } catch (JSONException e) {

    }
  }

  @Override
  public void updateIndividualFields(String collection, String name,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys,
          ArrayList<Object> valuesMapValues) throws FailedDBOperationException {
    LOGGER.log(Level.FINE, "Update fields {0}/{1}", new Object[]{name, valuesMapKeys});
    JSONObject record;
    try {
      record = lookupEntireRecord(collection, name);
    } catch (RecordNotFoundException e) {
      throw new FailedDBOperationException(collection, name, "Record not found.");
    }
    LOGGER.log(Level.FINE, "Record before:{0}", record);
    if (record == null) {
      throw new FailedDBOperationException(collection, name, "Record is null.");
    }
    if (valuesMapField != null && valuesMapKeys != null) {
      try {
        JSONObject json = record.getJSONObject(valuesMapField.getName());
        for (int i = 0; i < valuesMapKeys.size(); i++) {
          String fieldName = valuesMapKeys.get(i).getName();
          switch (valuesMapKeys.get(i).type()) {
            case LIST_STRING:
              JSONDotNotation.putWithDotNotation(json, fieldName, valuesMapValues.get(i));
              break;
            case USER_JSON:
              JSONDotNotation.putWithDotNotation(json, fieldName, JSONParse(valuesMapValues.get(i)));
              break;
            default:
              LOGGER.log(Level.WARNING,
                      "Ignoring unknown format: {0}", valuesMapKeys.get(i).type());
              break;
          }
        }
        LOGGER.log(Level.FINE, "Json after:{0}", json);
        record.put(valuesMapField.getName(), json);
        LOGGER.log(Level.FINE, "Record after:{0}", record);
      } catch (JSONException e) {
        LOGGER.log(Level.SEVERE,
                "Problem updating json: {0}", e.getMessage());
      }
    }
    getMap(collection).put(name, record);
  }
  // not sure why the JSON.parse doesn't handle things this way but it doesn't

  private Object JSONParse(Object object) {
    if (object instanceof String || object instanceof Number) {
      return object;
    } else {
      return JSON.parse(object.toString());
    }
  }

  @Override
  public void removeMapKeys(String collection, String name,
          ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws FailedDBOperationException {
    JSONObject record = null;
    try {
      record = lookupEntireRecord(collection, name);
    } catch (RecordNotFoundException e) {
    }
    LOGGER.log(Level.FINE, "Record before:{0}", record);
    if (record == null) {
      throw new FailedDBOperationException(collection, name, "Record not found.");
    }
    if (mapField != null && mapKeys != null) {
      try {
        JSONObject json = record.getJSONObject(mapField.getName());
        LOGGER.log(Level.FINE, "Json before:{0}", json);
        for (int i = 0; i < mapKeys.size(); i++) {
          String fieldName = mapKeys.get(i).getName();
          LOGGER.log(Level.FINE, "Removing: {0}", fieldName);
          JSONDotNotation.removeWithDotNotation(fieldName, json);
        }
        LOGGER.log(Level.FINE, "Json after:{0}", json);
        record.put(mapField.getName(), json);
        LOGGER.log(Level.FINE, "Record after:{0}", record);
      } catch (JSONException e) {
        LOGGER.log(Level.SEVERE,
                "Problem updating json: {0}", e.getMessage());
      }
    }
    getMap(collection).put(name, record);
  }

  @Override
  public AbstractRecordCursor getAllRowsIterator(String collection) throws FailedDBOperationException {
    getMap(collection).commit();
    return getMongoRecords(collection).getAllRowsIterator(MongoRecords.DBNAMERECORD);
  }

  @Override
  public AbstractRecordCursor selectRecords(String collection, ColumnField valuesMapField, String key, Object value) throws FailedDBOperationException {
    getMap(collection).commit();
    return getMongoRecords(collection).selectRecords(MongoRecords.DBNAMERECORD, valuesMapField, key, value);
  }

  @Override
  public AbstractRecordCursor selectRecordsWithin(String collection, ColumnField valuesMapField, String key, String value) throws FailedDBOperationException {
    getMap(collection).commit();
    return getMongoRecords(collection).selectRecordsWithin(MongoRecords.DBNAMERECORD, valuesMapField, key, value);
  }

  @Override
  public AbstractRecordCursor selectRecordsNear(String collection, ColumnField valuesMapField, String key, String value, Double maxDistance) throws FailedDBOperationException {
    getMap(collection).commit();
    return getMongoRecords(collection).selectRecordsNear(MongoRecords.DBNAMERECORD, valuesMapField, key, value, maxDistance);
  }

  @Override
  public AbstractRecordCursor selectRecordsQuery(String collection, ColumnField valuesMapField, String query) throws FailedDBOperationException {
    getMap(collection).commit();
    return getMongoRecords(collection).selectRecordsQuery(MongoRecords.DBNAMERECORD, valuesMapField, query);
  }

  @Override
  public void createIndex(String collection, String field, String index) {
    getMap(collection).commit();
    getMongoRecords(collection).createIndex(MongoRecords.DBNAMERECORD, field, index);
  }

  @Override
  public void printAllEntries(String collection) throws FailedDBOperationException {
    getMap(collection).commit();
    getMongoRecords(collection).printAllEntries(MongoRecords.DBNAMERECORD);
  }
}
