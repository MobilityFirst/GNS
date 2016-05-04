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
import static edu.umass.cs.gnsserver.database.MongoRecords.DBNAMERECORD;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DiskMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
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
 * @author westy
 */
public class DiskMapRecords implements NoSQLRecords {

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

  public DiskMap<String, JSONObject> getMap(String name) {
    return getCollection(name).getMap();
  }

  public MongoRecords getMongoRecords(String name) {
    return getCollection(name).getMongoRecords();
  }

  public DiskMapRecords(String nodeID) {
    this(nodeID, -1);
  }

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
    return record;
  }

  @Override
  // FIXME: Why does this still have valuesMapField
  public HashMap<ColumnField, Object> lookupSomeFields(String collection, String name,
          ColumnField nameField, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys)
          throws RecordNotFoundException, FailedDBOperationException {

    JSONObject record;
    if ((record = getMap(collection).get(name)) == null) {
      throw new RecordNotFoundException(name);
    }
    DatabaseConfig.getLogger().log(Level.FINE, "Full record {0}", new Object[]{record.toString()});
    HashMap<ColumnField, Object> hashMap = new HashMap<>();
    hashMap.put(nameField, name);
    if (valuesMapField != null && valuesMapKeys != null) {
      try {
        JSONObject readValuesMap = record.getJSONObject(valuesMapField.getName());
        ValuesMap valuesMapOut = new ValuesMap();
        for (int i = 0; i < valuesMapKeys.size(); i++) {
          String userKey = valuesMapKeys.get(i).getName();
          if (containsFieldDotNotation(userKey, readValuesMap) == false) {
            DatabaseConfig.getLogger().log(Level.SEVERE, "DBObject doesn't contain {0}", userKey);
            continue;
          }
          try {
            switch (valuesMapKeys.get(i).type()) {
              case USER_JSON:
                Object value = getWithDotNotation(userKey, readValuesMap);
                DatabaseConfig.getLogger().log(Level.FINE,
                        "Object is {0}", new Object[]{value.toString()});
                valuesMapOut.put(userKey, value);
                break;
              case LIST_STRING:
                valuesMapOut.putAsArray(userKey,
                        JSONUtils.JSONArrayToResultValue(
                                new JSONArray(getWithDotNotation(userKey, readValuesMap).toString())));
                break;
              default:
                DatabaseConfig.getLogger().log(Level.SEVERE,
                        "ERROR: Error: User keys field {0} is not a known type:{1}",
                        new Object[]{userKey, valuesMapKeys.get(i).type()});
                break;
            }
          } catch (JSONException e) {
            DatabaseConfig.getLogger().log(Level.SEVERE, "Error parsing json: {0}", e.getMessage());
            e.printStackTrace();
          }
        }
        hashMap.put(valuesMapField, valuesMapOut);
      } catch (JSONException e) {
        DatabaseConfig.getLogger().severe("Problem getting values map: " + e);
      }
    }
    return hashMap;
  }

  private Object getWithDotNotation(String key, JSONObject json) throws JSONException {
    if (key.contains(".")) {
      int indexOfDot = key.indexOf(".");
      String subKey = key.substring(0, indexOfDot);
      JSONObject subJSON = (JSONObject) json.get(subKey);
      if (subJSON == null) {
        throw new JSONException(subKey + " is null");
      }
      try {
        return getWithDotNotation(key.substring(indexOfDot + 1), subJSON);
      } catch (JSONException e) {
        throw new JSONException(subKey + "." + e.getMessage());
      }
    } else {
      Object result = json.get(key);
      return result;
    }
  }

  private boolean containsFieldDotNotation(String key, JSONObject json) {
    try {
      return getWithDotNotation(key, json) != null;
    } catch (JSONException e) {
      return false;
    }
  }

  @Override
  public boolean contains(String collection, String name) throws FailedDBOperationException {
    return getMap(collection).containsKey(name);
  }

  @Override
  public void removeEntireRecord(String collection, String name) throws FailedDBOperationException {
    DatabaseConfig.getLogger().fine("Remove: " + name);
    getMap(collection).remove(name);
  }

  @Override
  public void updateEntireRecord(String collection, String name, ValuesMap valuesMap) throws FailedDBOperationException {
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
    JSONObject record = getMap(collection).get(name);
    DatabaseConfig.getLogger().fine("Record before:" + record);
    if (record == null) {
      throw new FailedDBOperationException(collection, name);
    }
    if (valuesMapField != null && valuesMapKeys != null) {
      try {
        JSONObject json = record.getJSONObject(valuesMapField.getName());
        for (int i = 0; i < valuesMapKeys.size(); i++) {
          String fieldName = valuesMapKeys.get(i).getName();
          switch (valuesMapKeys.get(i).type()) {
            case LIST_STRING:
              json.put(fieldName, valuesMapValues.get(i));
              break;
            case USER_JSON:
              json.put(fieldName, JSONParse(valuesMapValues.get(i)));
              break;
            default:
              DatabaseConfig.getLogger().log(Level.WARNING,
                      "Ignoring unknown format: {0}", valuesMapKeys.get(i).type());
              break;
          }
        }
        DatabaseConfig.getLogger().fine("Json after:" + json);
        record.put(valuesMapField.getName(), json);
        DatabaseConfig.getLogger().fine("Record after:" + record);
      } catch (JSONException e) {
        DatabaseConfig.getLogger().log(Level.SEVERE,
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
    JSONObject record = getMap(collection).get(name);
    DatabaseConfig.getLogger().fine("Record before:" + record);
    if (record == null) {
      throw new FailedDBOperationException(collection, name);
    }
    if (mapField != null && mapKeys != null) {
      try {
        JSONObject json = record.getJSONObject(mapField.getName());
        for (int i = 0; i < mapKeys.size(); i++) {
          String fieldName = mapKeys.get(i).getName();
          json.remove(fieldName);
        }
        DatabaseConfig.getLogger().fine("Json after:" + json);
        record.put(mapField.getName(), json);
        DatabaseConfig.getLogger().fine("Record after:" + record);
      } catch (JSONException e) {
        DatabaseConfig.getLogger().log(Level.SEVERE,
                "Problem updating json: {0}", e.getMessage());
      }
    }
    getMap(collection).put(name, record);
  }

  @Override
  public AbstractRecordCursor getAllRowsIterator(String collection) throws FailedDBOperationException {
    getMap(collection).commit();
    return getMongoRecords(collection).getAllRowsIterator(DBNAMERECORD);
  }

  @Override
  public AbstractRecordCursor selectRecords(String collection, ColumnField valuesMapField, String key, Object value) throws FailedDBOperationException {
    getMap(collection).commit();
    return getMongoRecords(collection).selectRecords(DBNAMERECORD, valuesMapField, key, value);
  }

  @Override
  public AbstractRecordCursor selectRecordsWithin(String collection, ColumnField valuesMapField, String key, String value) throws FailedDBOperationException {
    getMap(collection).commit();
    return getMongoRecords(collection).selectRecordsWithin(DBNAMERECORD, valuesMapField, key, value);
  }

  @Override
  public AbstractRecordCursor selectRecordsNear(String collection, ColumnField valuesMapField, String key, String value, Double maxDistance) throws FailedDBOperationException {
    getMap(collection).commit();
    return getMongoRecords(collection).selectRecordsNear(DBNAMERECORD, valuesMapField, key, value, maxDistance);
  }

  @Override
  public AbstractRecordCursor selectRecordsQuery(String collection, ColumnField valuesMapField, String query) throws FailedDBOperationException {
    getMap(collection).commit();
    return getMongoRecords(collection).selectRecordsQuery(DBNAMERECORD, valuesMapField, query);
  }

  @Override
  public void createIndex(String collection, String field, String index) {
    getMap(collection).commit();
    getMongoRecords(collection).createIndex(DBNAMERECORD, field, index);
  }

  @Override
  public void printAllEntries(String collection) throws FailedDBOperationException {
    getMap(collection).commit();
    getMongoRecords(collection).printAllEntries(DBNAMERECORD);
  }
}
