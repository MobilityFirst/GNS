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
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DiskMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class DiskMapRecords implements NoSQLRecords {

  private static final String COLLECTION_NAME = "diskmap";
  private DiskMap<String, JSONObject> map;
  private MongoRecords<String> mongoRecords;

  public DiskMapRecords(String nodeID, int port) {
    mongoRecords = new MongoRecords<>(nodeID, port);
    map = new DiskMap<String, JSONObject>(100000) {
      @Override
      public Set<String> commit(Map<String, JSONObject> toCommit) throws IOException {
        try {
          mongoRecords.bulkInsert(COLLECTION_NAME, toCommit);
        } catch (FailedDBOperationException | RecordExistsException e) {
          throw new IOException(e);
        }
        return toCommit.keySet();
      }

      @Override
      public JSONObject restore(String key) throws IOException {
        try {
          return mongoRecords.lookupEntireRecord(COLLECTION_NAME, key);
        } catch (FailedDBOperationException | RecordNotFoundException e) {
          throw new IOException(e);
        }
      }
    };
  }

  private String generateName(String collection, String name) {
    return collection + "/" + name;
  }

  @Override
  public void insert(String collection, String name, JSONObject value) throws FailedDBOperationException, RecordExistsException {
    map.put(generateName(collection, name), value);
  }

  @Override
  public JSONObject lookupEntireRecord(String collection, String name) throws FailedDBOperationException, RecordNotFoundException {
    return map.get(generateName(collection, name));
  }

  @Override
  public HashMap<ColumnField, Object> lookupSomeFields(String collectionName, String guid,
          ColumnField nameField, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys)
          throws RecordNotFoundException, FailedDBOperationException {

    JSONObject fullRecord = map.get(generateName(collectionName, guid));
    if (fullRecord == null) {
      throw new RecordNotFoundException(guid);
    }
    HashMap<ColumnField, Object> hashMap = new HashMap<>();
    hashMap.put(nameField, guid);
    if (valuesMapField != null && valuesMapKeys != null) {
      try {
        JSONObject valuesMapIn = fullRecord.getJSONObject(valuesMapField.getName());
        ValuesMap valuesMapOut = new ValuesMap();
        for (int i = 0; i < valuesMapKeys.size(); i++) {
          String userKey = valuesMapKeys.get(i).getName();
          if (containsFieldDotNotation(userKey, valuesMapIn) == false) {
            DatabaseConfig.getLogger().log(Level.FINE,
                    "DBObject doesn't contain {0}", new Object[]{userKey});

            continue;
          }
          try {
            switch (valuesMapKeys.get(i).type()) {
              case USER_JSON:
                Object value = getWithDotNotation(userKey, valuesMapIn);
                DatabaseConfig.getLogger().log(Level.FINE,
                        "Object is {0}", new Object[]{value.toString()});
                valuesMapOut.put(userKey, value);
                break;
              case LIST_STRING:
                valuesMapOut.putAsArray(userKey,
                        JSONUtils.JSONArrayToResultValue(
                                new JSONArray(getWithDotNotation(userKey, valuesMapIn).toString())));
                break;
              default:
                DatabaseConfig.getLogger().log(Level.SEVERE,
                        "ERROR: Error: User keys field {0} is not a known type:{1}",
                        new Object[]{userKey, valuesMapKeys.get(i).type()});
                break;
            }
          } catch (JSONException e) {
            DatabaseConfig.getLogger().log(Level.SEVERE, "Error parsing json: {0}", e);
            e.printStackTrace();
          }

        }
        hashMap.put(valuesMapField, valuesMapIn);
      } catch (JSONException e) {
        DatabaseConfig.getLogger().log(Level.FINE,
                "Problem getting values map: ", new Object[]{e.getMessage()});
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
    return map.containsKey(generateName(collection, name));
  }

  @Override
  public void removeEntireRecord(String collection, String name) throws FailedDBOperationException {
    map.remove(generateName(collection, name));
  }

  @Override
  public void updateEntireRecord(String collection, String name, ValuesMap valuesMap) throws FailedDBOperationException {
    map.put(generateName(collection, name), valuesMap);
  }

  @Override
  public void updateIndividualFields(String collectionName, String guid,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys,
          ArrayList<Object> valuesMapValues) throws FailedDBOperationException {
    JSONObject json = map.get(generateName(collectionName, guid));
    if (json == null) {
      throw new FailedDBOperationException(collectionName, guid);
    }
    if (valuesMapField != null && valuesMapKeys != null) {
      try {
        for (int i = 0; i < valuesMapKeys.size(); i++) {
          String fieldName = valuesMapField.getName() + "." + valuesMapKeys.get(i).getName();
          switch (valuesMapKeys.get(i).type()) {
            case LIST_STRING:
              // special case for old format
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
      } catch (JSONException e) {
        DatabaseConfig.getLogger().log(Level.SEVERE,
                "Problem updating json: {0}", e.getMessage());
      }
    }
    map.put(generateName(collectionName, guid), json);
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
  public void removeMapKeys(String collectionName, String name,
          ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws FailedDBOperationException {
    JSONObject json = map.get(generateName(collectionName, name));
    if (json == null) {
      throw new FailedDBOperationException(collectionName, name);
    }
    if (mapField != null && mapKeys != null) {
      for (int i = 0; i < mapKeys.size(); i++) {
        String fieldName = mapField.getName() + "." + mapKeys.get(i).getName();
        json.remove(fieldName);
      }
    }
    map.put(generateName(collectionName, name), json);
  }

  @Override
  public AbstractRecordCursor getAllRowsIterator(String collection) throws FailedDBOperationException {
    map.commit();
    return mongoRecords.getAllRowsIterator(COLLECTION_NAME);
  }

  @Override
  public AbstractRecordCursor selectRecords(String collectionName, ColumnField valuesMapField, String key, Object value) throws FailedDBOperationException {
    map.commit();
    return mongoRecords.selectRecords(collectionName, valuesMapField, key, value);
  }

  @Override
  public AbstractRecordCursor selectRecordsWithin(String collectionName, ColumnField valuesMapField, String key, String value) throws FailedDBOperationException {
    map.commit();
    return mongoRecords.selectRecordsWithin(collectionName, valuesMapField, key, value);
  }

  @Override
  public AbstractRecordCursor selectRecordsNear(String collectionName, ColumnField valuesMapField, String key, String value, Double maxDistance) throws FailedDBOperationException {
    map.commit();
    return mongoRecords.selectRecordsNear(collectionName, valuesMapField, key, value, maxDistance);
  }

  @Override
  public AbstractRecordCursor selectRecordsQuery(String collection, ColumnField valuesMapField, String query) throws FailedDBOperationException {
    map.commit();
    return mongoRecords.selectRecordsQuery(collection, valuesMapField, query);
  }

  @Override
  public void createIndex(String collectionName, String field, String index) {
    map.commit();
    mongoRecords.createIndex(collectionName, field, index);
  }

  @Override
  public void printAllEntries(String collection) throws FailedDBOperationException {
    map.commit();
    mongoRecords.printAllEntries(COLLECTION_NAME);
  }

}
