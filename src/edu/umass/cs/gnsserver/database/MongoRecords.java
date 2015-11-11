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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.database;

/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved
 */
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.RecordExistsException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Provides insert, update, removeEntireRecord and lookupEntireRecord operations for 
 * guid, key, record triples using JSONObjects as the intermediate representation.
 * All records are stored in a document called NameRecord.
 *
 * @author westy, Abhigyan
 * @param <NodeIDType>
 */
public class MongoRecords<NodeIDType> implements NoSQLRecords {

  private static final String DBROOTNAME = "GNS";
  /**
   * The name of the document where name records are stored.
   */
  public static final String DBNAMERECORD = "NameRecord";

  private DB db;
  private String dbName;

  private MongoClient mongoClient;
  private MongoCollectionSpecs mongoCollectionSpecs;
  private boolean debuggingEnabled = false;

  /**
   * Creates database tables for nodeID, by connecting to mongoDB on default port.
   *
   * @param nodeID nodeID of name server
   */
  public MongoRecords(NodeIDType nodeID) {
    this(nodeID, -1);
  }

  /**
   * Creates database tables for nodeID, by connecting to mongoDB on given port.
   *
   * @param nodeID nodeID of name server
   * @param port port at which mongo is running. if port = -1, mongo connects to default port.
   */
  public MongoRecords(NodeIDType nodeID, int port) {
    init(nodeID, port);
  }

  private void init(NodeIDType nodeID, int mongoPort) {
    mongoCollectionSpecs = new MongoCollectionSpecs();
    mongoCollectionSpecs.addCollectionSpec(DBNAMERECORD, NameRecord.NAME);
    // add location as another index
    mongoCollectionSpecs.getCollectionSpec(DBNAMERECORD)
            .addOtherIndex(new BasicDBObject(NameRecord.VALUES_MAP.getName() + "." + GnsProtocol.LOCATION_FIELD_NAME, "2d"));
    // The good thing is that indexes are not required for 2dsphere fields, but they will make things faster
    mongoCollectionSpecs.getCollectionSpec(DBNAMERECORD)
            .addOtherIndex(new BasicDBObject(NameRecord.VALUES_MAP.getName() + "." + GnsProtocol.LOCATION_FIELD_NAME_2D_SPHERE, "2dsphere"));
    mongoCollectionSpecs.getCollectionSpec(DBNAMERECORD)
            .addOtherIndex(new BasicDBObject(NameRecord.VALUES_MAP.getName() + "." + GnsProtocol.IPADDRESS_FIELD_NAME, 1));
    try {
      // use a unique name in case we have more than one on a machine (need to remove periods, btw)
      dbName = DBROOTNAME + "-" + nodeID.toString().replace('.', '-');
      //MongoClient mongoClient;
      //MongoCredential credential = MongoCredential.createMongoCRCredential("admin", dbName, "changeit".toCharArray());
      if (mongoPort > 0) {
        //mongoClient = new MongoClient(new ServerAddress("localhost", mongoPort), Arrays.asList(credential));
        mongoClient = new MongoClient("localhost", mongoPort);
      } else {
        mongoClient = new MongoClient("localhost");
      }
      db = mongoClient.getDB(dbName);

      initializeIndexes();
    } catch (UnknownHostException e) {
      GNS.getLogger().severe("Unable to open Mongo DB: " + e);
    }
  }

  private void initializeIndexes() {
    for (MongoCollectionSpec spec : mongoCollectionSpecs.allCollectionSpecs()) {
      initializeIndex(spec.getName());
    }
  }

  private void initializeIndex(String collectionName) {
    MongoCollectionSpec spec = mongoCollectionSpecs.getCollectionSpec(collectionName);
    db.getCollection(spec.getName()).createIndex(spec.getPrimaryIndex(), new BasicDBObject("unique", true));
    for (BasicDBObject index : spec.getOtherIndexes()) {
      db.getCollection(spec.getName()).createIndex(index);
    }
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().info("Indexes initialized");
    }
  }

  @Override
  public void reset(String collectionName) throws FailedDBOperationException {
    if (mongoCollectionSpecs.getCollectionSpec(collectionName) != null) {
      db.requestStart();
      try {
        db.requestEnsureConnection();
        db.getCollection(collectionName).dropIndexes();
        db.getCollection(collectionName).drop();
        GNS.getLogger().info("MONGO DB RESET. DBNAME: " + dbName + " Collection name: " + collectionName);

        // IMPORTANT... recreate the indices
        initializeIndex(collectionName);
      } catch (MongoException e) {
        throw new FailedDBOperationException(collectionName, "reset");
      } finally {
        db.requestDone();
      }
    } else {
      GNS.getLogger().severe("MONGO DB: No collection named: " + collectionName);
    }
  }

  @Override
  public void insert(String collectionName, String guid, JSONObject value) throws FailedDBOperationException, RecordExistsException {
    db.requestStart();
    try {
      db.requestEnsureConnection();

      DBCollection collection = db.getCollection(collectionName);
      DBObject dbObject = (DBObject) JSON.parse(value.toString());
      try {
        collection.insert(dbObject);
      } catch (DuplicateKeyException e) {
        throw new RecordExistsException(collectionName, guid);
      } catch (MongoException e) {
        throw new FailedDBOperationException(collectionName, dbObject.toString());
      }
    } finally {
      db.requestDone();
    }
  }

  @Override
  public void bulkInsert(String collectionName, ArrayList<JSONObject> values) throws FailedDBOperationException, RecordExistsException {

    DBCollection collection = db.getCollection(collectionName);
    ArrayList<DBObject> dbObjects = new ArrayList<DBObject>();
    for (JSONObject json : values) {
      dbObjects.add((DBObject) JSON.parse(json.toString()));
    }
    try {
      collection.insert(dbObjects);
    } catch (DuplicateKeyException e) {
      throw new RecordExistsException(collectionName, "MultiInsert");
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, dbObjects.toString());
    }
  }

  @Override
  public JSONObject lookupEntireRecord(String collectionName, String guid) throws RecordNotFoundException, FailedDBOperationException {
    return lookupEntireRecord(collectionName, guid, false);
  }

  private JSONObject lookupEntireRecord(String collectionName, String guid, boolean explain) throws RecordNotFoundException, FailedDBOperationException {
    long startTime = System.currentTimeMillis();
    db.requestStart();
    try {
      String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      DBCursor cursor = collection.find(query);
      if (explain) {
        System.out.println(cursor.explain().toString());
      }
      if (cursor.hasNext()) {
        DBObject obj = cursor.next();
        JSONObject json = new JSONObject(obj.toString());
        // instrumentation
        DelayProfiler.updateDelay("lookupEntireRecord", startTime);
        // older style
        int lookupTime = (int) (System.currentTimeMillis() - startTime);
        if (debuggingEnabled && lookupTime > 20) {
          GNS.getLogger().warning(" mongoLookup Long delay " + lookupTime);
        }
        // instrumentation
        json.put(NameRecord.LOOKUP_TIME.getName(), lookupTime);
        return json;
      } else {
        throw new RecordNotFoundException(guid);
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("Unable to parse JSON: " + e);
      return null;
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, guid);
    } finally {
      db.requestDone();
    }
  }

  @Override
  public HashMap<ColumnField, Object> lookupMultipleSystemFields(String collectionName, String guid, ColumnField nameField,
          ArrayList<ColumnField> fields1)
          throws RecordNotFoundException, FailedDBOperationException {
    return lookupMultipleSystemAndUserFields(collectionName, guid, nameField, fields1, null, null);
  }

  @Override
  public HashMap<ColumnField, Object> lookupMultipleSystemAndUserFields(String collectionName, String guid, ColumnField nameField,
          ArrayList<ColumnField> systemFields, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys)
          throws RecordNotFoundException, FailedDBOperationException {
    long startTime = System.currentTimeMillis();
    if (guid == null) {
      GNS.getLogger().fine("GUID is null: " + guid);
      throw new RecordNotFoundException(guid);
    }
    db.requestStart();
    try {
      String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();

      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      BasicDBObject projection = new BasicDBObject().append("_id", 0);
      if (systemFields != null) {
        for (ColumnField f : systemFields) {
          projection.append(f.getName(), 1);
        }
      }

      if (valuesMapField != null && valuesMapKeys != null) {
        for (int i = 0; i < valuesMapKeys.size(); i++) {
          String fieldName = valuesMapField.getName() + "." + valuesMapKeys.get(i).getName();
          projection.append(fieldName, 1);
        }
      }

      DelayProfiler.updateDelay("lookupMSAUFPreFind", startTime);
      long findStartTime = System.currentTimeMillis();
      DBObject dbObject = collection.findOne(query, projection);
      DelayProfiler.updateDelay("lookupMSAUFJustFind", findStartTime);
      long postFindStartTime = System.currentTimeMillis();
      if (dbObject == null) {
        throw new RecordNotFoundException(guid);
      }
      HashMap<ColumnField, Object> hashMap = new HashMap<ColumnField, Object>();
      hashMap.put(nameField, guid);// put the name in the hashmap!! very important!!
      // put all the system fields in the hashmap for the name record
      ColumnFieldType.populateHashMap(hashMap, dbObject, systemFields);
      // prepare to return the user values
      if (valuesMapField != null && valuesMapKeys != null) {
        // first we pull all the user values from the dbObject and put in a bson object
        // FIXME: Why not convert this to a JSONObject right now? We know that's what it is.
        BasicDBObject bson = (BasicDBObject) dbObject.get(valuesMapField.getName());
        if (debuggingEnabled) {
          GNS.getLogger().info("@@@@@@@@ " + bson.toString());
        }
        // then we run thru each userkey in the valuesMapKeys and pull the
        // value put stuffing it into the values map
        ValuesMap valuesMap = new ValuesMap();
        for (int i = 0; i < valuesMapKeys.size(); i++) {
          String userKey = valuesMapKeys.get(i).getName();
          if (containsFieldDotNotation(userKey, bson) == false) {
            if (debuggingEnabled) {
              GNS.getLogger().info("DBObject doesn't contain " + userKey);
            }
            continue;
          }
          try {
            switch (valuesMapKeys.get(i).type()) {
              case USER_JSON:
                Object value = getWithDotNotation(userKey, bson);
                if (debuggingEnabled) {
                  GNS.getLogger().info("Object is " + value.toString());
                }
                valuesMap.put(userKey, value);
                break;
              case LIST_STRING:
                valuesMap.putAsArray(userKey, JSONUtils.JSONArrayToResultValue(new JSONArray(getWithDotNotation(userKey, bson).toString())));
                //valuesMap.putAsArray(userKey, JSONUtils.JSONArrayToResultValue(new JSONArray(bson.toString(userKey).toString())));
                break;
              default:
                GNS.getLogger().severe("ERROR: Error: User keys field " + userKey + " is not a known type:" + valuesMapKeys.get(i).type());
                break;
            }
          } catch (JSONException e) {
            GNS.getLogger().severe("Error parsing json: " + e);
            e.printStackTrace();
          }
        }
        hashMap.put(valuesMapField, valuesMap);
      }
      DelayProfiler.updateDelay("lookupMSAUFPostFind", postFindStartTime);
      // instrumentation
      DelayProfiler.updateDelay("lookupMSAUF", startTime);
      // older style
      int lookupTime = (int) (System.currentTimeMillis() - startTime);
      if (debuggingEnabled && lookupTime > 20) {
        GNS.getLogger().warning(" mongoLookup Long delay " + lookupTime);
      }
      hashMap.put(NameRecord.LOOKUP_TIME, lookupTime);
      return hashMap;
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, guid);
    } finally {
      db.requestDone();
    }
  }

  private Object getWithDotNotation(String key, BasicDBObject bson) throws JSONException {
//    if (Config.debugMode) {
//      GNS.getLogger().info("###fullkey=" + key + " bson=" + bson);
//    }
    if (key.contains(".")) {
      int indexOfDot = key.indexOf(".");
      String subKey = key.substring(0, indexOfDot);
//      if (Config.debugMode) {
//        GNS.getLogger().info("###subkey=" + subKey);
//      }
      BasicDBObject subBson = (BasicDBObject) bson.get(subKey);
      if (subBson == null) {
//        if (Config.debugMode) {
//          GNS.getLogger().info("### " + subKey + " is null");
//        }
        throw new JSONException(subKey + " is null");
      }
      try {
        return getWithDotNotation(key.substring(indexOfDot + 1), subBson);
      } catch (JSONException e) {
        throw new JSONException(subKey + "." + e.getMessage());
      }
    } else {
      Object result = bson.get(key);
//      if (Config.debugMode) {
//        GNS.getLogger().info("###result=" + result);
//      }
      return result;
    }
  }

  private boolean containsFieldDotNotation(String key, BasicDBObject bson) {
    try {
      return getWithDotNotation(key, bson) != null;
    } catch (JSONException e) {
      return false;
    }
  }

  @Override
  public boolean contains(String collectionName, String guid) throws FailedDBOperationException {
    db.requestStart();
    try {
      String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      DBCursor cursor = null;

      cursor = collection.find(query);

      if (cursor.hasNext()) {
        return true;
      } else {
        return false;
      }
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, guid);
    } finally {
      db.requestDone();
    }
  }

  @Override
  public void removeEntireRecord(String collectionName, String guid) throws FailedDBOperationException {
    String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
    try {
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      collection.remove(query);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, guid);
    }
  }

  @Override
  public void update(String collectionName, String guid, JSONObject value) throws FailedDBOperationException {
    String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
    DBCollection collection = db.getCollection(collectionName);
    BasicDBObject query = new BasicDBObject(primaryKey, guid);
    DBObject dbObject = (DBObject) JSON.parse(value.toString());
    try {
      collection.update(query, dbObject);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, dbObject.toString());
    }
  }

  @Override
  public void updateField(String collectionName, String guid, String key, Object object) throws FailedDBOperationException {
    String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
    DBCollection collection = db.getCollection(collectionName);
    BasicDBObject query = new BasicDBObject(primaryKey, guid);
    BasicDBObject newValue = new BasicDBObject(key, object);
    BasicDBObject updateOperator = new BasicDBObject("$set", newValue);
    try {
      collection.update(query, updateOperator);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, updateOperator.toString());
    }
  }

  @Override
  public void update(String collectionName, String guid, ColumnField nameField,
          ArrayList<ColumnField> fields1, ArrayList<Object> values1) throws FailedDBOperationException {
    update(collectionName, guid, nameField, fields1, values1, null, null, null);
  }

  @Override
  // THE ONLY METHOD THAT CURRENTLY SUPPORTS WRITING USER JSON OBJECTS AS VALUES IN THE VALUES MAP
  // ALSO SUPPORTS DOT NOTATION
  public void update(String collectionName, String guid, ColumnField nameField, ArrayList<ColumnField> systemFields,
          ArrayList<Object> systemValues, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys,
          ArrayList<Object> valuesMapValues) throws FailedDBOperationException {
    String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
    DBCollection collection = db.getCollection(collectionName);
    BasicDBObject query = new BasicDBObject(primaryKey, guid);
    BasicDBObject updates = new BasicDBObject();
    if (systemFields != null) {
      for (int i = 0; i < systemFields.size(); i++) {
        Object newValue;
        // Special case for the VALUES_MAP field which is all the user values in a JSONObject format
        if (systemFields.get(i).type().equals(ColumnFieldType.VALUES_MAP)) {
          // convert the JSONObject value of the ValuesMap into a string that we then parse into
          // a BSON object (ugly, but necessary)
          newValue = (DBObject) JSON.parse(((ValuesMap) systemValues.get(i)).toString());
          //newValue = ((ValuesMap) values.getAsArray(i)).getMap();
        } else {
          newValue = systemValues.get(i);
        }
        updates.append(systemFields.get(i).getName(), newValue);
      }
    }
    if (valuesMapField != null && valuesMapKeys != null) {
      for (int i = 0; i < valuesMapKeys.size(); i++) {
        String fieldName = valuesMapField.getName() + "." + valuesMapKeys.get(i).getName();
        if (valuesMapKeys.get(i).type().equals(ColumnFieldType.LIST_STRING)) { // special case for old format
          updates.append(fieldName, valuesMapValues.get(i));
        } else if (valuesMapKeys.get(i).type().equals(ColumnFieldType.USER_JSON)) { // value is any valid JSON
          updates.append(fieldName, JSONParse(valuesMapValues.get(i)));
        } else {
          GNS.getLogger().warning("Ignoring unknown format: " + valuesMapKeys.get(i).type());
        }
      }
    }
    if (updates.keySet().size() > 0) {
      long startTime = System.currentTimeMillis();
      try {
        collection.update(query, new BasicDBObject("$set", updates));
      } catch (MongoException e) {
        throw new FailedDBOperationException(collectionName, updates.toString());
      }
      DelayProfiler.updateDelay("updateJustThe$set", startTime);
      long finishTime = System.currentTimeMillis();
      if (debuggingEnabled && finishTime - startTime > 10) {
        GNS.getLogger().warning("Long latency mongoUpdate " + (finishTime - startTime));
      }
    }
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
  public boolean updateConditional(String collectionName, String guid, ColumnField nameField,
          ColumnField conditionField, Object conditionValue, ArrayList<ColumnField> fields, ArrayList<Object> values,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues)
          throws FailedDBOperationException {
    boolean actuallyUpdatedTheRecord = false;
    String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
    DBCollection collection = db.getCollection(collectionName);
    // build the query part
    BasicDBObject query = new BasicDBObject(primaryKey, guid);
    query.append(conditionField.getName(), conditionValue);
    // build the updates part
    BasicDBObject updates = new BasicDBObject();
    if (fields != null) {
      for (int i = 0; i < fields.size(); i++) {
        Object newValue;
        // Special case for the VALUES_MAP field which is all the user values in a JSONObject format
        if (fields.get(i).type().equals(ColumnFieldType.VALUES_MAP)) {
          // convert the JSONObject value of the ValuesMap into a string that we then parse into
          // a BSON object (ugly, but necessary)
          newValue = (DBObject) JSON.parse(((ValuesMap) values.get(i)).toString());
        } else {
          newValue = values.get(i);
        }
        updates.append(fields.get(i).getName(), newValue);
      }
    }

    if (valuesMapField != null && valuesMapKeys != null) {
      for (int i = 0; i < valuesMapKeys.size(); i++) {
        String fieldName = valuesMapField.getName() + "." + valuesMapKeys.get(i).getName();
        updates.append(fieldName, valuesMapValues.get(i));
      }
    }

    if (debuggingEnabled) {
      GNS.getLogger().info("UPDATES: " + updates.toString());
    }

    if (updates.keySet().size() > 0) { // only if there are some things to update
      long startTime = System.currentTimeMillis();
      WriteResult writeResult;
      try {
        writeResult = collection.update(query, new BasicDBObject("$set", updates));
      } catch (MongoException e) {
        throw new FailedDBOperationException(collectionName, updates.toString());
      }
      actuallyUpdatedTheRecord = writeResult.isUpdateOfExisting();
      DelayProfiler.updateDelay("updateConditionalJustThe$set", startTime);
      long finishTime = System.currentTimeMillis();
      if (debuggingEnabled && finishTime - startTime > 10) {
        GNS.getLogger().warning("Long latency mongoUpdate " + (finishTime - startTime));
      }
    }
    if (debuggingEnabled) {
      GNS.getLogger().info(actuallyUpdatedTheRecord ? "ACTUALLY UPDATED " : "DIDN'T UPDATE " + guid);
    }
    return actuallyUpdatedTheRecord;
  }

  @Override
  public void removeMapKeys(String collectionName, String name, ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws FailedDBOperationException {
    String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
    DBCollection collection = db.getCollection(collectionName);
    BasicDBObject query = new BasicDBObject(primaryKey, name);

    BasicDBObject updates = new BasicDBObject();

    if (mapField != null && mapKeys != null) {
      for (int i = 0; i < mapKeys.size(); i++) {
        String fieldName = mapField.getName() + "." + mapKeys.get(i).getName();
        updates.append(fieldName, 1);
      }
    }
    if (updates.keySet().size() > 0) {
      try {
        collection.update(query, new BasicDBObject("$unset", updates));
      } catch (MongoException e) {
        throw new FailedDBOperationException(collectionName, updates.toString());
      }
    }
  }

  /**
   * Given a key and a value return all the records that have a *user* key with that value.
   * User keys are stored in the valuesMap field.
   * The key should be declared as an index otherwise this baby will be slow.
   *
   * @param collectionName
   * @param key
   * @param value
   * // * @param explain
   * @return a MongoRecordCursor
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  @Override
  public MongoRecordCursor selectRecords(String collectionName, ColumnField valuesMapField, String key, Object value)
          throws FailedDBOperationException {
    return selectRecords(collectionName, valuesMapField, key, value, false);
  }

  private MongoRecordCursor selectRecords(String collectionName, ColumnField valuesMapField, String key, Object value,
          boolean explain) throws FailedDBOperationException {
    db.requestEnsureConnection();
    DBCollection collection = db.getCollection(collectionName);
    // note that if the value of the key in the database is a list (which it is) this
    // query will find all records where the value (a list) *contains* an element whose value is the value
    //
    //FROM MONGO DOC: Match an Array Element
    //Equality matches can specify a single element in the array to match. These specifications match
    //if the array contains at least one element with the specified value.
    //In the following example, the query matches all documents where the value of the field tags is
    //an array that contains 'fruit' as one of its elements:
    //db.inventory.find( { tags: 'fruit' } )

    String fieldName = valuesMapField.getName() + "." + key;
    BasicDBObject query = new BasicDBObject(fieldName, value);
    //System.out.println("***QUERY***: " + query.toString());
    DBCursor cursor = null;
    try {
      cursor = collection.find(query);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, fieldName);
    }
    if (explain) {
      System.out.println(cursor.explain().toString());
    }
    return new MongoRecordCursor(cursor, mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey());
  }

  @Override
  public MongoRecordCursor selectRecordsWithin(String collectionName, ColumnField valuesMapField, String key, String value)
          throws FailedDBOperationException {
    return selectRecordsWithin(collectionName, valuesMapField, key, value, false);
  }

  private MongoRecordCursor selectRecordsWithin(String collectionName, ColumnField valuesMapField, String key, String value, boolean explain)
          throws FailedDBOperationException {
    db.requestEnsureConnection();
    DBCollection collection = db.getCollection(collectionName);

//    db.<collection>.find( { <location field> :
//                         { $geoWithin :
//                            { <shape operator> : <coordinates>
//                      } } } )
    BasicDBList box = parseJSONArrayLocationStringIntoDBList(value);
    String fieldName = valuesMapField.getName() + "." + key;
    BasicDBObject shapeClause = new BasicDBObject("$box", box);
    BasicDBObject withinClause = new BasicDBObject("$within", shapeClause);
    BasicDBObject query = new BasicDBObject(fieldName, withinClause);
    DBCursor cursor = null;
    try {
      cursor = collection.find(query);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, fieldName);
    }
    if (explain) {
      System.out.println(cursor.explain().toString());
    }
    return new MongoRecordCursor(cursor, mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey());
  }

  private BasicDBList parseJSONArrayLocationStringIntoDBList(String string) {
    BasicDBList box1 = new BasicDBList();
    BasicDBList box2 = new BasicDBList();
    BasicDBList box = new BasicDBList();
    try {
      JSONArray json = new JSONArray(string);
      box1.add(json.getJSONArray(0).getDouble(0));
      box1.add(json.getJSONArray(0).getDouble(1));
      box2.add(json.getJSONArray(1).getDouble(0));
      box2.add(json.getJSONArray(1).getDouble(1));
      box.add(box1);
      box.add(box2);
    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to parse JSON: " + e);
    }
    return box;
  }

  private final static double METERS_PER_DEGREE = 111.12 * 1000; // at the equator

  @Override
  public MongoRecordCursor selectRecordsNear(String collectionName, ColumnField valuesMapField, String key, String value,
          Double maxDistance) throws FailedDBOperationException {
    return selectRecordsNear(collectionName, valuesMapField, key, value, maxDistance, false);
  }

  private MongoRecordCursor selectRecordsNear(String collectionName, ColumnField valuesMapField, String key, String value,
          Double maxDistance, boolean explain) throws FailedDBOperationException {
    db.requestEnsureConnection();
    DBCollection collection = db.getCollection(collectionName);

//   db.<collection>.find( { <location field> :
//                         { $near : [ <x> , <y> ] ,
//                           $maxDistance: <distance>
//                    } } )
    double maxDistanceInRadians = maxDistance / METERS_PER_DEGREE;
    BasicDBList tuple = new BasicDBList();
    try {
      JSONArray json = new JSONArray(value);
      tuple.add(json.getDouble(0));
      tuple.add(json.getDouble(1));
    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to parse JSON: " + e);
    }
    String fieldName = valuesMapField.getName() + "." + key;
    BasicDBObject nearClause = new BasicDBObject("$near", tuple).append("$maxDistance", maxDistanceInRadians);
    BasicDBObject query = new BasicDBObject(fieldName, nearClause);
    DBCursor cursor = null;
    try {
      cursor = collection.find(query);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, fieldName);
    }
    if (explain) {
      System.out.println(cursor.explain().toString());
    }
    return new MongoRecordCursor(cursor, mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey());
  }

  @Override
  public MongoRecordCursor selectRecordsQuery(String collectionName, ColumnField valuesMapField, String query) throws FailedDBOperationException {
    return selectRecordsQuery(collectionName, valuesMapField, query, false);
  }

  private MongoRecordCursor selectRecordsQuery(String collectionName, ColumnField valuesMapField, String query, boolean explain) throws FailedDBOperationException {
    db.requestEnsureConnection();
    DBCollection collection = db.getCollection(collectionName);
    DBCursor cursor = null;
    try {
      cursor = collection.find(parseMongoQuery(query, valuesMapField));
    } catch (Exception e) {
      throw new FailedDBOperationException(collectionName, query);
    }
    if (explain) {
      System.out.println(cursor.explain().toString());
    }
    return new MongoRecordCursor(cursor, mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey());
  }

  private DBObject parseMongoQuery(String query, ColumnField valuesMapField) {
    // convert something like this: ~fred : ($gt: 0) into the queryable 
    // format, namely this: {~nr_valuesMap.fred : ($gt: 0)}
    query = "{" + query + "}";
    query = query.replace("(", "{");
    query = query.replace(")", "}");
    query = query.replace("~", valuesMapField.getName() + ".");
    DBObject parse = (DBObject) JSON.parse(query);
    return parse;
  }

  @Override
  public MongoRecordCursor getAllRowsIterator(String collectionName, ColumnField nameField, ArrayList<ColumnField> fields)
          throws FailedDBOperationException {
    return new MongoRecordCursor(db, collectionName, mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey(), fields);
  }

  @Override
  public MongoRecordCursor getAllRowsIterator(String collectionName) throws FailedDBOperationException {
    return new MongoRecordCursor(db, collectionName, mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey());
  }

  @Override
  public void printAllEntries(String collectionName) throws FailedDBOperationException {
    MongoRecordCursor cursor = getAllRowsIterator(collectionName);
    while (cursor.hasNext()) {
      System.out.println(cursor.nextJSONObject());
    }
  }

  @Override
  public String toString() {
    return "DB " + dbName;
  }

  //THIS ISN'T JUST TEST CODE - DO NOT REMOVE
  // the -clear option is currently used by the EC2 installer so keep it working
  // this use will probably go away at some point
  /**
   * Does a few things.
   * 
   * @param args
   * @throws Exception
   * @throws RecordNotFoundException 
   */
  public static void main(String[] args) throws Exception, RecordNotFoundException {
    if (args.length > 0 && args[0].startsWith("-clear")) {
      dropAllDatabases();
    } else if (args.length == 3) {
      //testlookupMultipleSystemAndUserFields(args[0], args[1], args[2]);
      queryTest(args[0], args[1], args[2], null);
    } else if (args.length == 4) {
      queryTest(args[0], args[1], args[2], args[3]);
    } else {
    }
    // important to include this!!
    System.exit(0);
  }

  /**
   * A utility to drop all the databases.
   * 
   */
  public static void dropAllDatabases() {
    MongoClient mongoClient;
    try {
      mongoClient = new MongoClient("localhost");
    } catch (UnknownHostException e) {
      GNS.getLogger().severe("Unable to open Mongo DB: " + e);
      return;
    }
    List<String> names = mongoClient.getDatabaseNames();
    for (String name : names) {
      mongoClient.dropDatabase(name);
    }
    System.out.println("Dropped mongo DBs: " + names.toString());
    // reinit the instance
//    init();
  }

  // ALL THE CODE BELOW IS TEST CODE
  //  //test code
  
//  private static final ArrayList<ColumnField> dnsSystemFields = new ArrayList<ColumnField>();
//
//  static {
//    dnsSystemFields.add(NameRecord.ACTIVE_VERSION);
//    dnsSystemFields.add(NameRecord.TIME_TO_LIVE);
//  }
//
//  private static void testlookupMultipleSystemAndUserFields(String node, String guid, String field) {
//    try {
//      MongoRecords instance = new MongoRecords(node);
//      ArrayList<ColumnField> userFields
//              = new ArrayList<ColumnField>(Arrays.asList(new ColumnField(field,
//                                      ColumnFieldType.USER_JSON)));
//      int cnt = 0;
//      long startTime = System.currentTimeMillis();
//      do {
//        Map<ColumnField, Object> map  
//                = instance.lookupMultipleSystemAndUserFields(
//                        DBNAMERECORD,
//                        guid,
//                        NameRecord.NAME,
//                        dnsSystemFields,
//                        NameRecord.VALUES_MAP,
//                        userFields);
//        if (cnt++ % 10000 == 0) {
//          System.out.println(map);
//          System.out.println(DelayProfiler.getStats());
//          System.out.println("op/s = " + Format.formatTime(10000000.0 / (System.currentTimeMillis() - startTime)));
//          startTime = System.currentTimeMillis();
//        }
//      } while (true);
//    } catch (FailedDBOperationException | RecordNotFoundException e) {
//      System.out.println("Lookup failed: " + e);
//    }
//  }

  
  @SuppressWarnings("unchecked") /// because it's static
  private static void queryTest(Object nodeID, String key, String searchArg, String otherArg) throws RecordNotFoundException, Exception {
    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig("ns1", nodeID);
    Set nameServerIDs = new HashSet();
    nameServerIDs.add("0");
    nameServerIDs.add("1");
    nameServerIDs.add("2");
    //ConsistentHashing.reInitialize(3, nameServerIDs);
    MongoRecords instance = new MongoRecords(nodeID, -1);
    System.out.println("***ALL RECORDS***");
    instance.printAllEntries(DBNAMERECORD);

    Object search;
    try {
      search = Double.parseDouble(searchArg);
    } catch (NumberFormatException e) {
      search = searchArg;
    }

    Object other = null;
    if (otherArg != null) {
      try {
        other = Double.parseDouble(otherArg);
      } catch (NumberFormatException e) {
        other = otherArg;
      }
    }

    System.out.println("***LOCATION QUERY***");
    MongoRecordCursor cursor;
    if (search instanceof Double) {
      cursor = instance.selectRecords(DBNAMERECORD, NameRecord.VALUES_MAP, key, search, true);
    } else if (other != null) {
      cursor = instance.selectRecordsNear(DBNAMERECORD, NameRecord.VALUES_MAP, key, (String) search, (Double) other, true);
    } else {
      cursor = instance.selectRecordsWithin(DBNAMERECORD, NameRecord.VALUES_MAP, key, (String) search, true);
    }
    while (cursor.hasNext()) {
      try {
        JSONObject json = cursor.nextJSONObject();
        System.out.println(json.getString(NameRecord.NAME.getName()) + " -> " + json.toString());
      } catch (Exception e) {
        System.out.println("Exception: " + e);
        e.printStackTrace();
      }
    }
    System.out.println("***ALL RECORDS ACTIVE FIELD***");
    cursor = instance.getAllRowsIterator(DBNAMERECORD, NameRecord.NAME, new ArrayList<ColumnField>(Arrays.asList(NameRecord.PRIMARY_NAMESERVERS)));
    while (cursor.hasNext()) {
      System.out.println(cursor.nextJSONObject().toString());
    }
  }

  /**
   * *
   * Close mongo client before shutting down name server. 
   * As per mongo doc:
   * "to dispose of an instance, make sure you call MongoClient.close() to clean up resources."
   */
  public void close() {
    mongoClient.close();
  }
  
}
