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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * Provides insert, updateEntireRecord, removeEntireRecord and lookupEntireRecord operations for
 * guid, key, record triples using JSONObjects as the intermediate representation.
 * All records are stored in a document called NameRecord.
 *
 * @author westy, Abhigyan, arun
 */
public class MongoRecords implements NoSQLRecords {

  private static final String DBROOTNAME = "UMASS_GNS_DB_";
  /**
   * The name of the document where name records are stored.
   */
  public static final String DBNAMERECORD = "NameRecord";

  private DB db;
  private String dbName;

  private MongoClient mongoClient;
  private MongoCollectionSpecs mongoCollectionSpecs;

  /**
   * Creates database tables for nodeID, by connecting to mongoDB on default port.
   *
   * @param nodeID nodeID of name server
   */
  public MongoRecords(String nodeID) {
    this(nodeID, -1);
  }

  /**
   * Creates database tables for nodeID, by connecting to mongoDB on given port.
   *
   * @param nodeID nodeID of name server
   * @param port port at which mongo is running. if port = -1, mongo connects to default port.
   */
  public MongoRecords(String nodeID, int port) {
    init(nodeID, port);
  }

  private void init(String nodeID, int mongoPort) {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.IN_MEMORY_DB)) {
      return;
    }
    mongoCollectionSpecs = new MongoCollectionSpecs();
    mongoCollectionSpecs.addCollectionSpec(DBNAMERECORD, NameRecord.NAME);
    // add location as another index
    mongoCollectionSpecs.getCollectionSpec(DBNAMERECORD)
            .addOtherIndex(new BasicDBObject(NameRecord.VALUES_MAP.getName() + "." + GNSProtocol.LOCATION_FIELD_NAME.toString(), "2d"));
    // The good thing is that indexes are not required for 2dsphere fields, but they will make things faster
    mongoCollectionSpecs.getCollectionSpec(DBNAMERECORD)
            .addOtherIndex(new BasicDBObject(NameRecord.VALUES_MAP.getName() + "." + GNSProtocol.LOCATION_FIELD_NAME_2D_SPHERE.toString(), "2dsphere"));
    mongoCollectionSpecs.getCollectionSpec(DBNAMERECORD)
            .addOtherIndex(new BasicDBObject(NameRecord.VALUES_MAP.getName() + "." + GNSProtocol.IPADDRESS_FIELD_NAME.toString(), 1));

    boolean fatalException = false;
    try {
      // use a unique name in case we have more than one on a machine (need to remove periods, btw)
      dbName = DBROOTNAME + sanitizeDBName(nodeID);
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
      fatalException = true;
      DatabaseConfig.getLogger().severe("Unable to open Mongo DB: " + e);
    } catch (com.mongodb.MongoServerSelectionException msse) {
      fatalException = true;
      DatabaseConfig.getLogger().severe("Fatal exception while trying to initialize Mongo DB: " + msse);
    } finally {
      if (fatalException) {
        Util.suicide("Mongo DB initialization failed likely because a mongo DB server is not listening at the expected port; exiting.");
      }
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
    DatabaseConfig.getLogger().fine("Indexes initialized");
  }

  @Override
  public void createIndex(String collectionName, String field, String index) {
    MongoCollectionSpec spec = mongoCollectionSpecs.getCollectionSpec(collectionName);
    // Prepend this because of the way we store the records.
    DBObject index2d = BasicDBObjectBuilder.start(NameRecord.VALUES_MAP.getName() + "." + field, index).get();
    db.getCollection(spec.getName()).createIndex(index2d);
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
        throw new FailedDBOperationException(collectionName, dbObject.toString(),
                "Original mongo exception:" + e.getMessage());
      }
    } finally {
      db.requestDone();
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
        // arun: optimized for the common case of Map
        @SuppressWarnings("unchecked")
        JSONObject json = obj instanceof Map ? DiskMapRecords
                .recursiveCopyMap((Map<String, ?>) obj)
                : new JSONObject(obj.toString());
        // instrumentation
        DelayProfiler.updateDelay("lookupEntireRecord", startTime);
        // older style
        int lookupTime = (int) (System.currentTimeMillis() - startTime);
        if (lookupTime > 20) {
          DatabaseConfig.getLogger().log(Level.FINE, " mongoLookup Long delay {0}", lookupTime);
        }
        return json;
      } else {
        throw new RecordNotFoundException(guid);
      }
    } catch (JSONException e) {
      DatabaseConfig.getLogger().log(Level.WARNING,
              "Unable to parse JSON: {0}", e);
      return null;
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, guid,
              "Original mongo exception:" + e.getMessage());
    } finally {
      db.requestDone();
    }
  }

  @Override
  public HashMap<ColumnField, Object> lookupSomeFields(String collectionName,
          String guid, ColumnField nameField, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys)
          throws RecordNotFoundException, FailedDBOperationException {
    if (guid == null) {
      DatabaseConfig.getLogger().log(Level.FINE, "GUID is null: {0}", new Object[]{guid});
      throw new RecordNotFoundException(guid);
    }
    db.requestStart();
    try {
      String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();

      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      BasicDBObject projection = new BasicDBObject().append("_id", 0);

      if (valuesMapField != null && valuesMapKeys != null) {
        for (int i = 0; i < valuesMapKeys.size(); i++) {
          String fieldName = valuesMapField.getName() + "." + valuesMapKeys.get(i).getName();
          projection.append(fieldName, 1);
        }
      }
      DBObject dbObject = collection.findOne(query, projection);
      if (dbObject == null) {
        throw new RecordNotFoundException(guid);
      }
      HashMap<ColumnField, Object> hashMap = new HashMap<>();
      hashMap.put(nameField, guid);// put the name in the hashmap!! very important!!
      if (valuesMapField != null && valuesMapKeys != null) {
        // first we pull all the user values from the dbObject and put in a bson object
        // FIXME: Why not convert this to a JSONObject right now? We know that's what it is.
        BasicDBObject bson = (BasicDBObject) dbObject.get(valuesMapField.getName());
        DatabaseConfig.getLogger().log(Level.FINER, "@@@@@@@@ {0}", new Object[]{bson});
        // then we run thru each userkey in the valuesMapKeys and pull the
        // value put stuffing it into the values map
        ValuesMap valuesMap = new ValuesMap();
        for (int i = 0; i < valuesMapKeys.size(); i++) {
          String userKey = valuesMapKeys.get(i).getName();
          if (containsFieldDotNotation(userKey, bson) == false) {
            DatabaseConfig.getLogger().log(Level.FINE,
                    "DBObject doesn't contain {0}", new Object[]{userKey});

            continue;
          }
          try {
            switch (valuesMapKeys.get(i).type()) {
              case USER_JSON:
                Object value = getWithDotNotation(userKey, bson);
                DatabaseConfig.getLogger().log(Level.FINE,
                        "Object is {0}", new Object[]{value.toString()});
                valuesMap.put(userKey, value);
                break;
              case LIST_STRING:
                valuesMap.putAsArray(userKey,
                        JSONUtils.JSONArrayToResultValue(new JSONArray(getWithDotNotation(userKey, bson).toString())));
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
        hashMap.put(valuesMapField, valuesMap);
      }
      return hashMap;
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, guid,
              "Original mongo exception:" + e.getMessage());
    } finally {
      db.requestDone();
    }
  }

  private Object getWithDotNotation(String key, BasicDBObject bson) throws JSONException {
    if (key.contains(".")) {
      int indexOfDot = key.indexOf(".");
      String subKey = key.substring(0, indexOfDot);
      BasicDBObject subBson = (BasicDBObject) bson.get(subKey);
      if (subBson == null) {
        throw new JSONException(subKey + " is null");
      }
      try {
        return getWithDotNotation(key.substring(indexOfDot + 1), subBson);
      } catch (JSONException e) {
        throw new JSONException(subKey + "." + e.getMessage());
      }
    } else {
      Object result = bson.get(key);
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
      DBCursor cursor = collection.find(query);
      return cursor.hasNext();
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, guid,
              "Original mongo exception:" + e.getMessage());
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
      throw new FailedDBOperationException(collectionName, guid,
              "Original mongo exception:" + e.getMessage());
    }
  }

  @Override
  public void updateEntireRecord(String collectionName, String guid, ValuesMap valuesMap)
          throws FailedDBOperationException {
    BasicDBObject updates = new BasicDBObject();
    updates.append(NameRecord.VALUES_MAP.getName(), JSON.parse(valuesMap.toString()));
    doUpdate(collectionName, guid, updates);
  }

  /**
   *
   * @param collectionName
   * @param values
   * @throws FailedDBOperationException
   * @throws RecordExistsException
   */
  public void bulkUpdate(String collectionName, Map<String, JSONObject> values)
          throws FailedDBOperationException, RecordExistsException {
    //String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
    DBCollection collection = db.getCollection(collectionName);
    String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
    db.requestEnsureConnection();
    BulkWriteOperation unordered = collection.initializeUnorderedBulkOperation();
    for (Map.Entry<String, JSONObject> entry : values.entrySet()) {
      BasicDBObject query = new BasicDBObject(primaryKey, entry.getKey());
      JSONObject value = entry.getValue();
      if (value != null) {
        DBObject document = (DBObject) JSON.parse(value.toString());
        unordered.find(query).upsert().replaceOne(document);
      } else {
        unordered.find(query).removeOne();
      }
    }
    // Maybe check the result?
    BulkWriteResult result = unordered.execute();
  }

  @Override
  public void updateIndividualFields(String collectionName, String guid,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys,
          ArrayList<Object> valuesMapValues) throws FailedDBOperationException {
    BasicDBObject updates = new BasicDBObject();
    if (valuesMapField != null && valuesMapKeys != null) {
      for (int i = 0; i < valuesMapKeys.size(); i++) {
        String fieldName = valuesMapField.getName() + "." + valuesMapKeys.get(i).getName();
        switch (valuesMapKeys.get(i).type()) {
          case LIST_STRING:
            // special case for old format
            updates.append(fieldName, valuesMapValues.get(i));
            break;
          case USER_JSON:
            // value is any valid JSON
            updates.append(fieldName, JSONParse(valuesMapValues.get(i)));
            break;
          default:
            DatabaseConfig.getLogger().log(Level.WARNING,
                    "Ignoring unknown format: {0}", valuesMapKeys.get(i).type());
            break;
        }
      }
    }
    doUpdate(collectionName, guid, updates);
  }

  private void doUpdate(String collectionName, String guid, BasicDBObject updates)
          throws FailedDBOperationException {
    String primaryKey = mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey().getName();
    DBCollection collection = db.getCollection(collectionName);
    BasicDBObject query = new BasicDBObject(primaryKey, guid);
    if (updates.keySet().size() > 0) {
      long startTime = System.currentTimeMillis();
      try {
        collection.update(query, new BasicDBObject("$set", updates));
      } catch (MongoException e) {
        DatabaseConfig.getLogger().severe("Update failed: " + e);
        throw new FailedDBOperationException(collectionName, updates.toString(),
                "Original mongo exception:" + e.getMessage());
      }
      DelayProfiler.updateDelay("mongoSetUpdate", startTime);
      long finishTime = System.currentTimeMillis();
      if (finishTime - startTime > 10) {
        DatabaseConfig.getLogger().log(Level.FINE,
                "Long latency mongoUpdate {0}", (finishTime - startTime));
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
        DatabaseConfig.getLogger().fine("<============>unset" + updates.toString() + "<============>");
        collection.update(query, new BasicDBObject("$unset", updates));
      } catch (MongoException e) {
        throw new FailedDBOperationException(collectionName, updates.toString(),
                "Original mongo exception:" + e.getMessage());
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
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
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
    //System.out.println("***GNSProtocol.QUERY.toString()***: " + query.toString());
    DBCursor cursor = null;
    try {
      cursor = collection.find(query);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, fieldName,
              "Original mongo exception:" + e.getMessage());
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

    BasicDBList box = parseJSONArrayLocationStringIntoDBList(value);
    String fieldName = valuesMapField.getName() + "." + key;
    BasicDBObject shapeClause = new BasicDBObject("$box", box);
    BasicDBObject withinClause = new BasicDBObject("$geoWithin", shapeClause);
    BasicDBObject query = new BasicDBObject(fieldName, withinClause);
    DBCursor cursor = null;
    try {
      cursor = collection.find(query);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, fieldName,
              "Original mongo exception:" + e.getMessage());
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
      DatabaseConfig.getLogger().severe("Unable to parse JSON: " + e);
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

    double maxDistanceInRadians = maxDistance / METERS_PER_DEGREE;
    BasicDBList tuple = new BasicDBList();
    try {
      JSONArray json = new JSONArray(value);
      tuple.add(json.getDouble(0));
      tuple.add(json.getDouble(1));
    } catch (JSONException e) {
      DatabaseConfig.getLogger().severe("Unable to parse JSON: " + e);
    }
    String fieldName = valuesMapField.getName() + "." + key;
    BasicDBObject nearClause = new BasicDBObject("$near", tuple).append("$maxDistance", maxDistanceInRadians);
    BasicDBObject query = new BasicDBObject(fieldName, nearClause);
    DBCursor cursor = null;
    try {
      cursor = collection.find(query);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, fieldName,
              "Original mongo exception:" + e.getMessage());
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
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, query,
              "Original mongo exception:" + e.getMessage());
    }
    if (explain) {
      System.out.println(cursor.explain().toString());
    }
    return new MongoRecordCursor(cursor, mongoCollectionSpecs.getCollectionSpec(collectionName).getPrimaryKey());
  }

  private DBObject parseMongoQuery(String query, ColumnField valuesMapField) {
    // convert something like this: ~fred : ($gt: 0) into the queryable 
    // format, namely this: {~nr_valuesMap.fred : ($gt: 0)}
    String edittedQuery = query;
    edittedQuery = "{" + edittedQuery + "}";
    edittedQuery = edittedQuery.replace("(", "{");
    edittedQuery = edittedQuery.replace(")", "}");
    edittedQuery = edittedQuery.replace("~", valuesMapField.getName() + ".");
    // Filter out HRN records
    String guidFilter = "{" + NameRecord.VALUES_MAP.getName() + "." + AccountAccess.GUID_INFO + ": { $exists: true}}";
    edittedQuery = buildAndQuery(guidFilter, edittedQuery);
    DatabaseConfig.getLogger().log(Level.INFO, "Edited query = {0}", edittedQuery);
    DBObject parse = (DBObject) JSON.parse(edittedQuery);
    return parse;
  }
  
  public static String buildAndQuery(String... querys) {
    StringBuilder result = new StringBuilder();
    result.append("{$and: [");
    String prefix = "";
    for (String query : querys) {
      result.append(prefix);
      result.append(query);
      prefix = ",";
    }
    result.append("]}");
    return result.toString();
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

  /**
   * *
   * Close mongo client before shutting down name server.
   * As per mongo doc:
   * "to dispose of an instance, make sure you call MongoClient.close() to clean up resources."
   */
  public void close() {
    mongoClient.close();
  }

  /**
   * @param nodeID
   */
  public static void dropNodeDatabase(String nodeID) {
    MongoClient mongoClient;
    try {
      mongoClient = new MongoClient("localhost");
    } catch (UnknownHostException e) {
      DatabaseConfig.getLogger().log(Level.SEVERE,
              "Unable to open Mongo DB: {0}", e);
      return;
    }
    String dbName = DBROOTNAME + sanitizeDBName(nodeID);
    mongoClient.dropDatabase(dbName);

    List<String> names = mongoClient.getDatabaseNames();
    for (String name : names) {
      if (name.startsWith(dbName)) {
        mongoClient.dropDatabase(name);
      }
    }

    System.out.println("Dropped DB " + dbName);
  }

  private static String sanitizeDBName(String nodeID) {
    return nodeID.replace('.', '_');
  }

}
