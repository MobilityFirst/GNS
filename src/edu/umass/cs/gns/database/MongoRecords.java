/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.JSONUtils;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.bson.BSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides insert, update, remove and lookup operations for guid, key, record triples using JSONObjects as the intermediate
 * representation
 * 
 * *** THIS CODE NEEDS SOME MORE WORK TO REMOVE REDUNDANT CODE AND CLEAN UP SOME OF THE EXCEPTION HANDLING ***
 *
 * @author westy
 */
public class MongoRecords implements NoSQLRecords {

  private static final String DBROOTNAME = "GNS";
  public static final String DBNAMERECORD = "NameRecord";
  public static final String DBREPLICACONTROLLER = "ReplicaControllerRecord";
  public static final String PAXOSLOG = "PaxosLog";
  private DB db;
  private String dbName;
  
  public static MongoRecords getInstance() {
    return MongoRecordCollectionHolder.INSTANCE;
  }

  private static class MongoRecordCollectionHolder {

    private static final MongoRecords INSTANCE = new MongoRecords();
  }

  private MongoRecords() {
    init();
  }
 
  private void init() {
    MongoCollectionSpec.addCollectionSpec(DBNAMERECORD, NameRecord.NAME);
    MongoCollectionSpec.addCollectionSpec(DBREPLICACONTROLLER, ReplicaControllerRecord.NAME);
    // add location as another index
    MongoCollectionSpec.getCollectionSpec(DBNAMERECORD).addOtherIndex(new BasicDBObject(NameRecord.VALUES_MAP.getName() + "." + "location", 1));
    MongoCollectionSpec.getCollectionSpec(DBNAMERECORD).addOtherIndex(new BasicDBObject(NameRecord.VALUES_MAP.getName() + "." + "ipAddress", 1));
    try {
      // use a unique name in case we have more than one on a machine
      dbName = DBROOTNAME + "-" + NameServer.nodeID;
      MongoClient mongoClient;
      if (StartNameServer.mongoPort > 0) {
        mongoClient = new MongoClient("localhost", StartNameServer.mongoPort);
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
    for (MongoCollectionSpec spec : MongoCollectionSpec.allCollectionSpecs()) {
      initializeIndex(spec.getName());
    }
  }

  private void initializeIndex(String collectionName) {
    MongoCollectionSpec spec = MongoCollectionSpec.getCollectionSpec(collectionName);
    db.getCollection(spec.getName()).ensureIndex(spec.getPrimaryIndex(), new BasicDBObject("unique", true));
    for (BasicDBObject index : spec.getOtherIndexes()) {
      db.getCollection(spec.getName()).ensureIndex(index);
    } 
  }

  @Override
  public void reset(String collectionName) {
    if (MongoCollectionSpec.getCollectionSpec(collectionName) != null) {
      db.requestStart();
      try {
        db.requestEnsureConnection();
        db.getCollection(collectionName).dropIndexes();
        db.getCollection(collectionName).drop();
        GNS.getLogger().info("MONGO DB RESET. DBNAME: " + dbName + " Collection name: " + collectionName);

        // IMPORTANT... recreate the index
        initializeIndex(collectionName);
      } finally {
        db.requestDone();
      }
    } else {
      GNS.getLogger().severe("MONGO DB: No collection named: " + collectionName);
    }

  }

  @Override
  public JSONObject lookup(String collectionName, String guid) throws RecordNotFoundException {
    return lookup(collectionName, guid, false);
  }

  private JSONObject lookup(String collectionName, String guid, boolean explain) {
    db.requestStart();
    try {
      String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      DBCursor cursor = collection.find(query);
      if (explain) {
        System.out.println(cursor.explain().toString());
      }
      if (cursor.hasNext()) {
        DBObject obj = cursor.next();
        return new JSONObject(obj.toString());
      } else {
        return null;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("Unable to parse JSON: " + e);
      return null;
    } finally {
      db.requestDone();
    }
  }

  @Override
  public String lookup(String collectionName, String guid, String key) {
    return lookup(collectionName, guid, key, false);
  }

  private String lookup(String collectionName, String guid, String key, boolean explain) {
    db.requestStart();
    try {
      String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      BasicDBObject projection = new BasicDBObject(key, 1).append("_id", 0);
      DBCursor cursor = collection.find(query, projection);
      if (explain) {
        System.out.println(cursor.explain().toString());
      }
      if (cursor.hasNext()) {
        DBObject obj = cursor.next();
        JSONObject json = new JSONObject(obj.toString());
        if (json.has(key)) {
          return json.getString(key);
        } else {
          return null;
        }
      } else {
        return null;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("Unable to parse JSON: " + e);
      return null;
    } finally {
      db.requestDone();
    }
  }

  @Override
  public ResultValue lookup(String collectionName, String guid, ArrayList<String> keys) {
    return lookup(collectionName, guid, keys, false);
  }

  private ResultValue lookup(String collectionName, String guid, ArrayList<String> keys, boolean explain) {
    db.requestStart();
    try {
      String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();

      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      //The projection parameter takes a document of the following form:
      // { field1: <boolean>, field2: <boolean> ... } where boolean is 0 or 1.
      BasicDBObject projection = new BasicDBObject().append("_id", 0);
      for (String key : keys) {
        projection.append(key, 1);
      }
      DBCursor cursor = collection.find(query, projection);
      if (explain) {
        System.out.println(cursor.explain().toString());
      }
      ResultValue values = new ResultValue();
      if (cursor.hasNext()) {
        DBObject obj = cursor.next();
        for (String key : keys) {
          Object field = obj.get(key);
          if (field == null) {
            values.add(null);
          } else {
            values.add(field.toString());
          }
        }
      } else {
        return null;
      }
      return values;
    } finally {
      db.requestDone();
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
   * @param explain
   * @return a MongoRecordCursor
   */
  @Override
  public MongoRecordCursor selectRecords(String collectionName, Field valuesMapField, String key, Object value) {
    return selectRecords(collectionName, valuesMapField, key, value, false);
  }
  
  private MongoRecordCursor selectRecords(String collectionName, Field valuesMapField, String key, Object value, boolean explain) {
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
    DBCursor cursor = collection.find(query);
    if (explain) {
      System.out.println(cursor.explain().toString());
    }
    return new MongoRecordCursor(cursor, MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey());
  }

  @Override
  public void insert(String collectionName, String guid, JSONObject value) throws RecordExistsException {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      DBObject dbObject = (DBObject) JSON.parse(value.toString());
      try {
        collection.insert(dbObject);
      } catch (Exception e) {
        throw new RecordExistsException(collectionName, guid);
//        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    } finally {
      db.requestDone();
    }
  }

  @Override
  public void update(String collectionName, String guid, JSONObject value) {
    db.requestStart();
    try {
      String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      DBObject dbObject = (DBObject) JSON.parse(value.toString());
      collection.update(query, dbObject);
    } finally {
      db.requestDone();
    }
  }

  public void updateSingleValue(String collectionName, String name, String key, String value) {
    updateField(collectionName, name, key, new ArrayList(Arrays.asList(value)));
  }

  @Override
  public void updateField(String collectionName, String guid, String key, Object object) {
    db.requestStart();
    try {
      String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      BasicDBObject newValue = new BasicDBObject(key, object);
      BasicDBObject updateOperator = new BasicDBObject("$set", newValue);
      collection.update(query, updateOperator);
    } finally {
      db.requestDone();
    }
  }

  @Override
  public boolean contains(String collectionName, String guid) {
    db.requestStart();
    try {
      String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      DBCursor cursor = collection.find(query);
      if (cursor.hasNext()) {
        return true;
      } else {
        return false;
      }
    } finally {
      db.requestDone();
    }
  }

  @Override
  public void remove(String collectionName, String guid) {
    db.requestStart();
    try {
      String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      collection.remove(query);
    } finally {
      db.requestDone();
    }
  }

  @Override
  public HashMap<Field, Object> lookup(String collectionName, String guid, Field nameField, ArrayList<Field> fields1) throws RecordNotFoundException {
    return lookup(collectionName, guid, nameField, fields1, null, null);
  }

  @Override
  public HashMap<Field, Object> lookup(String collectionName, String guid, Field nameField, ArrayList<Field> fields1, Field valuesMapField, ArrayList<Field> valuesMapKeys) throws RecordNotFoundException {
    if (guid == null) {
      GNS.getLogger().fine("GUID is null: " + guid);
      throw new RecordNotFoundException(guid);
    }
    db.requestStart();
    try {
      String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();

      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      BasicDBObject projection = new BasicDBObject().append("_id", 0);
      if (fields1 != null) {
        for (Field f : fields1) {
          projection.append(f.getName(), 1);
        }
      }

      if (valuesMapField != null && valuesMapKeys != null) {
        for (int i = 0; i < valuesMapKeys.size(); i++) {
          String fieldName = valuesMapField.getName() + "." + valuesMapKeys.get(i).getName();
          projection.append(fieldName, 1);
        }
      }

      DBCursor cursor = collection.find(query, projection);
      HashMap<Field, Object> hashMap = new HashMap<Field, Object>();
      if (cursor.hasNext()) {
        hashMap.put(nameField, guid);// put the name in the hashmap!! very important!!
        DBObject dbObject = cursor.next();
        FieldType.populateHashMap(hashMap, dbObject, fields1);

        if (valuesMapField != null && valuesMapKeys != null) {
          BSONObject bson = (BSONObject) dbObject.get(valuesMapField.getName());

          ValuesMap valuesMap = new ValuesMap();
          for (int i = 0; i < valuesMapKeys.size(); i++) {
            JSONArray fieldValue;
            if (bson.containsField(valuesMapKeys.get(i).getName()) == false) {
              continue;
            }
            try {
              fieldValue = new JSONArray(bson.get(valuesMapKeys.get(i).getName()).toString());
//                System.out.println("\nKEY = " + valuesMapKeys.get(i).getFieldName() + " \tVALUE = " + fieldValue+"\n");
            } catch (JSONException e) {
              e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
              continue;
            }
            if (valuesMapKeys.get(i).type() == FieldType.LIST_STRING) {
              try {
                valuesMap.put(valuesMapKeys.get(i).getName(), JSONUtils.JSONArrayToResultValue(fieldValue));
              } catch (JSONException e) {
                GNS.getLogger().fine("Error parsing json");
                e.printStackTrace();
              }
            } else {
              GNS.getLogger().fine("ERROR: Error: User keys field is not of type " + FieldType.LIST_STRING);
              System.exit(2);
            }
          }
          hashMap.put(valuesMapField, valuesMap);
        }

        return hashMap;
      } else {
        throw new RecordNotFoundException(guid);
      }
    } finally {
      db.requestDone();
    }
  }

  @Override
  public void update(String collectionName, String guid, Field nameField, ArrayList<Field> fields1, ArrayList<Object> values1) {
    update(collectionName, guid, nameField, fields1, values1, null, null, null);
  }

  @Override
  public void update(String collectionName, String guid, Field nameField, ArrayList<Field> fields, ArrayList<Object> values,
          Field valuesMapField, ArrayList<Field> valuesMapKeys, ArrayList<Object> valuesMapValues) {
    db.requestStart();
    try {
      String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      BasicDBObject updates = new BasicDBObject();
      if (fields != null) {
        for (int i = 0; i < fields.size(); i++) {
          Object newValue;
          if (fields.get(i).type() == FieldType.VALUES_MAP) {
            newValue = ((ValuesMap) values.get(i)).getMap();
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
      if (updates.keySet().size() > 0) {
        long t0 = System.currentTimeMillis();
        collection.update(query, new BasicDBObject("$set", updates));
        long t1 = System.currentTimeMillis();
        if (t1 - t0 > 10) {
          //System.out.println(" Long latency mongoUpdate " + (t1 - t0) + "\ttime\t" + t0);
          GNS.getLogger().warning(" Long latency mongoUpdate " + (t1 - t0));
        }
//        System.out.println("\nTHIS SHOULD NOT PRINT !!!--> "  );
      }
    } finally {
      db.requestDone();
    }
  }

  @Override
  public void increment(String collectionName, String guid, ArrayList<Field> fields, ArrayList<Object> values) {
    increment(collectionName, guid, fields, values, null, null, null);
  }

  @Override
  public void increment(String collectionName, String guid, ArrayList<Field> fields, ArrayList<Object> values,
          Field votesMapField, ArrayList<Field> votesMapKeys, ArrayList<Object> votesMapValues) {
    db.requestStart();
    try {
      String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      BasicDBObject updates = new BasicDBObject();
      if (fields != null) {
        for (int i = 0; i < fields.size(); i++) {
          Object newValue;
          if (fields.get(i).type() == FieldType.VALUES_MAP) {
            newValue = ((ValuesMap) values.get(i)).getMap();
          } else {
            newValue = values.get(i);
          }
          updates.append(fields.get(i).getName(), newValue);
        }
      }
      if (votesMapField != null && votesMapKeys != null) {
        for (int i = 0; i < votesMapKeys.size(); i++) {
          String fieldName = votesMapField.getName() + "." + votesMapKeys.get(i).getName();
          updates.append(fieldName, votesMapValues.get(i));
        }
      }
      if (updates.keySet().size() > 0) {
        collection.update(query, new BasicDBObject("$inc", updates));
      }
    } finally {
      db.requestDone();
    }
  }

  @Override
  public MongoRecordCursor getAllRowsIterator(String collectionName, Field nameField, ArrayList<Field> fields) {
    return new MongoRecordCursor(db, collectionName, MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey(), fields);
  }

  @Override
  public MongoRecordCursor getAllRowsIterator(String collectionName) {
    return new MongoRecordCursor(db, collectionName, MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey());
  }

  @Override
  public Set<String> keySet(String collectionName) {
    Set<String> result = new HashSet<String>();
    // Get a cursor for all the rows with just the name column filled in.
    MongoRecordCursor cursor = getAllRowsIterator(collectionName, MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey(), null);
    String nameField = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
    while (cursor.hasNext()) {
      result.add(cursor.nextRowField(nameField));
    }
    return result;
  }

  @Override
  public void printAllEntries(String collectionName) {
    MongoRecordCursor cursor = getAllRowsIterator(collectionName);
    while (cursor.hasNext()) {
      System.out.println(cursor.nextJSONObject());
    }
  }

  @Override
  public String toString() {
    return "DB " + dbName;
  }

  //THIS ISN'T TEST CODE
  // the -clear option is currently used by the EC2 installer so keep it working
  // this use will probably go away at some point
  public static void main(String[] args) throws Exception, RecordNotFoundException {
    if (args.length > 0 && args[0].startsWith("-clear")) {
      dropAllDatabases();
    } else if (args.length == 3) {
      queryTest(Integer.parseInt(args[0]), args[1], args[2]);
    } else {
      
    }
  }

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
    getInstance().init();
  }

  // ALL THE CODE BELOW IS TEST CODE
//  //test code
  private static void queryTest(int nodeID, String key, String searchArg) throws RecordNotFoundException, Exception {
    NameServer.nodeID = nodeID;
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    MongoRecords instance = MongoRecords.getInstance();
    System.out.println("***ALL RECORDS***");
    instance.printAllEntries(DBNAMERECORD);
    System.out.println("***ALL RECORD KEYS ->" + instance.keySet(DBNAMERECORD).toString());

    Object search;
    try {
      search = Double.parseDouble(searchArg);
    } catch (NumberFormatException e) {
      search = searchArg;
    }

    System.out.println("***LOCATION QUERY***");
    MongoRecordCursor cursor = instance.selectRecords(DBNAMERECORD, NameRecord.VALUES_MAP, key, search, true);
    while (cursor.hasNext()) {
      try {
        JSONObject json = cursor.next();
        System.out.println(json.getString(NameRecord.NAME.getName()) + " -> " + json.toString());
      } catch (Exception e) {
        System.out.println("Exception: " + e);
        e.printStackTrace();
      }
    }
    System.out.println("***ALL RECORDS ACTIVE FIELD***");
    cursor = instance.getAllRowsIterator(DBNAMERECORD, NameRecord.NAME, new ArrayList<Field>(Arrays.asList(NameRecord.ACTIVE_NAMESERVERS)));
    while (cursor.hasNext()) {
      System.out.println(cursor.nextJSONObject().toString());
    }
  }
//  private static NameRecord createNameRecord(String name, String key1, String key2, String value) throws Exception {
//    ValuesMap valuesMap = new ValuesMap();
//    ArrayList<String> x = new ArrayList();
//    x.add(value);
//    x.add(value);
//    x.add(value);
//    x.add(value);
//    x.add(value);
//    x.add(value);
//    x.add(value);
//    x.add(value);
//
////    Arrays.asList(value)
//    valuesMap.put(key1, x);
//    valuesMap.put(key2, x);
//
//    HashSet<Integer> y = new HashSet<Integer>();
//    y.add(0);
//    y.add(1);
//    y.add(2);
//    return new NameRecord(name, y, name + "-2", valuesMap);
//  }
//
//  // THIS ISN'T ONLY TEST CODE
//  // the -clear option is currently used by the EC2 installer so keep it working
//  // this use will probably go away at some point
//  public static void main(String[] args) throws Exception, RecordNotFoundException, FieldNotFoundException, RecordExistsException {
//    if (args.length > 0 && args[0].startsWith("-clear")) {
//      dropAllDatabases();
////    } else if (args.length > 0) {
////      String configFile = args[0];
////      NameServer.nodeID = 0;
////      listDatabases();
////      runtest(configFile);
//    } else if (args.length > 0) {
//      queryTest();
//    } else {
//      NameServer.nodeID = 4;
//      listDatabases();
//      queryTest();
//    }
//    //printFieldsTest();
//    //retrieveFieldTest();
//    System.exit(0);
//  }
//
//  private static void listDatabases() throws Exception {
//    MongoClient mongoClient = new MongoClient("localhost");
//    List<String> names = mongoClient.getDatabaseNames();
//    System.out.println(names.toString());
//    //updateFieldTest();
//    //retrieveTest();
//  }
//
//  public static void dropAllDatabases() {
//    MongoClient mongoClient;
//    try {
//      mongoClient = new MongoClient("localhost");
//    } catch (UnknownHostException e) {
//      GNS.getLogger().severe("Unable to open Mongo DB: " + e);
//      return;
//    }
//    List<String> names = mongoClient.getDatabaseNames();
//    for (String name : names) {
//      mongoClient.dropDatabase(name);
//    }
//    System.out.println("Dropped mongo DBs: " + names.toString());
//    // reinit the instance
//    getInstance().init();
//  }
//
//  
//
//  public static void runtest(String configFile) throws FieldNotFoundException, Exception, RecordNotFoundException, RecordExistsException {
//    ConfigFileInfo.readHostInfo(configFile, NameServer.nodeID);
//    HashFunction.initializeHashFunction();
//    MongoRecords instance = MongoRecords.getInstance();
//    instance.reset(collectionSpecs.get(0).getName());
//    for (CollectionSpec spec : collectionSpecs) {
//      String collectionName = spec.getName();
//
//      List<DBObject> list = instance.db.getCollection(collectionName).getIndexInfo();
//      for (DBObject o : list) {
//        System.out.println(o);
//      }
//      System.out.println(MongoRecords.getInstance().db.getCollection(collectionName).getStats().toString());
//    }
//
//    Random random = new Random();
//    NameRecord n = null;
//    String key1 = "ABCD";
//    String key2 = "PQRS";
//    int count = 100;
//    for (int i = 0; i < count; i++) {
//      NameRecord x = createNameRecord(Long.toHexString(random.nextLong()), key1, key2, Long.toHexString(random.nextLong()));
//      if (i == count / 2) {
//        n = x;
//      }
//      instance.insert(collectionSpecs.get(0).getName(), x.getName(), x.toJSONObject());
//    }
//
//
//    // test update latency
//
//
//
//    // test iterator
//    ArrayList<Field> fields = new ArrayList<Field>();
////    fields.add(NameRecord.ACTIVE_NAMESERVERS);
////    Object iterator = instance.getIterator(collectionSpecs.get(0).getName(), NameRecord.NAME, fields);
////    if (iterator != null) {
////      int i = 0;
////      while (true) {
////        i++;
////        HashMap<Field,Object> hashMap = instance.next(iterator,NameRecord.NAME,fields);
////        if (hashMap == null) {
////          System.out.println(" Iterating .... complete. " + i);
////          break;
////        }
////
////        NameRecord nameRecord = new NameRecord(hashMap);
////        System.out.println("Got record " + i + " name = " + nameRecord.getName() + " Actives = " + nameRecord.getActiveNameServers());
////
////      }
////    }
////    instance.returnIterator();
////    System.exit(0);
//
////    System.out.println("LOOKUP BY GUID =>" + instance.lookup(n.getName(), true));
////    instance.update(n.getName(), "timeToLive", "777");
////    System.out.println("LOOKUP AFTER 777 =>" + instance.lookup(n.getName(), true));
////    instance.update(n.getName(), "FRANK", "777");
////    System.out.println("LOOKUP AFTER FRANK =>" + instance.lookup(n.getName(), true));
//
//    JSONObject json = instance.lookup(collectionSpecs.get(0).getName(), n.getName(), true);
//    System.out.println("LOOKUP BY GUID => " + json);
//    NameRecord record = new NameRecord(json);
////    record.updateKey("FRED", new ArrayList<String>(Arrays.asList("BARNEY")), null, UpdateOperation.REPLACE_ALL);
//    System.out.println("JSON AFTER UPDATE => " + record.toJSONObject());
//    System.out.println("READ FIELD " + NameRecord.ACTIVE_NAMESERVERS + " Value = "
//            + MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.ACTIVE_NAMESERVERS.getFieldName()));
//
//    // reading multiple fields
//    ArrayList<String> fieldStrings = new ArrayList<String>();
//    fieldStrings.add(NameRecord.ACTIVE_NAMESERVERS.getFieldName());
//    fieldStrings.add(NameRecord.ACTIVE_PAXOS_ID.getFieldName());
//    fieldStrings.add(NameRecord.TIME_TO_LIVE.getFieldName());
////    fields.add(NameRecord.KEY);
//    System.out.println("OLD READ MULTIPLE FIELDS. FIELDS => " + fieldStrings);
//    System.out.println("OLD READ MULTIPLE FIELDS. VALUES => " + MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(),
//            fieldStrings, false));
//
//    fields = new ArrayList<Field>();
//    fields.add(NameRecord.ACTIVE_NAMESERVERS);
//    fields.add(NameRecord.ACTIVE_PAXOS_ID);
//    fields.add(NameRecord.TIME_TO_LIVE);
//
//
//    System.out.println("\n\n");
//    System.out.println("Reading fields => " + fields);
//    System.out.println("Values are => " + MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, fields));
//    System.out.println("\n\n");
//
//    // now reading values from ValuesMap
//    ArrayList<Field> userKeys = new ArrayList<Field>();
//    userKeys.add(new Field(key1, FieldType.LIST_STRING));
//    userKeys.add(new Field(key2, FieldType.LIST_STRING));
//    System.out.println("Reading Fields => " + fields + "\t and User Fields => " + userKeys);
//    System.out.println("All values => " + MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, fields, NameRecord.VALUES_MAP, userKeys));
//
//    // now update the values of multiple fields
//    ArrayList<Object> values = new ArrayList<Object>();
//    HashSet<Integer> newActives = new HashSet<Integer>();
//    newActives.add(400);
//    newActives.add(4000);
//    newActives.add(40000);
//    String dummyPaxosID = "DUMMY-Paxos-ID";
//    values.add(newActives);
//    values.add(dummyPaxosID);
//    values.add(1000);
//    instance.update(collectionSpecs.get(0).getName(), n.getName(), NameRecord.NAME, fields, values);
//    System.out.println("\nUpdate COMPLETE\n");
//    // now update user keys values
//    int maxThreads = 5;
//    ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(maxThreads);
//
//    for (int i = 0; i < 100000; i++) {
//      executorService.submit(new SendUpdate(n.getName()));
//      Thread.sleep(1);
////      if (i%10 == 0) Thread.sleep(10);
////      if (i%1000 == 0) System.out.println("\t" + i);
////      ArrayList<Object> userKeysUpdates = new ArrayList<Object>();
////      ArrayList<String> userKeysValues1 = new ArrayList<String>();
////      userKeysValues1.add(Util.randomString(100));
//////      userKeysValues1.add("MY");userKeysValues1.add("NAME");userKeysValues1.add("IS");userKeysValues1.add("RED");
////      ArrayList<String> userKeysValues2 = new ArrayList<String>();
////      userKeysValues2.add(Util.randomString(100));
//////      userKeysValues2.add("MERA");userKeysValues2.add("NAAM");userKeysValues2.add("LAAL");userKeysValues2.add("HAI");
////      userKeysUpdates.add(userKeysValues1);userKeysUpdates.add(userKeysValues2);
////      long t0 = System.currentTimeMillis();
////      instance.update(collectionSpecs.get(0).getName(),n.getName(),NameRecord.NAME, fields,values, NameRecord.VALUES_MAP, userKeys, userKeysUpdates);
////      long t1 = System.currentTimeMillis();
////      if (t1 - t0 > 20) {
////        System.out.println("Long latency Request\t" + i + "\ttime\t" + t0 + "\tlatency\t"  + (t1 - t0));
////      }
//    }
//
//    System.exit(2);
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//    System.out.println("\n");
//    System.out.println("AFTER UPDATE: Reading fields => " + fields + "\t and User Fields => " + userKeys);
//    System.out.println("AFTER UPDATE: All values => " + MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, fields, NameRecord.VALUES_MAP, userKeys));
//    System.out.println("\n");
//
//
//    // update values map field directly, not a specific key in values map
//    ValuesMap vMap = new ValuesMap();
//    vMap.put(key1, new ArrayList<String>(Arrays.asList("All the worlds a stage".split(" "))));
//    vMap.put(key2, new ArrayList<String>(Arrays.asList("The atmosphere is electric".split(" "))));
//
//    fields.clear();
//    fields.add(NameRecord.VALUES_MAP);
//
//    values.clear();
//    values.add(vMap);
//
//    instance.update(collectionSpecs.get(0).getName(), n.getName(), NameRecord.NAME, fields, values);
//    System.out.println("ValuesMap update complete");
//
//    System.out.println("\n");
//    System.out.println("After ValuesMap update: Reading fields => " + fields);
//    System.out.println("After ValuesMap update: Values are => " + MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, fields));
//    System.out.println("\n");
//
//
//    // update old values map also
//    vMap = new ValuesMap();
//    vMap.put(key1, new ArrayList<String>(Arrays.asList("old old old old".split(" "))));
//    vMap.put(key2, new ArrayList<String>(Arrays.asList("gold gold gold gold gold gold gold".split(" "))));
//
//    fields.clear();
//    fields.add(NameRecord.OLD_VALUES_MAP);
//
//    values.clear();
//    values.add(vMap);
//
//    instance.update(collectionSpecs.get(0).getName(), n.getName(), NameRecord.NAME, fields, values);
//    System.out.println("Writing to old values map complete.");
//
//    // lookup multiple values map with one lookup
//    fields.clear();
//    fields.add(NameRecord.VALUES_MAP);
//    fields.add(NameRecord.OLD_VALUES_MAP);
//    System.out.println("\n");
//    System.out.println("Reading two values map: Reading fields => " + fields);
//    System.out.println("Reading two values map: Values are => " + MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, fields));
//    System.out.println("\n");
//
//
//    // test code for name record class.
//
//    // try to get a non-existent key
//    String nonexistingKey = "Non-Existing-Key";
//    fields.clear();
//    fields.add(new Field(nonexistingKey, FieldType.LIST_STRING)); // this will not exist
//    NameRecord nr = new NameRecord(MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, null, NameRecord.VALUES_MAP, fields));
//
//    System.out.println("\nName = " + nr.getName());
//    System.out.println("Key exists = " + nr.containsKey(nonexistingKey));
//    try {
//      System.out.println("Trying to get = " + nr.getKey(nonexistingKey));
//    } catch (FieldNotFoundException e) {
//      System.out.println("Since key does not exist, exception is expected.");
//    }
//    System.out.println("\n");
//
//    // try to get an existing key
//    String existingKey = key1;
//    fields.clear();
//    fields.add(new Field(existingKey, FieldType.LIST_STRING)); // this will not exist
//    nr = new NameRecord(MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, null, NameRecord.VALUES_MAP, fields));
//
//    System.out.println("\nName = " + nr.getName());
//    System.out.println("Key exists = " + nr.containsKey(existingKey));
//    try {
//      System.out.println("Value of key = " + nr.getKey(existingKey));
//    } catch (FieldNotFoundException e) {
//      System.out.println("Since key does not exist, exception is expected.");
//    }
//    System.out.println("\n");
//
//
//    // test for method NameRecord.containsActiveNameServer
//    fields.clear();
//    fields.add(NameRecord.ACTIVE_NAMESERVERS);
//    nr = new NameRecord(MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, fields));
//
//    System.out.println("\nName = " + nr.getName());
//    int active = 400;
//    System.out.println("Contains active name server = " + active + "\t" + nr.containsActiveNameServer(active));
//    active = 401;
//    System.out.println("Contains active name server = " + active + "\t" + nr.containsActiveNameServer(active));
//    System.out.println("\n");
//
//
//    // test for method: NameRecord.getPaxosStatus
//    fields.clear();
//    fields.add(NameRecord.ACTIVE_PAXOS_ID);
//    fields.add(NameRecord.OLD_ACTIVE_PAXOS_ID);
//    nr = new NameRecord(MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, fields));
//
//    System.out.println("\nName = " + nr.getName());
//    String oldPaxosID = nr.getName() + "-1";
//    System.out.println("Paxos stats for old Paxos ID = " + oldPaxosID + "\t is " + nr.getPaxosStatus(oldPaxosID));
//    String newPaxosID = dummyPaxosID;
//    System.out.println("Paxos stats for new Paxos ID = " + newPaxosID + "\t is " + nr.getPaxosStatus(newPaxosID));
//    String randomString = "nasdcuhao;sfj";
//    System.out.println("Paxos stats for random string Paxos ID = " + randomString + "\t is " + nr.getPaxosStatus(randomString));
//    System.out.println("\n");
//
//    // test for method: NameRecord.getOldValuesOnPaxosIDMatch
//    fields.clear();
//    fields.add(NameRecord.OLD_ACTIVE_PAXOS_ID);
//    fields.add(NameRecord.OLD_VALUES_MAP);
//    nr = new NameRecord(MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, fields));
//
//    System.out.println("\nName = " + nr.getName());
//    oldPaxosID = nr.getName() + "-1";
//    System.out.println("Old values map for old paxos id = " + oldPaxosID + "\t is \t" + nr.getOldValuesOnPaxosIDMatch(oldPaxosID));
//    randomString = "as;dcja;ifj";
//    System.out.println("Old values map for random string paxos id = " + randomString + "\t is \t" + nr.getOldValuesOnPaxosIDMatch(randomString));
//    System.out.println("\n");
//
//
//    // now testing update methods. so we initialize record map first.
//    NameServer.recordMap = (BasicRecordMap) Util.createObject(StartNameServer.dataStore.getClassName(),
//            MongoRecords.DBNAMERECORD);
//
//    // test for NameRecord.updateKey
//    fields.clear();
//    fields.add(new Field(key1, FieldType.LIST_STRING));
//    nr = new NameRecord(MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, null, NameRecord.VALUES_MAP, fields));
//    System.out.println("\nName = " + nr.getName());
//    System.out.println("\nRead Key = " + key1 + "\tValue = " + nr.getKey(key1));
//    System.out.println("\n");
//
//
//    System.out.println("\nName = " + nr.getName());
//    nr.updateKey(key1, new ArrayList<String>(Arrays.asList("I wanna be a millionaire".split(" "))), null, UpdateOperation.REPLACE_ALL);
//    System.out.println("Updating key = " + key1 + " to value = I wanna be a millionaire");
//
//    nr = new NameRecord(MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, null, NameRecord.VALUES_MAP, fields));
//    System.out.println("\nAfter update: Name = " + nr.getName());
//    System.out.println("After update: Read Key = " + key1 + "\tValue = " + nr.getKey(key1));
//    System.out.println("\n");
//
//    // test for NameRecord.handleCurrentActiveStop
//    fields.clear();
//    fields.add(NameRecord.ACTIVE_PAXOS_ID);
//    fields.add(NameRecord.VALUES_MAP);
//    nr = new NameRecord(MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, fields));
//    System.out.println("\nName = " + nr.getName());
//    String currentPaxosID = nr.getActivePaxosID();
//    System.out.println("Read ActivePaxoID = " + currentPaxosID);
//    System.out.println("Read Values Map = " + nr.getValuesMap().toString());
//    System.out.println("\n");
//
//    nr.handleCurrentActiveStop(currentPaxosID);
//    System.out.println("Handling stopping of paxos instance " + currentPaxosID);
//
//
//    fields.clear();
//    fields.add(NameRecord.ACTIVE_PAXOS_ID);
//    fields.add(NameRecord.VALUES_MAP);
//    fields.add(NameRecord.OLD_ACTIVE_PAXOS_ID);
//    fields.add(NameRecord.OLD_VALUES_MAP);
//    fields.add(NameRecord.ACTIVE_NAMESERVERS);
//    nr = new NameRecord(MongoRecords.getInstance().lookup(collectionSpecs.get(0).getName(), record.getName(), NameRecord.NAME, fields));
//    System.out.println("\nAfter stopping: Name = " + nr.getName());
//    System.out.println("After stopping: ActivePaxosID = " + nr.getActivePaxosID());
//    System.out.println("After stopping: ValuesMap = " + nr.getValuesMap());
//    System.out.println("After stopping: OldActivePaxosID = " + nr.getOldActivePaxosID());
//    System.out.println("After stopping: OldValuesMap = " + nr.getOldValuesMap());
//    System.out.println("After stopping: ActiveNameServers = " + nr.getActiveNameServers());
//    System.out.println("\n");
//
//
//    fields.clear();
//
//    System.exit(2);
//
//    instance.update(collectionSpecs.get(0).getName(), record.getName(), record.toJSONObject());
//    JSONObject json2 = instance.lookup(collectionSpecs.get(0).getName(), n.getName(), true);
//    System.out.println("2ND LOOKUP BY GUID => " + json2);
//
//    //System.out.println("LOOKUP BY KEY =>" + instance.lookup(n.getName(), n.getRecordKey().getName(), true));
//
////    System.out.println("DUMP =v");
////    instance.printAllEntries();
////    System.out.println(MongoRecordsV2.getInstance().db.getCollection(COLLECTIONNAME).getStats().toString());
//
//    // next test complete name record lookup, and update, and remove.
//
//    instance.remove(collectionSpecs.get(0).getName(), n.getName());
//    json2 = instance.lookup(collectionSpecs.get(0).getName(), n.getName(), true);
//    System.out.println("SHOULD BE EMPTY => " + json2);
//  }
//}
//class SendUpdate extends TimerTask {
//
//  String name;
//
//  public SendUpdate(String name) {
//    this.name = name;
//  }
//
//  @Override
//  public void run() {
//
//    ArrayList<Field> userKeys = new ArrayList<Field>();
//
//    userKeys.add(new Field("ABCD", FieldType.LIST_STRING));
//    userKeys.add(new Field("PQRS", FieldType.LIST_STRING));
//
//    ArrayList<Object> userKeysUpdates = new ArrayList<Object>();
//    ArrayList<String> userKeysValues1 = new ArrayList<String>();
//    userKeysValues1.add(Util.randomString(100));
////      userKeysValues1.add("MY");userKeysValues1.add("NAME");userKeysValues1.add("IS");userKeysValues1.add("RED");
//    ArrayList<String> userKeysValues2 = new ArrayList<String>();
//    userKeysValues2.add(Util.randomString(100));
////      userKeysValues2.add("MERA");userKeysValues2.add("NAAM");userKeysValues2.add("LAAL");userKeysValues2.add("HAI");
//    userKeysUpdates.add(userKeysValues1);
//    userKeysUpdates.add(userKeysValues2);
//
//    MongoRecords.getInstance().update("NameRecord", name, NameRecord.NAME, null, null, NameRecord.VALUES_MAP, userKeys, userKeysUpdates);
//  }
}