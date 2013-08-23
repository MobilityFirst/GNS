/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.database;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import edu.umass.cs.gns.client.AccountAccess;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.packet.UpdateOperation;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides insert, update, remove and lookup operations for guid, key, record triples using JSONObjects as the intermediate
 * representation
 *
 * @author westy
 */
public class MongoRecords implements NoSQLRecords {

  private static final String DBROOTNAME = "GNS";
  public static final String DBNAMERECORD = "NameRecord";
  public static final String DBREPLICACONTROLLER = "ReplicaControllerRecord";
  //public static final String[] COLLECTIONS = {DBNAMERECORD, DBREPLICACONTROLLER};
  //public static final String PRIMARYKEY = NameRecord.NAME;
  private DB db;
  private String dbName;
  // maintains information about the collections we maintain in the mongo db
  private static Map<String, CollectionSpec> collectionSpecMap = new HashMap<String, CollectionSpec>();

  public CollectionSpec getCollectionSpec(String name) {
    return collectionSpecMap.get(name);
  }

  /**
   * Stores the name, primary key, and index of each collection we maintain in the mongo db.
   */
  static class CollectionSpec {

    private String name;
    private String primaryKey;
    private BasicDBObject index;

    public CollectionSpec(String name, String primaryKey) {
      this.name = name;
      this.primaryKey = primaryKey;
      this.index = new BasicDBObject(primaryKey, 1);
      collectionSpecMap.put(name, this);
    }

    public String getName() {
      return name;
    }

    public String getPrimaryKey() {
      return primaryKey;
    }

    public BasicDBObject getIndex() {
      return index;
    }
  }
  private static List<CollectionSpec> collectionSpecs =
          Arrays.asList(
          new CollectionSpec(DBNAMERECORD, NameRecord.NAME),
          new CollectionSpec(DBREPLICACONTROLLER, ReplicaControllerRecord.NAME));

  public static MongoRecords getInstance() {
    return MongoRecordCollectionHolder.INSTANCE;
  }

  private static class MongoRecordCollectionHolder {

    private static final MongoRecords INSTANCE = new MongoRecords();
  }
  //
  //private static final BasicDBObject NAME_INDEX = new BasicDBObject(PRIMARYKEY, 1);
  //private static final BasicDBObject RECORD_KEY_INDEX = new BasicDBObject(NameRecord.NAME, 1).append(NameRecord.KEY, -1);

  private MongoRecords() {
    init();
  }

  private void init() {
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
    for (CollectionSpec spec : collectionSpecs) {
      initializeIndex(spec.name);
    }
  }

  private void initializeIndex(String collectionName) {
    CollectionSpec spec = getCollectionSpec(collectionName);
    db.getCollection(spec.getName()).ensureIndex(spec.getIndex(), new BasicDBObject("unique", true));
    //db.getCollection(COLLECTIONNAME).ensureIndex(RECORD_KEY_INDEX, new BasicDBObject("unique", true));
  }

  @Override
  public void reset(String collectionName) {
//    ArrayList<JSONObject> result = new ArrayList<JSONObject>();
    if (getCollectionSpec(collectionName) != null) {
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
  public JSONObject lookup(String collectionName, String guid) {
    return lookup(collectionName, guid, false);
  }

  private JSONObject lookup(String collectionName, String guid, boolean explain) {
    db.requestStart();
    try {
      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
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
      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      BasicDBObject projection = new BasicDBObject(key, 1).append("_id", 0);
      //System.out.println("Query: " + query.toString() + " Projection: " + projection);
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
  public void insert(String collectionName, String guid, JSONObject value) {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      DBObject dbObject = (DBObject) JSON.parse(value.toString());
      collection.insert(dbObject);
    } finally {
      db.requestDone();
    }
  }

  @Override
  public void update(String collectionName, String guid, JSONObject value) {
    db.requestStart();
    try {
      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
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
    updateListValue(collectionName, name, key, new ArrayList(Arrays.asList(value)));
  }

  public void updateField(String collectionName, String guid, String key, Object object) {  
    db.requestStart();
    try {
      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
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
  
  public void updateListValue(String collectionName, String guid, String key, ArrayList<String> value) {
    updateField(collectionName, guid, key, value);
  }

  public void updateListValueInt(String collectionName, String guid, String key, Set<Integer> value) {
   updateField(collectionName, guid, key, value);
  }

  public void updateFieldAsString(String collectionName, String guid, String key, String string) {
    updateField(collectionName, guid, key, string);
  }
  
  public void updateFieldAsMap(String collectionName, String guid, String key, Map map) {
    updateField(collectionName, guid, key, map);
  }
  
  public void updateFieldAsCollection(String collectionName, String guid, String key, Collection list) {
    updateField(collectionName, guid, key, list);
  }
  
//  @Override
//  public void updateListValue(String collectionName, String guid, String key, ArrayList<String> value) {
//    db.requestStart();
//    try {
//      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
//      db.requestEnsureConnection();
//      DBCollection collection = db.getCollection(collectionName);
//      BasicDBObject query = new BasicDBObject(primaryKey, guid);
//      BasicDBObject newValue = new BasicDBObject(key, value);
//      BasicDBObject updateOperator = new BasicDBObject("$set", newValue);
//      collection.update(query, updateOperator);
//    } finally {
//      db.requestDone();
//    }
//  }
//
//
//  @Override
//  public void updateListValueInt(String collectionName, String guid, String key, Set<Integer> value) {
//    db.requestStart();
//    try {
//      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
//      db.requestEnsureConnection();
//      DBCollection collection = db.getCollection(collectionName);
//      BasicDBObject query = new BasicDBObject(primaryKey, guid);
//      BasicDBObject newValue = new BasicDBObject(key, value);
//      BasicDBObject updateOperator = new BasicDBObject("$set", newValue);
//      collection.update(query, updateOperator);
//    } finally {
//      db.requestDone();
//    }
//  }
//
//  @Override
//  public void updateFieldAsString(String collectionName, String guid, String key, String string) {
//    db.requestStart();
//    try {
//      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
//      db.requestEnsureConnection();
//      DBCollection collection = db.getCollection(collectionName);
//      BasicDBObject query = new BasicDBObject(primaryKey, guid);
//      BasicDBObject newValue = new BasicDBObject(key, string);
//      BasicDBObject updateOperator = new BasicDBObject("$set", newValue);
//      collection.update(query, updateOperator);
//    } finally {
//      db.requestDone();
//    }
//  }
//  
//  @Override
//  public void updateFieldAsMap(String collectionName, String guid, String key, Map map) {
//    db.requestStart();
//    try {
//      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
//      db.requestEnsureConnection();
//      DBCollection collection = db.getCollection(collectionName);
//      BasicDBObject query = new BasicDBObject(primaryKey, guid);
//      BasicDBObject newValue = new BasicDBObject(key, map);
//      BasicDBObject updateOperator = new BasicDBObject("$set", newValue);
//      collection.update(query, updateOperator);
//    } finally {
//      db.requestDone();
//    }
//  }
//  
//  @Override
//  public void updateFieldAsCollection(String collectionName, String guid, String key, Collection list) {
//    db.requestStart();
//    try {
//      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
//      db.requestEnsureConnection();
//      DBCollection collection = db.getCollection(collectionName);
//      BasicDBObject query = new BasicDBObject(primaryKey, guid);
//      BasicDBObject newValue = new BasicDBObject(key, list);
//      BasicDBObject updateOperator = new BasicDBObject("$set", newValue);
//      collection.update(query, updateOperator);
//    } finally {
//      db.requestDone();
//    }
//  }

  @Override
  public boolean contains(String collectionName, String guid) {
    db.requestStart();
    try {
      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
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
      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(primaryKey, guid);
      collection.remove(query);
    } finally {
      db.requestDone();
    }
  }

  /**
   * THIS SHOULD NEVER BE CALLED IN PRODUCTION CODE UNLESS IT IS A TEST FUNCTION.
   *
   * @param collectionName
   * @return
   */
  @Override
  public ArrayList<JSONObject> retrieveAllEntries(String collectionName) {
    ArrayList<JSONObject> result = new ArrayList<JSONObject>();
    db.requestStart();
    try {
      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      // get all documents that have a name field (all doesn't work because of extra stuff mongo adds to the database)
      BasicDBObject query = new BasicDBObject(primaryKey, new BasicDBObject("$exists", true));
      DBCursor cursor = collection.find(query);
      while (cursor.hasNext()) {
        DBObject obj = cursor.next();
        result.add(new JSONObject(obj.toString()));
      }
      return result;
    } catch (JSONException e) {
      GNS.getLogger().warning("Unable to parse JSON: " + e);
      return null;
    } finally {
      db.requestDone();
    }
  }

  @Override
  public Set<String> keySet(String collectionName) {
    Set<String> result = new HashSet<String>();
    db.requestStart();
    try {
      String primaryKey = getCollectionSpec(collectionName).getPrimaryKey();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      // get all documents that have a name field (all doesn't work because of extra stuff mongo adds to the database)
      BasicDBObject query = new BasicDBObject(primaryKey, new BasicDBObject("$exists", true));
      DBCursor cursor = collection.find(query);

      while (cursor.hasNext()) {
        DBObject obj = cursor.next();
        JSONObject json = new JSONObject(obj.toString());
        result.add(json.getString(primaryKey));
      }
      return result;
    } catch (JSONException e) {
      GNS.getLogger().warning("Unable to parse JSON: " + e);
      return null;
    } finally {
      db.requestDone();
    }
  }

  @Override
  public void printAllEntries(String collectionName) {
    for (JSONObject entry : retrieveAllEntries(collectionName)) {
      System.out.println(entry.toString());
    }

  }

  @Override
  public String toString() {
    return "DB " + dbName;
  }

  //test code
  private static NameRecord createNameRecord(String name, String key, String value) throws Exception {
    return new NameRecord(name, new NameRecordKey(key), new ArrayList(Arrays.asList(value)));
  }

  // test code
  public static void main(String[] args) throws Exception {
    NameServer.nodeID = 4;
    listDatabases();
    if (args.length > 0 && args[0].startsWith("-clear")) {
      dropAllDatabases();
    }
    runtest();
    //printFieldsTest();
    //retrieveFieldTest();
    System.exit(0);
  }

  private static void listDatabases() throws Exception {
    MongoClient mongoClient = new MongoClient("localhost");
    List<String> names = mongoClient.getDatabaseNames();
    System.out.println(names.toString());

    //updateFieldTest();
    //retrieveTest();
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

  private static void retrieveTest() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    MongoRecords instance = MongoRecords.getInstance();
    System.out.println(instance.lookup(collectionSpecs.get(0).getName(), "1A434C0DAA0B17E48ABD4B59C632CF13501C7D24"));
  }

  private static void retrieveFieldTest() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    MongoRecords instance = MongoRecords.getInstance();
    System.out.println(instance.lookup(collectionSpecs.get(0).getName(), "1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", AccountAccess.ACCOUNT_INFO));
  }

  private static void printFieldsTest() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    MongoRecords instance = MongoRecords.getInstance();
    instance.printAllEntries(collectionSpecs.get(0).getName());
  }

  private static void updateFieldTest() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    MongoRecords instance = MongoRecords.getInstance();
    instance.updateSingleValue(collectionSpecs.get(0).getName(), "1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "POSITION", "HERE");
    System.out.println(instance.lookup(collectionSpecs.get(0).getName(), "1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "POSITION"));
  }

  public static void runtest() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    MongoRecords instance = MongoRecords.getInstance();
    instance.reset(collectionSpecs.get(0).getName());
    for (CollectionSpec spec : collectionSpecs) {
      //for (String collectionName : COLLECTIONS) {
      String collectionName = spec.getName();
      List<DBObject> list = instance.db.getCollection(collectionName).getIndexInfo();
      for (DBObject o : list) {
        System.out.println(o);
      }
      System.out.println(MongoRecords.getInstance().db.getCollection(collectionName).getStats().toString());
    }
    Random random = new Random();
    NameRecord n = null;
    for (int i = 0; i < 1000; i++) {
      NameRecord x = createNameRecord(Long.toHexString(random.nextLong()), Long.toHexString(random.nextLong()), Long.toHexString(random.nextLong()));
      if (i == 500) {
        n = x;
      }
      instance.insert(collectionSpecs.get(0).getName(), x.getName(), x.toJSONObject());
    }

//    System.out.println("LOOKUP BY GUID =>" + instance.lookup(n.getName(), true));
//    instance.update(n.getName(), "timeToLive", "777");
//    System.out.println("LOOKUP AFTER 777 =>" + instance.lookup(n.getName(), true));
//    instance.update(n.getName(), "FRANK", "777");
//    System.out.println("LOOKUP AFTER FRANK =>" + instance.lookup(n.getName(), true));
//    
    JSONObject json = instance.lookup(collectionSpecs.get(0).getName(), n.getName(), true);
    System.out.println("LOOKUP BY GUID => " + json);
    NameRecord record = new NameRecord(json);
    record.updateField("FRED", new ArrayList<String>(Arrays.asList("BARNEY")), null, UpdateOperation.REPLACE_ALL);
    System.out.println("JSON AFTER UPDATE => " + record.toJSONObject());
    instance.update(collectionSpecs.get(0).getName(), record.getName(), record.toJSONObject());
    JSONObject json2 = instance.lookup(collectionSpecs.get(0).getName(), n.getName(), true);
    System.out.println("2ND LOOKUP BY GUID => " + json2);
    //
    //System.out.println("LOOKUP BY KEY =>" + instance.lookup(n.getName(), n.getRecordKey().getName(), true));

//    System.out.println("DUMP =v");
//    instance.printAllEntries();
//    System.out.println(MongoRecordsV2.getInstance().db.getCollection(COLLECTIONNAME).getStats().toString());
    //
    instance.remove(collectionSpecs.get(0).getName(), n.getName());
    json2 = instance.lookup(collectionSpecs.get(0).getName(), n.getName(), true);
    System.out.println("SHOULD BE EMPTY => " + json2);
  }
}
