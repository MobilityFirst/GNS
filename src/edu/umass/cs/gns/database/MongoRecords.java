/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.database;

import com.mongodb.*;
import com.mongodb.util.JSON;
import edu.umass.cs.gns.client.AccountAccess;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.UpdateOperation;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.UnknownHostException;
import java.util.*;

//import edu.umass.cs.gnrs.nameserver.NameRecord;
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
  public static final String[] COLLECTIONS = {DBNAMERECORD, DBREPLICACONTROLLER};
  public static final String PRIMARYKEY = NameRecord.NAME;
  private DB db;
  private String dbName;

  public static MongoRecords getInstance() {
    return MongoRecordCollectionHolder.INSTANCE;
  }

  private static class MongoRecordCollectionHolder {

    private static final MongoRecords INSTANCE = new MongoRecords();
  }
  //
  private static final BasicDBObject NAME_INDEX = new BasicDBObject(PRIMARYKEY, 1);
  //private static final BasicDBObject RECORD_KEY_INDEX = new BasicDBObject(NameRecord.NAME, 1).append(NameRecord.KEY, -1);

  private MongoRecords() {
    try {
      // use a unique name in case we have more than one on a machine
      dbName = DBROOTNAME + "-" + NameServer.nodeID;
      MongoClient mongoClient = new MongoClient("localhost");
      db = mongoClient.getDB(dbName);
      intializeIndexes();
    } catch (UnknownHostException e) {
      GNS.getLogger().severe("Unable to open Mongo DB: " + e);
    }
  }

  private void intializeIndexes() {
    for (String collectioName : COLLECTIONS) {
      db.getCollection(collectioName).ensureIndex(NAME_INDEX, new BasicDBObject("unique", true));
    }
    //db.getCollection(COLLECTIONNAME).ensureIndex(RECORD_KEY_INDEX, new BasicDBObject("unique", true));
  }

  @Override
  public void reset() {
//    ArrayList<JSONObject> result = new ArrayList<JSONObject>();
    db.requestStart();
    try {
      db.requestEnsureConnection();
      for (String collectionName : COLLECTIONS) {
        db.getCollection(collectionName).dropIndexes();
        db.getCollection(collectionName).drop();
      }
      // IMPORTANT... recreate the index
      intializeIndexes();
    } finally {
      db.requestDone();
    }
    GNS.getLogger().info("MONGO V2 DB RESET. DBNAME: " + dbName + " Collection name " + COLLECTIONS);
  }

  @Override
  public JSONObject lookup(String collectionName, String guid) {
    return lookup(collectionName, guid, false);
  }

  private JSONObject lookup(String collectionName, String guid, boolean explain) {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(PRIMARYKEY, guid);
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
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(PRIMARYKEY, guid);
      BasicDBObject projection = new BasicDBObject(key, 1).append("_id", 0);
      //System.out.println("Query: " + query.toString() + " Projection: " + projection);
      DBCursor cursor = collection.find(query, projection);
      if (explain) {
        System.out.println(cursor.explain().toString());
      }
      if (cursor.hasNext()) {
        DBObject obj = cursor.next();
        return new JSONObject(obj.toString()).getString(key);
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
  public void insert(String COLLECTIONNAME, String guid, JSONObject value) {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(COLLECTIONNAME);
      DBObject dbObject = (DBObject) JSON.parse(value.toString());
      collection.insert(dbObject);
    } finally {
      db.requestDone();
    }
  }

  @Override
  public void update(String COLLECTIONNAME, String guid, JSONObject value) {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(COLLECTIONNAME);
      BasicDBObject query = new BasicDBObject(PRIMARYKEY, guid);
      DBObject dbObject = (DBObject) JSON.parse(value.toString());
      collection.update(query, dbObject);
    } finally {
      db.requestDone();
    }
  }

  public void updateSingleValue(String collectionName, String name, String key, String value) {
    updateListValue(collectionName, name, key, new ArrayList(Arrays.asList(value)));
  }

  @Override
  public void updateListValue(String collectionName, String guid, String key, ArrayList<String> value) {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(PRIMARYKEY, guid);
      BasicDBObject newValue = new BasicDBObject(key, value);
      BasicDBObject updateOperator = new BasicDBObject("$set", newValue);
      collection.update(query, updateOperator);
    } finally {
      db.requestDone();
    }
  }
  
  public void updateField(String collectionName, String guid, String key, String string) {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(PRIMARYKEY, guid);
      BasicDBObject newValue = new BasicDBObject(key, string);
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
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(PRIMARYKEY, guid);
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
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      BasicDBObject query = new BasicDBObject(PRIMARYKEY, guid);
      collection.remove(query);
    } finally {
      db.requestDone();
    }
  }

  @Override
  public ArrayList<JSONObject> retrieveAllEntries(String collectionName) {
    ArrayList<JSONObject> result = new ArrayList<JSONObject>();
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      // get all documents that have a name field (all doesn't work because of extra stuff mongo adds to the database)
      BasicDBObject query = new BasicDBObject(PRIMARYKEY, new BasicDBObject("$exists", true));
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
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      // get all documents that have a name field (all doesn't work because of extra stuff mongo adds to the database)
      BasicDBObject query = new BasicDBObject(PRIMARYKEY, new BasicDBObject("$exists", true));
      DBCursor cursor = collection.find(query);

      while (cursor.hasNext()) {
        DBObject obj = cursor.next();
        JSONObject json = new JSONObject(obj.toString());
        result.add(json.getString(PRIMARYKEY));
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
  public void printAllEntries() {
    for (String collectionName : COLLECTIONS) {
      for (JSONObject entry : retrieveAllEntries(collectionName)) {
        System.out.println(entry.toString());
      }
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
    NameServer.nodeID = 2;
    //listDatabases();
    //clear();
    printFieldsTest();
    retrieveFieldTest();
    //runtest(args);
    //System.exit(0);
    
  }

  private static void listDatabases() throws Exception {
    MongoClient mongoClient = new MongoClient("localhost");
    List<String> names = mongoClient.getDatabaseNames();
    System.out.println(names.toString());

    //updateFieldTest();
    //retrieveTest();
  }

  private static void clear() throws Exception {
    MongoClient mongoClient = new MongoClient("localhost");
    List<String> names = mongoClient.getDatabaseNames();
    for (String name : names) {
      mongoClient.dropDatabase(name);
    }
    System.out.println("Dropped " + names.toString());
  }

  private static void retrieveTest() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    MongoRecords instance = MongoRecords.getInstance();
    System.out.println(instance.lookup(COLLECTIONS[0], "1A434C0DAA0B17E48ABD4B59C632CF13501C7D24"));
  }

  private static void retrieveFieldTest() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    MongoRecords instance = MongoRecords.getInstance();
    System.out.println(instance.lookup(COLLECTIONS[0], "1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", AccountAccess.ACCOUNT_INFO));
  }
  
  private static void printFieldsTest() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    MongoRecords instance = MongoRecords.getInstance();
    instance.printAllEntries();
  }

  private static void updateFieldTest() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    MongoRecords instance = MongoRecords.getInstance();
    instance.updateSingleValue(COLLECTIONS[0], "1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "POSITION", "HERE");
    System.out.println(instance.lookup(COLLECTIONS[0], "1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "POSITION"));
  }

  public static void runtest(String[] args) throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    MongoRecords instance = MongoRecords.getInstance();
    instance.reset();
    List<DBObject> list = instance.db.getCollection(COLLECTIONS[0]).getIndexInfo();
    for (DBObject o : list) {
      System.out.println(o);
    }
    System.out.println(MongoRecords.getInstance().db.getCollection(COLLECTIONS[0]).getStats().toString());
    Random random = new Random();
    NameRecord n = null;
    for (int i = 0; i < 1000; i++) {
      NameRecord x = createNameRecord(Long.toHexString(random.nextLong()), Long.toHexString(random.nextLong()), Long.toHexString(random.nextLong()));
      if (i == 500) {
        n = x;
      }
      instance.insert(COLLECTIONS[0], x.getName(), x.toJSONObject());
    }

//    System.out.println("LOOKUP BY GUID =>" + instance.lookup(n.getName(), true));
//    instance.update(n.getName(), "timeToLive", "777");
//    System.out.println("LOOKUP AFTER 777 =>" + instance.lookup(n.getName(), true));
//    instance.update(n.getName(), "FRANK", "777");
//    System.out.println("LOOKUP AFTER FRANK =>" + instance.lookup(n.getName(), true));
//    
    JSONObject json = instance.lookup(COLLECTIONS[0], n.getName(), true);
    System.out.println("LOOKUP BY GUID => " + json);
    NameRecord record = new NameRecord(json);
    record.updateValuesMap("FRED", new ArrayList<String>(Arrays.asList("BARNEY")), null, UpdateOperation.REPLACE_ALL);
    System.out.println("JSON AFTER UPDATE => " + record.toJSONObject());
    instance.update(COLLECTIONS[0], record.getName(), record.toJSONObject());
    JSONObject json2 = instance.lookup(COLLECTIONS[0], n.getName(), true);
    System.out.println("2ND LOOKUP BY GUID => " + json2);
    //
    //System.out.println("LOOKUP BY KEY =>" + instance.lookup(n.getName(), n.getRecordKey().getName(), true));

//    System.out.println("DUMP =v");
//    instance.printAllEntries();
//    System.out.println(MongoRecordsV2.getInstance().db.getCollection(COLLECTIONNAME).getStats().toString());
    //
    instance.remove(COLLECTIONS[0], n.getName());
    json2 = instance.lookup(COLLECTIONS[0], n.getName(), true);
    System.out.println("SHOULD BE EMPTY => " + json2);
  }
}
