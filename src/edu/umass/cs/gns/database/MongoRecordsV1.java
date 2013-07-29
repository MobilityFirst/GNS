/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.database;

import com.mongodb.*;
import com.mongodb.util.JSON;
import edu.umass.cs.gns.main.GNS;
//import edu.umass.cs.gnrs.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.NameRecordV1;
import edu.umass.cs.gns.nameserver.NameServer;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Provides insert, update, remove and lookup operations for guid, key, record triples using JSONObjects as the
 * intermediate representation
 *
 * @author westy
 */
public class MongoRecordsV1 {

  private static final String DBROOTNAME = "gnrs";
  private static final String COLLECTIONNAME = "GNRS";
  DB db;
  String dbName;

  public static MongoRecordsV1 getInstance() {
    return MongoRecordCollectionHolder.INSTANCE;
  }

  private static class MongoRecordCollectionHolder {

    private static final MongoRecordsV1 INSTANCE = new MongoRecordsV1();
  }
  private static final BasicDBObject INDEX = new BasicDBObject(NameRecordV1.NAME, 1).append(NameRecordV1.RECORDKEY, -1);

  private MongoRecordsV1() {
    try {
      // use a unique name in case we have more than one on a machine
      dbName = DBROOTNAME + NameServer.nodeID;
      MongoClient mongoClient = new MongoClient("localhost");
      db = mongoClient.getDB(dbName);
      // create an index for our usual query
      db.getCollection(COLLECTIONNAME).ensureIndex(INDEX);
    } catch (UnknownHostException e) {
      GNS.getLogger().severe("Unable to open Mongo DB: " + e);
    }
  }

  public JSONObject lookup(String guid, String key) {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(COLLECTIONNAME);
      BasicDBObject query = new BasicDBObject(NameRecordV1.NAME, guid).append(NameRecordV1.RECORDKEY, key);
      DBCursor cursor = collection.find(query);
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

  public void insert(String guid, String key, JSONObject value) {
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

  public void update(String guid, String key, JSONObject value) {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(COLLECTIONNAME);
      BasicDBObject query = new BasicDBObject(NameRecordV1.NAME, guid).append(NameRecordV1.RECORDKEY, key);
      DBObject dbObject = (DBObject) JSON.parse(value.toString());
      collection.update(query, dbObject, false, false);
    } finally {
      db.requestDone();
    }
  }

  public boolean contains(String guid, String key) {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(COLLECTIONNAME);
      BasicDBObject query = new BasicDBObject(NameRecordV1.NAME, guid).append(NameRecordV1.RECORDKEY, key);
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

  public void remove(String guid, String key) {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(COLLECTIONNAME);
      BasicDBObject query = new BasicDBObject(NameRecordV1.NAME, guid).append(NameRecordV1.RECORDKEY, key);
      collection.remove(query);
    } finally {
      db.requestDone();
    }
  }

  public void delete(String guid) {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(COLLECTIONNAME);
      BasicDBObject query = new BasicDBObject(NameRecordV1.NAME, guid);
      collection.remove(query);
    } finally {
      db.requestDone();
    }
  }

  public ArrayList<JSONObject> retrieveAllEntries() {
    ArrayList<JSONObject> result = new ArrayList<JSONObject>();
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(COLLECTIONNAME);
      // get all documents that have a name and record key field (all doesn't work because of extra stuff mongo adds to the database)
      BasicDBObject query = new BasicDBObject(NameRecordV1.NAME, new BasicDBObject("$exists", true)).append(NameRecordV1.RECORDKEY, new BasicDBObject("$exists", true));
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

  public void printAllEntries() {
    for (JSONObject entry : retrieveAllEntries()) {
      System.out.println(entry.toString());
    }
  }

  public void reset() {
    ArrayList<JSONObject> result = new ArrayList<JSONObject>();
    db.requestStart();
    try {
      db.requestEnsureConnection();
      db.getCollection(COLLECTIONNAME).dropIndexes();
      db.getCollection(COLLECTIONNAME).drop();
      // IMPORTANT... recreate the index
      db.getCollection(COLLECTIONNAME).ensureIndex(INDEX);
    } finally {
      db.requestDone();
    }
  }

  @Override
  public String toString() {
    return "MongoRecords{" + "name = " + dbName + '}';
  }

  // test code
  
  private JSONObject lookupExplain(String guid, String key) {
    db.requestStart();
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(COLLECTIONNAME);
      BasicDBObject query = new BasicDBObject(NameRecordV1.NAME, guid).append(NameRecordV1.RECORDKEY, key);
      DBCursor cursor = collection.find(query);
      System.out.println(cursor.explain().toString());
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
  private static NameRecordV1 createNameRecord(String name, String key, String value) throws Exception {
    Set<Integer> primary = new HashSet<Integer>();
    primary.add(1);
    primary.add(3);
    primary.add(5);
    return new NameRecordV1(name, new NameRecordKey(key), new ArrayList(Arrays.asList(value)));
  }

  public static void main(String[] args) throws Exception {
    MongoRecordsV1.getInstance().reset();
    List<DBObject> list = MongoRecordsV1.getInstance().db.getCollection(COLLECTIONNAME).getIndexInfo();
    for (DBObject o : list) {
      System.out.println(o);
    }
    System.out.println(MongoRecordsV1.getInstance().db.getCollection(COLLECTIONNAME).getStats().toString());
    Random random = new Random();
    NameRecordV1 n = null;
    for (int i = 0; i < 1000; i++) {
      NameRecordV1 x = createNameRecord(Long.toHexString(random.nextLong()), Long.toHexString(random.nextLong()), Long.toHexString(random.nextLong()));
      if (i == 500) {
        n = x;
      }
      //MongoRecords.getInstance().delete(n.getName());
      MongoRecordsV1.getInstance().insert(x.getName(), x.getRecordKey().getName(), x.toJSONObject());
    }
    System.out.println("LOOKUP =>" + MongoRecordsV1.getInstance().lookupExplain(n.getName(), n.getRecordKey().getName()));
    System.out.println("DUMP =v");
    MongoRecordsV1.getInstance().printAllEntries();
    System.out.println(MongoRecordsV1.getInstance().db.getCollection(COLLECTIONNAME).getStats().toString());
  }
}
