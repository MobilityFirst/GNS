package edu.umass.cs.gns.databaseV2;

/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved
 */
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
import edu.umass.cs.gns.clientsupport.Defs;
import edu.umass.cs.gns.database.AbstractRecordCursor;
import edu.umass.cs.gns.database.MongoCollectionSpec;
import edu.umass.cs.gns.database.MongoRecordCursor;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Provides insert, update, remove and lookup operations for guid, key, record triples using JSONObjects as the intermediate
 * representation.
 *
 * @author westy, Abhigyan
 */
public class MongoRecordsV2 implements NoSQLRecordsV2 {

  private static final String DBROOTNAME = "GNS-V2";
  public static final String DBNAMERECORD = "NameRecord";
  public static final String DBREPLICACONTROLLER = "ReplicaControllerRecord";

  private DB db;
  private String dbName;

  /**
   * Creates database tables for nodeID, by connecting to mongoDB on default port.
   *
   * @param nodeID nodeID of name server
   */
  public MongoRecordsV2(int nodeID) {
    this(nodeID, -1);
  }

  /**
   * Creates database tables for nodeID, by connecting to mongoDB on default port.
   *
   * @param nodeID nodeID of name server
   * @param port port at which mongo is running. if port = -1, mongo connects to default port.
   */
  public MongoRecordsV2(int nodeID, int port) {
    init(nodeID, port);
  }

  private void init(int nodeID, int mongoPort) {
    MongoCollectionSpec.addCollectionSpec(DBNAMERECORD, NameRecord.NAME);
    MongoCollectionSpec.addCollectionSpec(DBREPLICACONTROLLER, ReplicaControllerRecord.NAME);
    // add location as another index
    MongoCollectionSpec.getCollectionSpec(DBNAMERECORD)
            .addOtherIndex(new BasicDBObject(NameRecord.VALUES_MAP.getName() + "." + Defs.LOCATION_FIELD_NAME, "2d"));
    MongoCollectionSpec.getCollectionSpec(DBNAMERECORD)
            .addOtherIndex(new BasicDBObject(NameRecord.VALUES_MAP.getName() + "." + Defs.IPADDRESS_FIELD_NAME, 1));
    try {
      // use a unique name in case we have more than one on a machine
      dbName = DBROOTNAME + "-" + nodeID;
      MongoClient mongoClient;
      if (mongoPort > 0) {
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
    for (MongoCollectionSpec spec : MongoCollectionSpec.allCollectionSpecs()) {
      initializeIndex(spec.getName());
    }
  }

  private void initializeIndex(String collectionName) {
    MongoCollectionSpec spec = MongoCollectionSpec.getCollectionSpec(collectionName);
    db.getCollection(spec.getName()).createIndex(spec.getPrimaryIndex(), new BasicDBObject("unique", true));
    for (BasicDBObject index : spec.getOtherIndexes()) {
      db.getCollection(spec.getName()).createIndex(index);
    }
    GNS.getLogger().info("Indexes initialized");
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
  public void update(String collectionName, String guid, JSONObject value) throws FailedDBOperationException {
    update(collectionName, guid, value, null);
  }

  @Override
  public boolean update(String collectionName, String name, JSONObject value, JSONObject query) throws FailedDBOperationException {
    String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
    DBCollection collection = db.getCollection(collectionName);
    DBObject dbQuery = parseMongoQuery(collectionName, name, query);
    DBObject dbObject = (DBObject) JSON.parse(value.toString());
    WriteResult writeResult;
    try {
      writeResult = collection.update(dbQuery, dbObject);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, dbObject.toString());
    }
    return writeResult.isUpdateOfExisting();
  }

  @Override
  public AbstractRecordCursor getAllRowsIterator(String collection, JSONObject projection) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public JSONObject find(String collectionName, String name) throws RecordNotFoundException {
    return find(collectionName, name, null, false);
  }

  @Override
  public JSONObject find(String collectionName, String name, JSONObject projection) throws RecordNotFoundException {
    return find(collectionName, name, projection, false);
  }

  private JSONObject find(String collectionName, String name, JSONObject projection, boolean explain) throws RecordNotFoundException {
    db.requestStart();
    try {
      String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      DBCursor cursor;
      BasicDBObject query = new BasicDBObject(primaryKey, name);
      if (projection != null) {
        cursor = collection.find(query, parseMongoProjection(projection));
      } else {
        cursor = collection.find(query);
      }
      if (explain) {
        System.out.println(cursor.explain().toString());
      }
      if (cursor.hasNext()) {
        DBObject obj = cursor.next();
        return new JSONObject(obj.toString());
      } else {
        throw new RecordNotFoundException(name);
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("Unable to parse JSON: " + e);
      return null;
    } finally {
      db.requestDone();
    }
  }

  @Override
  public MongoRecordCursor find(String collectionName, JSONObject query, JSONObject projection) {
    return find(collectionName, query, projection, false);
  }

  @Override
  public MongoRecordCursor find(String collectionName, JSONObject query) {
    return find(collectionName, query, null, false);
  }

  private MongoRecordCursor find(String collectionName, JSONObject query, JSONObject projection, boolean explain) {
    db.requestEnsureConnection();
    DBCollection collection = db.getCollection(collectionName);
    DBCursor cursor;
    if (projection != null) {
      cursor = collection.find(parseMongoQuery(collectionName, null, query), parseMongoProjection(projection));
    } else {
      cursor = collection.find(parseMongoQuery(collectionName, null, query));
    }
    if (explain) {
      System.out.println(cursor.explain().toString());
    }
    return new MongoRecordCursor(cursor, MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey());
  }

  private DBObject parseMongoQuery(String collectionName, String name, JSONObject query) {
    assert name != null; // for now
    if (query == null) {
      String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
      return new BasicDBObject(primaryKey, name);
    } else {
      // !!! THIS IS A STUB, DOESN'T WORK !!!!
      return (DBObject) JSON.parse(query.toString());
    }
  }

  private DBObject parseMongoProjection(JSONObject projection) {
    // !!! THIS IS A STUB, DOESN'T WORK !!!!
    DBObject parse = (DBObject) JSON.parse(projection.toString());
    return parse;
  }
  
  private DBObject parseMongoUpdates(JSONObject projection) {
    // !!! THIS IS A STUB, DOESN'T WORK !!!!
    DBObject parse = (DBObject) JSON.parse(projection.toString());
    return parse;
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
  public void remove(String collectionName, String guid) throws FailedDBOperationException {
    String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
    DBCollection collection = db.getCollection(collectionName);
    BasicDBObject query = new BasicDBObject(primaryKey, guid);
    try {
      collection.remove(query);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, query.toString());
    }
  }

  @Override
  public void increment(String collectionName, String name, JSONObject updates)
          throws FailedDBOperationException {
    String primaryKey = MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey().getName();
    DBCollection collection = db.getCollection(collectionName);
    BasicDBObject query = new BasicDBObject(primaryKey, name);
    DBObject updatesDb = parseMongoUpdates(updates);
    if (updates.length() > 0) {
      try {
        collection.update(query, new BasicDBObject("$inc", updatesDb));
      } catch (MongoException e) {
        throw new FailedDBOperationException(collectionName, updates.toString());
      }
    }
  }

  @Override
  public MongoRecordCursor getAllRowsIterator(String collectionName) throws FailedDBOperationException {
    return new MongoRecordCursor(db, collectionName, MongoCollectionSpec.getCollectionSpec(collectionName).getPrimaryKey());
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

  //THIS ISN'T TEST CODE
  // the -clear option is currently used by the EC2 installer so keep it working
  // this use will probably go away at some point
  public static void main(String[] args) throws Exception, RecordNotFoundException {
    if (args.length > 0 && args[0].startsWith("-clear")) {
      dropAllDatabases();
    } else if (args.length == 3) {
      queryTest(Integer.parseInt(args[0]), args[1], args[2], null);
    } else if (args.length == 4) {
      queryTest(Integer.parseInt(args[0]), args[1], args[2], args[3]);
    } else {
    }
    // important to include this!!
    System.exit(0);
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
//    init();
  }

  // ALL THE CODE BELOW IS TEST CODE
//  //test code
  private static void queryTest(int nodeID, String key, String searchArg, String otherArg) throws RecordNotFoundException, Exception {
    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig("ns1", nodeID);
    Set<Integer> nameServerIDs = new HashSet<Integer>();
    nameServerIDs.add(0);
    nameServerIDs.add(1);
    nameServerIDs.add(2);
    ConsistentHashing.initialize(3, nameServerIDs);
    MongoRecordsV2 instance = new MongoRecordsV2(nodeID, -1);
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
    AbstractRecordCursor cursor;
    cursor = instance.find(DBNAMERECORD, new JSONObject());
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
    cursor = instance.getAllRowsIterator(DBNAMERECORD, new JSONObject());
    while (cursor.hasNext()) {
      System.out.println(cursor.nextJSONObject().toString());
    }
  }

  public static String Version = "$Revision$";

}
