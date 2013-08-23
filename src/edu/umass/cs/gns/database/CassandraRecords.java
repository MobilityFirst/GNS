/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.packet.UpdateOperation;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

/**
 *
 * @author westy
 */
public class CassandraRecords implements NoSQLRecords {

  private static final String DBROOTNAME = "CGNS";
  public static final String DBNAMERECORD = "CNameRecord";
  public static final String DBREPLICACONTROLLER = "CReplicaControllerRecord";
  private String dbName;
  private Cluster cluster;
  private Session session;
  private static Map<String, CassandraRecords.CollectionSpec> collectionSpecMap = new HashMap<String, CassandraRecords.CollectionSpec>();

  public CassandraRecords.CollectionSpec getCollectionSpec(String name) {
    return collectionSpecMap.get(name);
  }

  /**
   * Stores the name, primary key, and index of each collection we maintain in the mongo db.
   */
  static class CollectionSpec {

    private String name;
    private String primaryKey;

    public CollectionSpec(String name, String primaryKey) {
      this.name = name;
      this.primaryKey = primaryKey;
      collectionSpecMap.put(name, this);
    }

    public String getName() {
      return name;
    }

    public String getPrimaryKey() {
      return primaryKey;
    }
  }
  private static List<CassandraRecords.CollectionSpec> collectionSpecs =
          Arrays.asList(
          new CassandraRecords.CollectionSpec(DBNAMERECORD, NameRecord.NAME),
          new CassandraRecords.CollectionSpec(DBREPLICACONTROLLER, ReplicaControllerRecord.NAME));

  public static CassandraRecords getInstance() {
    return CassandraRecordCollectionHolder.INSTANCE;
  }

  private static class CassandraRecordCollectionHolder {

    private static final CassandraRecords INSTANCE = new CassandraRecords();
  }

  public CassandraRecords() {
    dbName = DBROOTNAME + NameServer.nodeID;
    System.out.println("CASSANDRA: " + dbName + " INIT");
    this.connect("localhost");
    this.createKeyspace();
    this.createSchemas();
  }

  private void connect(String node) {
    cluster = Cluster.builder().addContactPoint(node).build();
    Metadata metadata = cluster.getMetadata();
    System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
    for (Host host : metadata.getAllHosts()) {
      System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
    }
    session = cluster.connect();
  }

  public void close() {
    cluster.shutdown();
  }

  public void createKeyspace() {
    try {
      session.execute("CREATE KEYSPACE " + dbName + " WITH replication "
              + "= {'class':'SimpleStrategy', 'replication_factor':1};");
    } catch (AlreadyExistsException e) {
    }

    session.execute("USE " + dbName);
  }

  private void createSchemas() {

    for (CassandraRecords.CollectionSpec spec : collectionSpecs) {
      createSchema(spec.getName(), spec.getPrimaryKey());
    }
  }

  private void createSchema(String tableName, String key) {
//    String query = "DROP TABLE " + tableName + ";";
//    System.out.println("Executing query " + query);
//    try {
//      session.execute(query);
//    } catch (InvalidQueryException e) {
//    }
    String query = "CREATE TABLE " + tableName + " ("
            + key + " text"
            + ",PRIMARY KEY (" + key + ")"
            + ");";
    System.out.println("Executing query " + query);
    try {
      session.execute(query);
    } catch (AlreadyExistsException e) {
    }
  }

  private void insertColumn(String tableName, String guid, String columnName, String value) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      insertColumn(tableName, spec.getPrimaryKey(), guid, columnName, value);
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
    }
  }

  private void insertColumn(String tableName, String keyColumn, String guid, String columnName, String value) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "ALTER TABLE " + tableName + " ADD " + columnName + " text;";
      System.out.println("Executing query " + query);
      try {
        session.execute(query);
      } catch (InvalidQueryException e) {
      }
      query = "INSERT INTO " + tableName + " (" + keyColumn + ", " + columnName + ") "
              + "VALUES ("
              + "'" + guid + "'"
              + ",'" + value + "'"
              + ");";
      System.out.println("Executing query " + query);
      session.execute(query);

    }
  }

  private JSONObject retrieveJSONObject(String tableName, String guid) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "SELECT * FROM " + tableName + " WHERE " + spec.getPrimaryKey() + " = '" + guid + "';";
      System.out.println("Executing query " + query);
      ResultSet results = session.execute(query);
      JSONObject json = new JSONObject();
      for (Row row : results) {
        json = retrieveJSONObjectFromRow(row);
        System.out.println(json.toString());
      }
      return json;
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
      return null;
    }
  }

  private JSONObject retrieveJSONObjectFromRow(Row row) {
    JSONObject json = new JSONObject();
    for (Definition def : row.getColumnDefinitions().asList()) {
      String name = def.getName();
      try {
        // NEW WAY TO DO THIS
        json.put(name, row.getString(name));
      } catch (JSONException e) {
        GNS.getLogger().warning("Problem creating JSON object: " + e);
      }
    }
    return json;
  }

  @Override
  public void reset(String tableName) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "DROP TABLE " + tableName + ";";
      createSchema(tableName, spec.getPrimaryKey());
      GNS.getLogger().info("CASSANDRA DB RESET. DBNAME: " + dbName + " Table name: " + tableName);
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
    }
  }

  @Override
  public void printAllEntries(String tableName) {
    for (JSONObject entry : retrieveAllEntries(tableName)) {
      System.out.println(entry.toString());
    }
  }

  @Override
  public ArrayList<JSONObject> retrieveAllEntries(String tableName) {
    ArrayList<JSONObject> result = new ArrayList<JSONObject>();
    String query = "SELECT * FROM " + tableName + ";";
    System.out.println("Executing query " + query);
    ResultSet results = session.execute(query);
    JSONObject json = new JSONObject();
    for (Row row : results) {
      result.add(retrieveJSONObjectFromRow(row));
    }
    return result;
  }

  @Override
  public Set<String> keySet(String tableName) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void remove(String tableName, String guid) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "DELETE FROM " + tableName + " WHERE " + spec.getPrimaryKey() + " = '" + guid + "';";
      System.out.println("Executing query " + query);
      ResultSet results = session.execute(query);
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
    }
  }

  @Override
  public boolean contains(String tableName, String guid) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "SELECT " + spec.getPrimaryKey() + " FROM " + tableName
              + " WHERE " + spec.getPrimaryKey() + " = '" + guid + "';";
      System.out.println("Executing query " + query);
      ResultSet results = session.execute(query);
      return results.isExhausted();
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
      return false;
    }
  }

  @Override
  public JSONObject lookup(String tableName, String guid) {
    return retrieveJSONObject(tableName, guid);
  }

  @Override
  public void insert(String tableName, String guid, JSONObject json) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String primaryKey = spec.getPrimaryKey();
      Iterator<String> keys = json.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        if (!key.equals(primaryKey)) {
          try {
            String value = json.getString(key);
            insertColumn(tableName, primaryKey, guid, key, value);
          } catch (JSONException e) {
            GNS.getLogger().warning("Problem extracting feild from JSON object: " + e);
          }
        }
      }
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
    }
  }

  @Override
  public String lookup(String tableName, String guid, String key) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "SELECT " + key + " FROM " + tableName + " WHERE " + spec.getPrimaryKey() + " = '" + guid + "';";
      System.out.println("Executing query " + query);
      ResultSet results = session.execute(query);
      Row row = results.one();
      return row.getString(key);
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
      return null;
    }
  }

  @Override
  public ArrayList<String> lookup(String collection, String guid, ArrayList<String> key) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void update(String tableName, String guid, JSONObject value) {
    insert(tableName, guid, value);
  }

  @Override
  public void updateField(String tableName, String guid, String key, Object object) {
    JSONObject json = new JSONObject();
    try {
      json.put(key, object);
      insert(tableName, guid, json);
    } catch (JSONException e) {
      GNS.getLogger().warning("Problem creating JSON object: " + e);
    }
  }

  //
  // TEST CODE
  // 
  private void loadTestData(String tableName, String guid) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      insertColumn(tableName, guid, "value", "SAM");
      insertColumn(tableName, guid, "spank", "UMM");
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
    }
  }

  //test code
  private static NameRecord createNameRecord(String name, String key, String value) throws Exception {
    return new NameRecord(name, new NameRecordKey(key), new ArrayList(Arrays.asList(value)));
  }
  
  //
  // UTILS
  //
  static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static Random rnd = new Random(System.currentTimeMillis());

  private static String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(CHARACTERS.charAt(rnd.nextInt(CHARACTERS.length())));
    }
    return sb.toString();
  }

  public static void runTest() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    CassandraRecords instance = CassandraRecords.getInstance();
    instance.reset(DBNAMERECORD);
    Random random = new Random();
    NameRecord n = null;
    for (int i = 0; i < 10; i++) {
      NameRecord x = createNameRecord(randomString(6), randomString(6), randomString(6));
      if (i == 5) {
        n = x;
      }
      instance.insert(DBNAMERECORD, x.getName(), x.toJSONObject());
    }

//    System.out.println("LOOKUP BY GUID =>" + instance.lookup(n.getName(), true));
//    instance.update(n.getName(), "timeToLive", "777");
//    System.out.println("LOOKUP AFTER 777 =>" + instance.lookup(n.getName(), true));
//    instance.update(n.getName(), "FRANK", "777");
//    System.out.println("LOOKUP AFTER FRANK =>" + instance.lookup(n.getName(), true));
//    
    JSONObject json = instance.lookup(DBNAMERECORD, n.getName());
    System.out.println("LOOKUP BY GUID => " + json);
    NameRecord record = new NameRecord(json);
    record.updateField("FRED", new ArrayList<String>(Arrays.asList("BARNEY")), null, UpdateOperation.REPLACE_ALL);
    System.out.println("JSON AFTER UPDATE => " + record.toJSONObject());
    instance.update(DBNAMERECORD, record.getName(), record.toJSONObject());
    JSONObject json2 = instance.lookup(DBNAMERECORD, n.getName());
    System.out.println("2ND LOOKUP BY GUID => " + json2);
    //
    //System.out.println("LOOKUP BY KEY =>" + instance.lookup(n.getName(), n.getRecordKey().getName(), true));

//    System.out.println("DUMP =v");
//    instance.printAllEntries();
//    System.out.println(MongoRecordsV2.getInstance().db.getCollection(COLLECTIONNAME).getStats().toString());
    //
    instance.remove(DBNAMERECORD, n.getName());
    json2 = instance.lookup(DBNAMERECORD, n.getName());
    System.out.println("SHOULD BE EMPTY => " + json2);
  }

  private static void runBasicTest() {
    try {
      CassandraRecords client = CassandraRecords.getInstance();
      client.loadTestData(DBREPLICACONTROLLER, "FRED");
      System.out.println(client.retrieveJSONObject(DBREPLICACONTROLLER, "FRED"));
      //client.queryTestSchema(DBREPLICACONTROLLER, ReplicaControllerRecord.NAME);
      //client.close();
      System.exit(0);
    } catch (Exception e) {
      System.out.println("Error during main test execution: " + e);
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    runTest();
  }
}
