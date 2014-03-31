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
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.recordmap.NameRecord;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.nameserver.recordmap.ReplicaControllerRecord;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;
/**************** FIXME All functionality of this package is provided currently by class nsdesign/recordMap/MongoRecords.java.
 * FIXME Make changes to that file until we include this package again.. **/
/**
 *
 * @author westy
 */
public class CassandraRecords implements NoSQLRecords {

  private static final String DBROOTNAME = "GNS";
  public static final String DBNAMERECORD = "NameRecord";
  public static final String DBREPLICACONTROLLER = "ReplicaControllerRecord";
  private String dbName;
  private Cluster cluster;
  private Session session;
  private static Map<String, CassandraRecords.CollectionSpec> collectionSpecMap = new HashMap<String, CassandraRecords.CollectionSpec>();

  public static CassandraRecords getInstance() {
    return CassandraRecordCollectionHolder.INSTANCE;
  }
  
  private static class CassandraRecordCollectionHolder {

    private static final CassandraRecords INSTANCE = new CassandraRecords();
  }

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
          new CassandraRecords.CollectionSpec(DBNAMERECORD, NameRecord.NAME.getName()),
          new CassandraRecords.CollectionSpec(DBREPLICACONTROLLER, ReplicaControllerRecord.NAME.getName()));

  public CassandraRecords() {
    dbName = DBROOTNAME + NameServer.getNodeID();
    GNS.getLogger().info("CASSANDRA: " + dbName + " INIT");
    this.connect("localhost");
    this.createKeyspace();
    this.createSchemas();
  }

  private void connect(String node) {
    cluster = Cluster.builder().addContactPoint(node).build();
    Metadata metadata = cluster.getMetadata();
    GNS.getLogger().info("Connected to cluster: " + metadata.getClusterName());
    for (Host host : metadata.getAllHosts()) {
      GNS.getLogger().info("Datacenter: " + host.getDatacenter() + " Host: " + host.getAddress() + " Rack: " + host.getRack());
    }
    session = cluster.connect();
  }

  public void close() {
    cluster.shutdown();
  }

  /**
   * CREATE A C_ase S_ensitive I_dentifier
   *
   * @param string
   * @return
   */
  private String CSI(String string) {
    return "\"" + string + "\"";
  }

  public void createKeyspace() {
    try {
      session.execute("CREATE KEYSPACE " + CSI(dbName) + " WITH replication "
              + "= {'class':'SimpleStrategy', 'replication_factor':1};");
    } catch (AlreadyExistsException e) {
    }

    session.execute("USE " + CSI(dbName));
  }

  private void createSchemas() {

    for (CassandraRecords.CollectionSpec spec : collectionSpecs) {
      createSchema(spec.getName(), spec.getPrimaryKey());
    }
  }

  private void createSchema(String tableName, String key) {
//    String query = "DROP TABLE " + tableName + ";";
//    GNS.getLogger().finer("Executing query " + query);
//    try {
//      session.execute(query);
//    } catch (InvalidQueryException e) {
//    }
    String query = "CREATE TABLE " + CSI(tableName) + " ("
            + CSI(key) + " text"
            + ",PRIMARY KEY (" + CSI(key) + ")"
            + ");";
    GNS.getLogger().finer("Executing query " + query);
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
      String query = "ALTER TABLE " + CSI(tableName) + " ADD " + CSI(columnName) + " text;";
      GNS.getLogger().finer("Executing query " + query);
      try {
        session.execute(query);
      } catch (InvalidQueryException e) {
      }
      query = "INSERT INTO " + CSI(tableName) + " (" + CSI(keyColumn) + ", " + CSI(columnName) + ") "
              + "VALUES ("
              + "'" + guid + "'"
              + ",'" + value + "'"
              + ");";
      GNS.getLogger().finer("Executing query " + query);
      session.execute(query);

    }
  }

//  private String retrieveUserKeysString(String tableName, String guid) throws JSONException {
//    StringBuffer result = new StringBuffer();
//    String values = lookup(tableName, guid, NameRecord.USER_KEYS);
//    String prefix = "";
//    for (String key : JSONUtils.JSONArrayToArrayList(new JSONArray(values))) {
//      result.append(prefix);
//      result.append(CSI(key));
//      prefix = ",";
//    }
//    return result.toString();
//  }
  private JSONObject retrieveJSONObject(String tableName, String guid) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "SELECT * FROM " + CSI(tableName) + " WHERE " + CSI(spec.getPrimaryKey()) + " = '" + guid + "';";
      GNS.getLogger().finer("Executing query " + query);
      ResultSet results = session.execute(query);
      Row row = results.one();
      if (row != null) {
        JSONObject json = retrieveJSONObjectFromRow(row);
        GNS.getLogger().finest(json.toString());
        return json;
      } else {
        return null;
      }
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
      return null;
    }
  }

  private JSONObject retrieveJSONObjectFromRow(Row row) {
    //JSONObject json = new JSONObject();
    StringBuilder result = new StringBuilder();
    result.append("{");
    String prefix = "";
    for (Definition def : row.getColumnDefinitions().asList()) {
      String name = def.getName();
      String value = row.getString(name);
      if (value != null) {
        // Building the JSON string here
        GNS.getLogger().finer("Name = " + name + " value = " + value);
        result.append(prefix);
        result.append("\"");
        result.append(name);
        result.append("\"");
        result.append(":");
        // Only quote them if they aren't a JSON array or object
        if (!value.startsWith("[") && !value.startsWith("{")) {
          result.append("\"");
        }
        result.append(value);
        // Only quote them if they aren't a JSON array or object
        if (!value.startsWith("[") && !value.startsWith("{")) {
          result.append("\"");
        }
        prefix = ",";
      }
    }
    result.append("}");
    //System.out.println(result);
    try {
      return new JSONObject(result.toString());
    } catch (JSONException e) {
      GNS.getLogger().warning("Problem creating JSON object: " + e);
      return null;
    }
    //return json;
  }

  @Override
  public void reset(String tableName) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "TRUNCATE " + CSI(tableName) + ";";
      GNS.getLogger().finer("Executing query " + query);
      session.execute(query);
      GNS.getLogger().info("CASSANDRA DB RESET. DBNAME: " + dbName + " Table name: " + tableName);
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
    }
  }

  @Override
  public void printAllEntries(String collectionName) {
    BasicRecordCursor cursor = getAllRowsIterator(collectionName);
    while (cursor.hasNext()) {
      System.out.println(cursor.nextJSONObject());
    }
  }

  @Override
  public HashMap<ColumnField, Object> lookup(String collection, String name, ColumnField nameField, ArrayList<ColumnField> fields1) throws RecordNotFoundException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public HashMap<ColumnField, Object> lookup(String collection, String name, ColumnField nameField, ArrayList<ColumnField> fields1, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws RecordNotFoundException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(String collection, String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(String collection, String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void updateConditional(String collectionName, String guid, ColumnField nameField, ColumnField conditionField, Object conditionValue, ArrayList<ColumnField> fields, ArrayList<Object> values, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void increment(String collection, String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void increment(String collectionName, String guid, ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField votesMapField, ArrayList<ColumnField> votesMapKeys, ArrayList<Object> votesMapValues) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeMapKeys(String collectionName, String name, ColumnField mapField, ArrayList<ColumnField> mapKeys) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public BasicRecordCursor getAllRowsIterator(String collection, ColumnField nameField, ArrayList<ColumnField> fields) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public BasicRecordCursor getAllRowsIterator(String collection) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
 
  @Override
  public BasicRecordCursor selectRecords(String collectionName, ColumnField valuesMapField, String key, Object value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  @Override
  public BasicRecordCursor selectRecordsWithin(String collectionName, ColumnField valuesMapField, String key, String value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public BasicRecordCursor selectRecordsNear(String collectionName, ColumnField valuesMapField, String key, String value, Double maxDistance) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  @Override
  public BasicRecordCursor selectRecordsQuery(String collectionName, ColumnField valuesMapField, String query) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Set<String> keySet(String tableName) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void remove(String tableName, String guid) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "DELETE FROM " + CSI(tableName) + " WHERE " + CSI(spec.getPrimaryKey()) + " = '" + guid + "';";
      GNS.getLogger().finer("Executing query " + query);
      ResultSet results = session.execute(query);
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
    }
  }

  @Override
  public boolean contains(String tableName, String guid) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "SELECT " + CSI(spec.getPrimaryKey()) + " FROM " + CSI(tableName)
              + " WHERE " + CSI(spec.getPrimaryKey()) + " = '" + guid + "';";
      GNS.getLogger().finer("Executing query " + query);
      ResultSet results = session.execute(query);
      return !results.isExhausted();
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
            GNS.getLogger().warning("Problem extracting field from JSON object: " + e);
          }
        }
      }
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
    }
  }

  @Override
  public void bulkInsert(String collection, ArrayList<JSONObject> values) throws RecordExistsException {
        // todo
  }

  @Override
  public String lookup(String tableName, String guid, String key) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "SELECT " + CSI(key) + " FROM " + CSI(tableName) + " WHERE " + CSI(spec.getPrimaryKey()) + " = '" + guid + "';";
      GNS.getLogger().finer("Executing query " + query);
      ResultSet results;
      try {
        results = session.execute(query);
      } catch (InvalidQueryException e) {
        // this will happen if the column does not exist
        return null;
      }
      if (!results.isExhausted()) {
        Row row = results.one();
        return row.getString(key);
      } else {
        // this will happen if the row does not exist
        return null;
      }
    } else {
      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
      // this will happen if the table does not exist
      return null;
    }
  }

  @Override
  public ResultValue lookup(String collection, String guid, ArrayList<String> key) {
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

//  //
//  // TEST CODE
//  // 
//  private void loadTestData(String tableName, String guid) {
//    CollectionSpec spec = getCollectionSpec(tableName);
//    if (spec != null) {
//      insertColumn(tableName, guid, "KEY_1", "VALUE_1");
//      insertColumn(tableName, guid, "KEY_2", "VALUE_2");
//    } else {
//      GNS.getLogger().severe("CASSANDRA DB: No table named: " + tableName);
//    }
//  }
//
//  //test code
//  private static NameRecord createNameRecord(String name, String key, String value) throws Exception {
//    ValuesMap valuesMap = new ValuesMap();
//    valuesMap.put(key,new ArrayList(Arrays.asList(value)));
//    HashSet<Integer> x = new HashSet<Integer>();
//    x.add(0);
//    x.add(1);
//    x.add(2);
//    return new NameRecord(name, x, name+"-2",valuesMap);
//  }
//  //
//  // UTILS
//  //
//  static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
//  static Random rnd = new Random(System.currentTimeMillis());
//
//  private static String randomString(int len) {
//    StringBuilder sb = new StringBuilder(len);
//    for (int i = 0; i < len; i++) {
//      sb.append(CHARACTERS.charAt(rnd.nextInt(CHARACTERS.length())));
//    }
//    return sb.toString();
//  }
//
//  public static void runTest() {
//    try {
//      ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
//      ConsistentHashing.initializeHashFunction();
//      CassandraRecords instance = CassandraRecords.getInstance();
//      //instance.reset(DBNAMERECORD);
//      Random random = new Random();
//      NameRecord n = null;
//      for (int i = 0; i < 100; i++) {
//        NameRecord x = createNameRecord("GUID_" + randomString(8), "KEY_" + randomString(8), "VALUE_" + randomString(3));
//        if (i == 50) {
//          n = x;
//        }
//        System.out.println(x.toString());
//        instance.insert(DBNAMERECORD, x.getName(), x.toJSONObject());
//      }
//
////    System.out.println("LOOKUP BY GUID =>" + instance.lookup(n.getName(), true));
////    instance.update(n.getName(), "timeToLive", "777");
////    System.out.println("LOOKUP AFTER 777 =>" + instance.lookup(n.getName(), true));
////    instance.update(n.getName(), "FRANK", "777");
////    System.out.println("LOOKUP AFTER FRANK =>" + instance.lookup(n.getName(), true));
////    
//      JSONObject json = instance.lookup(DBNAMERECORD, n.getName());
//      System.out.println("LOOKUP BY GUID => " + json);
//      System.out.println("CONTAINS = (should be true) " + instance.contains(DBNAMERECORD, n.getName()));
//      System.out.println("CONTAINS = (should be false) " + instance.contains(DBNAMERECORD, "BLAH BLAH"));
//      NameRecord record = new NameRecord(json);
//      record.updateKey("FRED", new ArrayList<String>(Arrays.asList("BARNEY")), null, UpdateOperation.REPLACE_ALL);
//      System.out.println("JSON AFTER UPDATE => " + record.toJSONObject());
//      instance.update(DBNAMERECORD, record.getName(), record.toJSONObject());
//      JSONObject json2 = instance.lookup(DBNAMERECORD, n.getName());
//      System.out.println("2ND LOOKUP BY GUID => " + json2);
//      //
//      //System.out.println("LOOKUP BY KEY =>" + instance.lookup(n.getName(), n.getRecordKey().getName(), true));
//
////    System.out.println("DUMP =v");
////    instance.printAllEntries();
////    System.out.println(MongoRecordsV2.getInstance().db.getCollection(COLLECTIONNAME).getStats().toString());
//      //
//      instance.updateField(DBNAMERECORD, n.getName(), NameRecord.ACTIVE_NAMESERVERS.getFieldName(), Arrays.asList(97, 98, 99));
//      json2 = instance.lookup(DBNAMERECORD, n.getName());
//      System.out.println("JSON AFTER FIELD UPDATE => " + json2);
//      instance.remove(DBNAMERECORD, n.getName());
//      json2 = instance.lookup(DBNAMERECORD, n.getName());
//      System.out.println("SHOULD BE EMPTY => " + json2);
//      System.exit(0);
//    } catch (FieldNotFoundException e) {
//      System.out.println(" FieldNotFoundException. Field = " + e.getMessage());
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    } catch (Exception e) {
//      System.out.println("Error during main test execution: " + e);
//      e.printStackTrace();
//    }
//  }
//
//  private static void runBasicTest() {
//    try {
//      CassandraRecords client = CassandraRecords.getInstance();
//      client.loadTestData(DBREPLICACONTROLLER, "GUID_XYZ");
//      System.out.println(client.retrieveJSONObject(DBREPLICACONTROLLER, "GUID_XYZ"));
//      //client.queryTestSchema(DBREPLICACONTROLLER, ReplicaControllerRecord.NAME);
//      //client.close();
//      System.exit(0);
//    } catch (Exception e) {
//      System.out.println("Error during main test execution: " + e);
//      e.printStackTrace();
//    }
//  }
//
//  public static void main(String[] args) throws Exception {
//    runTest();
//  }
}
