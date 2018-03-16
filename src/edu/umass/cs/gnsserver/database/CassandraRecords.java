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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * An unfinished Cassandra implementation of NoSQLRecords.
 *
 * @author westy
 */
public class CassandraRecords implements NoSQLRecords {

  private static final String DBROOTNAME = "GNS";

  /**
   * The name of the document where name records are stored.
   */
  public static final String DBNAMERECORD = "NameRecord";

  private String dbName;
  private Cluster cluster;
  private Session session;
  private static Map<String, CassandraRecords.CollectionSpec> collectionSpecMap = new HashMap<String, CassandraRecords.CollectionSpec>();

  /**
   * Returns the CassandraRecords.CollectionSpec for the given name.
   *
   * @param name the name
   * @return a CassandraRecords.CollectionSpec object
   */
  public CassandraRecords.CollectionSpec getCollectionSpec(String name) {
    return collectionSpecMap.get(name);
  }

  @Override
  public void createIndex(String collectionName, String field, String index) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

//  @Override
//  public HashMap<ColumnField, Object> lookupSystemFields(String collectionName, String guid, ColumnField nameField, ArrayList<ColumnField> systemFields) throws RecordNotFoundException, FailedDBOperationException {
//    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//  }
  
  /**
   * Stores the name, primary key, and index of each collection we maintain in the mongo db.
   */
  static class CollectionSpec {

    private final String name;
    private final String primaryKey;

    /**
     * Create a CollectionSpec instance.
     *
     * @param name
     * @param primaryKey
     */
    public CollectionSpec(String name, String primaryKey) {
      this.name = name;
      this.primaryKey = primaryKey;
      collectionSpecMap.put(name, this);
    }

    /**
     * Return the name of a collection.
     *
     * @return the name
     */
    public String getName() {
      return name;
    }

    /**
     * Return the name of field that is the primary key of a collection.
     *
     * @return field name of primary key
     */
    public String getPrimaryKey() {
      return primaryKey;
    }
  }
  private static List<CassandraRecords.CollectionSpec> collectionSpecs
          = Arrays.asList(
                  new CassandraRecords.CollectionSpec(DBNAMERECORD, NameRecord.NAME.getName())
          //,new CassandraRecords.CollectionSpec(DBREPLICACONTROLLER, ReplicaControllerRecord.NAME.getName())
          );

  /**
   * Create a CassandraRecords instance.
   *
   * @param nodeID
   */
  public CassandraRecords(int nodeID) {
    dbName = DBROOTNAME + nodeID;
    DatabaseConfig.getLogger().log(Level.INFO, "CASSANDRA: {0} INIT", dbName);
    this.connect("localhost");
    this.createKeyspace();
    this.createSchemas();
  }

  private void connect(String node) {
    cluster = Cluster.builder().addContactPoint(node).build();
    Metadata metadata = cluster.getMetadata();
    DatabaseConfig.getLogger().log(Level.INFO, "Connected to cluster: {0}",
            metadata.getClusterName());
    for (Host host : metadata.getAllHosts()) {
      DatabaseConfig.getLogger().log(Level.INFO, "Datacenter: {0} Host: {1} Rack: {2}",
              new Object[]{host.getDatacenter(), host.getAddress(), host.getRack()});
    }
    session = cluster.connect();
  }

  /**
   * Close the database.
   */
  public void close() {
    cluster.shutdown();
  }

  /**
   * CREATE A C_ase S_ensitive I_dentifier
   *
   * @param string
   * @return an identifier
   */
  private String CSI(String string) {
    return "\"" + string + "\"";
  }

  /**
   * Create a keyspace.
   */
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
    DatabaseConfig.getLogger().log(Level.FINER, "Executing query {0}", query);
    try {
      session.execute(query);
    } catch (AlreadyExistsException e) {
    }
  }
  
  private void insertColumn(String tableName, String keyColumn, String guid, String columnName, String value) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "ALTER TABLE " + CSI(tableName) + " ADD " + CSI(columnName) + " text;";
      DatabaseConfig.getLogger().log(Level.FINER, "Executing query {0}", query);
      try {
        session.execute(query);
      } catch (InvalidQueryException e) {
      }
      query = "INSERT INTO " + CSI(tableName) + " (" + CSI(keyColumn) + ", " + CSI(columnName) + ") "
              + "VALUES ("
              + "'" + guid + "'"
              + ",'" + value + "'"
              + ");";
      DatabaseConfig.getLogger().log(Level.FINER, "Executing query {0}", query);
      session.execute(query);

    }
  }

//  private String retrieveUserKeysString(String tableName, String guid) throws JSONException {
//    StringBuffer result = new StringBuffer();
//    String values = lookupEntireRecord(tableName, guid, NameRecord.USER_KEYS);
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
      DatabaseConfig.getLogger().finer("Executing query " + query);
      ResultSet results = session.execute(query);
      Row row = results.one();
      if (row != null) {
        JSONObject json = retrieveJSONObjectFromRow(row);
        DatabaseConfig.getLogger().finest(json.toString());
        return json;
      } else {
        return null;
      }
    } else {
      DatabaseConfig.getLogger().log(Level.SEVERE, "CASSANDRA DB: No table named: {0}", tableName);
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
        DatabaseConfig.getLogger().log(Level.FINER,
                "Name = {0} value = {1}", new Object[]{name, value});
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
      DatabaseConfig.getLogger().log(Level.WARNING,
              "Problem creating JSON object: {0}", e);
      return null;
    }
    //return json;
  }

  @Override
  public void printAllEntries(String collectionName) throws FailedDBOperationException {
    AbstractRecordCursor cursor = getAllRowsIterator(collectionName);
    while (cursor.hasNext()) {
      System.out.println(cursor.nextJSONObject());
    }
  }
  
   @Override
  public HashMap<ColumnField, Object> lookupSomeFields(String collectionName, String guid, ColumnField nameField, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws RecordNotFoundException, FailedDBOperationException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void updateEntireRecord(String collection, String name, ValuesMap valuesMap) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  @Override
  public void updateIndividualFields(String collectionName, String guid, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues) throws FailedDBOperationException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void removeMapKeys(String collectionName, String name, ColumnField mapField, ArrayList<ColumnField> mapKeys) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public AbstractRecordCursor getAllRowsIterator(String collection) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public AbstractRecordCursor selectRecords(String collectionName, ColumnField valuesMapField, String key, Object value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public AbstractRecordCursor selectRecordsWithin(String collectionName, ColumnField valuesMapField, String key, String value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public AbstractRecordCursor selectRecordsNear(String collectionName, ColumnField valuesMapField, String key, String value, Double maxDistance) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public AbstractRecordCursor selectRecordsQuery(String collectionName, ColumnField valuesMapField, 
          String query, List<String> projection) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeEntireRecord(String tableName, String guid) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "DELETE FROM " + CSI(tableName) + " WHERE " + CSI(spec.getPrimaryKey()) + " = '" + guid + "';";
      DatabaseConfig.getLogger().log(Level.FINER, "Executing query {0}", query);
      session.execute(query);
    } else {
      DatabaseConfig.getLogger().log(Level.SEVERE, "CASSANDRA DB: No table named: {0}", tableName);
    }
  }

  @Override
  public boolean contains(String tableName, String guid) {
    CollectionSpec spec = getCollectionSpec(tableName);
    if (spec != null) {
      String query = "SELECT " + CSI(spec.getPrimaryKey()) + " FROM " + CSI(tableName)
              + " WHERE " + CSI(spec.getPrimaryKey()) + " = '" + guid + "';";
      DatabaseConfig.getLogger().log(Level.FINER, "Executing query {0}", query);
      ResultSet results = session.execute(query);
      return !results.isExhausted();
    } else {
      DatabaseConfig.getLogger().log(Level.SEVERE, "CASSANDRA DB: No table named: {0}", tableName);
      return false;
    }
  }

  @Override
  public JSONObject lookupEntireRecord(String tableName, String guid) {
    return retrieveJSONObject(tableName, guid);
  }

  @Override
  @SuppressWarnings("unchecked")
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
            DatabaseConfig.getLogger().log(Level.WARNING,
                    "Problem extracting field from JSON object: {0}", e);
          }
        }
      }
    } else {
      DatabaseConfig.getLogger().log(Level.SEVERE, "CASSANDRA DB: No table named: {0}", tableName);
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
////    System.out.println("LOOKUP BY GUID =>" + instance.lookupEntireRecord(n.getName(), true));
////    instance.updateEntireRecord(n.getName(), "timeToLive", "777");
////    System.out.println("LOOKUP AFTER 777 =>" + instance.lookupEntireRecord(n.getName(), true));
////    instance.updateEntireRecord(n.getName(), "FRANK", "777");
////    System.out.println("LOOKUP AFTER FRANK =>" + instance.lookupEntireRecord(n.getName(), true));
////    
//      JSONObject json = instance.lookupEntireRecord(DBNAMERECORD, n.getName());
//      System.out.println("LOOKUP BY GUID => " + json);
//      System.out.println("CONTAINS = (should be true) " + instance.contains(DBNAMERECORD, n.getName()));
//      System.out.println("CONTAINS = (should be false) " + instance.contains(DBNAMERECORD, "BLAH BLAH"));
//      NameRecord record = new NameRecord(json);
//      record.updateKey("FRED", new ArrayList<String>(Arrays.asList("BARNEY")), null, UpdateOperation.REPLACE_ALL);
//      System.out.println("JSON AFTER UPDATE => " + record.toJSONObject());
//      instance.updateEntireRecord(DBNAMERECORD, record.getName(), record.toJSONObject());
//      JSONObject json2 = instance.lookupEntireRecord(DBNAMERECORD, n.getName());
//      System.out.println("2ND LOOKUP BY GUID => " + json2);
//      //
//      //System.out.println("LOOKUP BY KEY =>" + instance.lookupEntireRecord(n.getName(), n.getRecordKey().getName(), true));
//
////    System.out.println("DUMP =v");
////    instance.printAllEntries();
////    System.out.println(MongoRecordsV2.getInstance().db.getCollection(COLLECTIONNAME).getStats().toString());
//      //
//      instance.updateField(DBNAMERECORD, n.getName(), NameRecord.ACTIVE_NAMESERVERS.getFieldName(), Arrays.asList(97, 98, 99));
//      json2 = instance.lookupEntireRecord(DBNAMERECORD, n.getName());
//      System.out.println("JSON AFTER FIELD UPDATE => " + json2);
//      instance.removeEntireRecord(DBNAMERECORD, n.getName());
//      json2 = instance.lookupEntireRecord(DBNAMERECORD, n.getName());
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
