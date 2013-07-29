/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnrs.database;

import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.nameserver.NameRecordV1;
import edu.umass.cs.gnrs.nameserver.NameServer;
import edu.umass.cs.gnrs.util.ThreadUtils;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

/**
 * Provides insert, update, remove and lookup operations for guid, key, record triples using JSONObjects as the intermediate
 * representation
 *
 * @author westy
 */
public class CassandraRecords implements NoSQLRecords {

  private static final String DBROOTNAME = "gnrs";
    private static final String[] COLLECTIONS = MongoRecords.COLLECTIONS;

  private static final StringSerializer SS = StringSerializer.get();
  String dbName;
  private Cluster cluster;
  private Keyspace keyspace;
  //SuperCfTemplate<String, String, String> superTemplate;
  private ColumnFamilyTemplate<String, String> template;

  public static CassandraRecords getInstance() {
    return CassandraRecordCollectionHolder.INSTANCE;
  }

  private static class CassandraRecordCollectionHolder {

    private static final CassandraRecords INSTANCE = new CassandraRecords();
  }
  private static final String GUIDCOLUMNFAMILY = "GUID";

  private CassandraRecords() {
    dbName = DBROOTNAME + NameServer.nodeID;
    System.out.println("CASSANDRA: " + dbName + " INIT");
    cluster = HFactory.getOrCreateCluster("GNRScluster", "localhost:9160");
    //cluster.dropKeyspace(dbName, true);
    initKeySpace();
  }

  private void initKeySpace() {
    KeyspaceDefinition keyspaceDef = null;
    boolean retry = false;
    do {
      try {
        keyspaceDef = cluster.describeKeyspace(dbName);
        retry = false;
      } catch (HectorException e) {
        // probably conflicted with another request - who knows - this shit aint documented
        retry = true;
        System.out.println("CASSANDRA: " + dbName + " Exception - " + e);
        // wait a bit and try again
        ThreadUtils.sleep(500);
      }
    } while (retry == true);
    // If keyspace does not exist, the CFs don't exist either. => create them.
    if (keyspaceDef == null) {
      System.out.println("CASSANDRA: " + dbName + " Creating Schema");
      createSchema();
    }
    keyspace = HFactory.createKeyspace(dbName, cluster);


    template = new ThriftColumnFamilyTemplate<String, String>(keyspace, GUIDCOLUMNFAMILY, SS, SS);

    System.out.println("CASSANDRA: " + dbName + " INIT DONE");
    //superTemplate = new ThriftSuperCfTemplate<String, String, String>(keyspace, "Super1", se, se, se);
  }

  public void createSchema() {
    ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(dbName,
            GUIDCOLUMNFAMILY,
            ComparatorType.BYTESTYPE);

//    ColumnFamilyDefinition scDef = HFactory.createColumnFamilyDefinition(dbName,
//            "Super1",
//            ComparatorType.BYTESTYPE);
//    

    KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(dbName, ThriftKsDef.DEF_STRATEGY_CLASS,
            1, Arrays.asList(cfDef));
    // Add the schema to the cluster.
    // "true" as the second param means that Hector will block until all nodes see the change.
    cluster.addKeyspace(newKeyspace, true);
  }

  @Override
  public boolean contains(String collection, String guid) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(String collection, String guid, JSONObject value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void insert(String collection, String guid, JSONObject value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public JSONObject lookup(String collection, String guid) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String lookup(String collection, String guid, String key) {
    ColumnFamilyResult<String, String> CFresult = template.queryColumns(guid);
    String value = CFresult.getString(key);
    return value;
  }

  public void insert(String guid, String key, String value) {
    ColumnFamilyUpdater<String, String> updater = template.createUpdater(guid);
    updater.setString(key, value.toString());
    try {
      template.update(updater);
    } catch (HectorException e) {
      GNS.getLogger().warning("Unable to insert: " + e);
    }
  }
  
  public void update(String collection, String name, String key, String value) {
    update(collection, name, key, new ArrayList(Arrays.asList(value)));
  }

  @Override
  public void update(String collection, String guid, String key, ArrayList<String> value) {
    ColumnFamilyUpdater<String, String> updater = template.createUpdater(guid);
    updater.setString(key, new JSONArray(value).toString());
    try {
      template.update(updater);
    } catch (HectorException e) {
      GNS.getLogger().warning("Unable to update: " + e);
    }
  }

  public boolean contains(String collection, String guid, String key) {
    ColumnFamilyResult<String, String> CFresult = template.queryColumns(guid);
    String value = CFresult.getString(key);
    return value != null;
  }

  public void remove(String collection, String guid, String key) {
    template.deleteColumn(guid, key);
  }

  @Override
  public void remove(String collection, String guid) {
    template.deleteRow(guid);
  }

  @Override
  public ArrayList<JSONObject> retrieveAllEntries(String collection) {
    ArrayList<JSONObject> result = new ArrayList<JSONObject>();
    // This will page through the column family in pages of 100 rows. 
    // It will only fetch 10 columns for each row (you will want to page very long rows too).
    int row_count = 100;

    RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
            .createRangeSlicesQuery(keyspace, SS, SS, SS)
            .setColumnFamily(GUIDCOLUMNFAMILY)
            .setRange(null, null, false, 10)
            .setRowCount(row_count);

    String last_key = null;

    try {
      while (true) {
        rangeSlicesQuery.setKeys(last_key, null);
        //System.out.println(" > " + last_key);

        QueryResult<OrderedRows<String, String, String>> queryResult = rangeSlicesQuery.execute();
        OrderedRows<String, String, String> rows = queryResult.get();
        Iterator<Row<String, String, String>> rowsIterator = rows.iterator();

        // we'll skip this first one, since it is the same as the last one from previous time we executed
        if (last_key != null && rowsIterator != null) {
          rowsIterator.next();
        }

        while (rowsIterator.hasNext()) {
          Row<String, String, String> row = rowsIterator.next();
          last_key = row.getKey();

          if (row.getColumnSlice().getColumns().isEmpty()) {
            continue;
          }
          for (HColumn<String, String> column : row.getColumnSlice().getColumns()) {
            result.add(new JSONObject(column.getValue().toString()));
          }
        }
        if (rows.getCount() < row_count) {
          break;
        }
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("Unable to parse JSON: " + e);
      return null;
    }
    return result;
  }

  @Override
  public Set<String> keySet(String collection) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

    @Override
    public void printAllEntries() {
        for (String collection: COLLECTIONS)
        for (JSONObject entry : retrieveAllEntries(collection)) {
            System.out.println(entry.toString());
        }
    }

    public void resetKeySpace() {
    cluster.dropKeyspace(dbName);
    initKeySpace();
  }

  public void deleteAllRows() {
    // make sure there isn't a better way to do this
    System.out.println("****!!!!! DELETING ALL ROWS !!!!!****");
    ArrayList<JSONObject> result = new ArrayList<JSONObject>();
    // This will page through the column family in pages of 100 rows. 
    // It will only fetch 10 columns for each row (you will want to page very long rows too).
    int row_count = 100;

    RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
            .createRangeSlicesQuery(keyspace, SS, SS, SS)
            .setColumnFamily(GUIDCOLUMNFAMILY)
            .setRange(null, null, false, 10)
            .setRowCount(row_count);

    String last_key = null;

    while (true) {
      rangeSlicesQuery.setKeys(last_key, null);
      //System.out.println(" > " + last_key);

      QueryResult<OrderedRows<String, String, String>> queryResult = rangeSlicesQuery.execute();
      OrderedRows<String, String, String> rows = queryResult.get();
      Iterator<Row<String, String, String>> rowsIterator = rows.iterator();

      // we'll skip this first one, since it is the same as the last one from previous time we executed
      if (last_key != null && rowsIterator != null) {
        rowsIterator.next();
      }

      while (rowsIterator.hasNext()) {
        Row<String, String, String> row = rowsIterator.next();
        template.deleteRow(row.getKey());
      }
      if (rows.getCount() < row_count) {
        break;
      }
    }
  }

//  public void printAllEntries() {
//
//  }

  @Override
  public String toString() {
    return "CassandraRecords{" + "name = " + dbName + '}';
  }
  // test code

  public static void main(String[] args) throws Exception {
      String collection = CassandraRecords.COLLECTIONS[0];
    NameRecordV1 n = NameRecordV1.testCreateNameRecord();
    CassandraRecords.getInstance().remove(collection, n.getName());
    CassandraRecords.getInstance().insert(n.getName(), n.getRecordKey().getName(), n.toJSONObject().toString());
    System.out.println("LOOKUP =>" + CassandraRecords.getInstance().lookup(n.getName(), n.getRecordKey().getName()));
    System.out.println("BAD LOOKUP =>" + CassandraRecords.getInstance().lookup(n.getName(), "fred"));
    System.out.println("BAD GUID LOOKUP =>" + CassandraRecords.getInstance().lookup("fred", "fred"));
    System.out.println("CONTAINS =>" + CassandraRecords.getInstance().contains(n.getName(), n.getRecordKey().getName()));
    System.out.println("BAD CONTAINS =>" + CassandraRecords.getInstance().contains(n.getName(), "fred"));
    System.out.println("DUMP vvvvv");
    CassandraRecords.getInstance().printAllEntries();
  }
}
