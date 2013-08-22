/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author westy
 */
public class CassandraRecordsNew {

  private static final String DBROOTNAME = "GNS";
  public static final String DBNAMERECORD = "NameRecord";
  public static final String DBREPLICACONTROLLER = "ReplicaControllerRecord";
  private String dbName;
  private Cluster cluster;
  private Session session;
  private static Map<String, CassandraRecordsNew.CollectionSpec> collectionSpecMap = new HashMap<String, CassandraRecordsNew.CollectionSpec>();

  public CassandraRecordsNew.CollectionSpec getCollectionSpec(String name) {
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
  private static List<CassandraRecordsNew.CollectionSpec> collectionSpecs =
          Arrays.asList(
          new CassandraRecordsNew.CollectionSpec(DBNAMERECORD, NameRecord.NAME),
          new CassandraRecordsNew.CollectionSpec(DBREPLICACONTROLLER, ReplicaControllerRecord.NAME));

  public static CassandraRecordsNew getInstance() {
    return CassandraRecordCollectionHolder.INSTANCE;
  }

  private static class CassandraRecordCollectionHolder {

    private static final CassandraRecordsNew INSTANCE = new CassandraRecordsNew();
  }

  public CassandraRecordsNew() {
    dbName = DBROOTNAME + NameServer.nodeID;
    System.out.println("CASSANDRA: " + dbName + " INIT");
    this.connect("localhost");
    this.createSchemas();
  }

  public void connect(String node) {
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

  public void createSchemas() {
    try {
      session.execute("CREATE KEYSPACE " + dbName + " WITH replication "
              + "= {'class':'SimpleStrategy', 'replication_factor':1};");
    } catch (AlreadyExistsException e) {
    }

    for (CassandraRecordsNew.CollectionSpec spec : collectionSpecs) {
      try {
        String query = "DROP TABLE " + dbName + "." + spec.getName() + ";";
        System.out.println("Executing query " + query);
        session.execute(query);
        query = "CREATE TABLE " + dbName + "." + spec.getName() + " ("
                + "name text PRIMARY KEY"
                //+ ", value text"
                + ");";
        System.out.println("Executing query " + query);
        session.execute(query);
      } catch (AlreadyExistsException e) {
      }
    }
  }

  private void initializeIndexes() {
    for (CassandraRecordsNew.CollectionSpec spec : collectionSpecs) {
      initializeIndex(spec.name);
    }
  }

  private void initializeIndex(String collectionName) {
    CassandraRecordsNew.CollectionSpec spec = getCollectionSpec(collectionName);
    session.execute("CREATE INDEX ON " + spec.getName() + " (" + spec.getPrimaryKey() + ");");
  }

  public void loadTestData() {
    String query = "INSERT INTO " + dbName + "." + DBNAMERECORD + " (name, value) "
            + "VALUES ("
            + "'FRED',"
            + "'Bye Bye Blackbird'"
            + ");";
    System.out.println("Executing query " + query);
    session.execute(query);
    query = "INSERT INTO " + dbName + "." + DBREPLICACONTROLLER + " (name, value) "
            + "VALUES ("
            + "'SAM',"
            + "'La Petite Tonkinoise'"
            + ");";
    System.out.println("Executing query " + query);
    session.execute(query);
  }

  public void querySchema() {
    ResultSet results = session.execute("SELECT * FROM " + dbName + "." + DBREPLICACONTROLLER
            + " WHERE name = 'SAM';");
    for (Row row : results) {
      System.out.println(String.format("%-30s\t%-20s", row.getString("name"), row.getString("value")));
    }
    System.out.println();
  }

  public static void main(String[] args) {
    CassandraRecordsNew client = CassandraRecordsNew.getInstance();
    client.loadTestData();
    client.querySchema();
    //client.close();
    System.exit(0);
  }
}
