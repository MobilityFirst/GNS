/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.database;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import static edu.umass.cs.gnsserver.database.MongoRecords.DBNAMERECORD;
import edu.umass.cs.utils.DiskMap;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class DiskMapCollection {

  private DiskMap<String, JSONObject> map;
  private MongoRecords<String> mongoRecords;

  public DiskMapCollection(String nodeID, String collectionName) {
    this(nodeID, -1, collectionName);
  }

  public DiskMapCollection(String nodeID, int port, String collectionName) {
    this.mongoRecords = new MongoRecords<>(nodeID + "-"
            + collectionName + new Random().nextInt(), port);
    this.map = new DiskMap<String, JSONObject>(100000) {
      @Override
      public Set<String> commit(Map<String, JSONObject> toCommit) throws IOException {
        DatabaseConfig.getLogger().fine("Commit: " + toCommit);
        try {
          mongoRecords.bulkInsert(DBNAMERECORD, toCommit);
        } catch (FailedDBOperationException | RecordExistsException e) {
          throw new IOException(e);
        }
        return toCommit.keySet();
      }

      @Override
      public JSONObject restore(String key) throws IOException {
        try {
          return mongoRecords.lookupEntireRecord(DBNAMERECORD, key);
        } catch (FailedDBOperationException | RecordNotFoundException e) {
          throw new IOException(e);
        }
      }
    };
  }

  public DiskMap<String, JSONObject> getMap() {
    return map;
  }

  public MongoRecords<String> getMongoRecords() {
    return mongoRecords;
  }

  // test code... there's also a junit test elsewhere
  public static void main(String[] args) throws Exception, RecordNotFoundException {

    DiskMapCollection map = new DiskMapCollection("node", "test");

    System.exit(0);
  }
}
