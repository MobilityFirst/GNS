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
 * Implements a "collection" (in the sense of mongo) of records whose
 * primary database is a DiskMap with Mongo as the backup for when we
 * need more NoSQL databasey features.
 *
 * A collection is basically a named separate namespace for documents.
 * A document is a JSONObject.
 *
 * @author westy
 */
public class DiskMapCollection {

  private DiskMap<String, JSONObject> map;
  private MongoRecords<String> mongoRecords;

  /**
   * Create a DiskMapCollection name collection on a given nodeID.
   * Note: nodeID is here so we can run multiple hosts on the same machine.
   *
   * @param nodeID
   * @param collectionName
   */
  public DiskMapCollection(String nodeID, String collectionName) {
    this(nodeID, -1, collectionName);
  }

  /**
   * Create a DiskMapCollection name collection on a given nodeID.
   * Specify port if you want to override the default mongo port.
   * Note: nodeID is here so we can run multiple hosts on the same machine.
   *
   * @param nodeID
   * @param port
   * @param collectionName
   */
  public DiskMapCollection(String nodeID, int port, String collectionName) {
    this.mongoRecords = new MongoRecords<>(nodeID + "-"
            + collectionName + new Random().nextInt(), port);
    this.map = new DiskMap<String, JSONObject>(100000) {
      @Override
      public Set<String> commit(Map<String, JSONObject> toCommit) throws IOException {
        DatabaseConfig.getLogger().fine("Commit: " + toCommit);
        try {
          mongoRecords.bulkUpdate(DBNAMERECORD, toCommit);
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
          return null;
        }
      }
    };
  }

  /**
   * 
   * @return the diskmap
   */
  public DiskMap<String, JSONObject> getMap() {
    return map;
  }

  /**
   * 
   * @return the mongo records
   */
  public MongoRecords<String> getMongoRecords() {
    return mongoRecords;
  }

}
