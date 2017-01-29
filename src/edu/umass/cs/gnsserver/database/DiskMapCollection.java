
package edu.umass.cs.gnsserver.database;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import static edu.umass.cs.gnsserver.database.MongoRecords.DBNAMERECORD;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DiskMap;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.json.JSONObject;


public class DiskMapCollection {

  private DiskMap<String, JSONObject> map;
  private MongoRecords mongoRecords;


  public DiskMapCollection(String nodeID, String collectionName) {
    this(nodeID, -1, collectionName);
  }


  public DiskMapCollection(String nodeID, int port, String collectionName) {
    this.mongoRecords = new MongoRecords(nodeID + "-"
            + collectionName + new Random().nextInt(), port);
    this.map = new DiskMap<String, JSONObject>(!Config.getGlobalBoolean(GNSConfig.GNSC.IN_MEMORY_DB) ? 128*1024 :
    	Long.MAX_VALUE) {
      @Override
      public Set<String> commit(Map<String, JSONObject> toCommit) throws IOException {
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
        } catch (FailedDBOperationException e) {
        	GNSConfig.getLogger().severe(e.getMessage());
        	e.printStackTrace();
        } catch (RecordNotFoundException e) {
        	// silently return null
        	return null;
        }
        return null;
      }
    };
  }


  public DiskMap<String, JSONObject> getMap() {
    return map;
  }


  public MongoRecords getMongoRecords() {
    return mongoRecords;
  }

}
