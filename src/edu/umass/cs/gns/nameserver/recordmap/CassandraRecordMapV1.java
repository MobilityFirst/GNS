package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.database.CassandraRecordsV1;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.NameRecordV1;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

/**
 * Stores GUID, KEY, VALUE triples in a Cassandra Database
 *
 * @author westy
 */
public class CassandraRecordMapV1 extends BasicRecordMapV1 {
  private String collectionName;

  public CassandraRecordMapV1(String collectionName) {
    this.collectionName = collectionName;
  }
  
  @Override
  public NameRecordV1 getNameRecord(String name, NameRecordKey recordKey) {
    CassandraRecordsV1 records = CassandraRecordsV1.getInstance();
    try {
      String string = records.lookup(collectionName, name, recordKey.getName());
      if (string != null) {
        return new NameRecordV1(new JSONObject(string));
      } else {
        return null;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Error getting name record " + name + " / " + recordKey.getName() + " from " + records.toString() + " : " + e);
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void addNameRecord(NameRecordV1 record) {
    CassandraRecordsV1 records = CassandraRecordsV1.getInstance();
    try {
      records.insert(collectionName, record.getName(), record.toJSONObject().toString());
    } catch (JSONException e) {
      GNS.getLogger().severe("Error adding name record: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void updateNameRecord(NameRecordV1 record) {
    CassandraRecordsV1 records = CassandraRecordsV1.getInstance();
    try {
      records.update(collectionName, record.getName(), record.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Error updating name record: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void removeNameRecord(String name, NameRecordKey recordKey) {
    CassandraRecordsV1 records = CassandraRecordsV1.getInstance();
    records.remove(name, recordKey.getName());
  }

  @Override
  public boolean containsName(String name, NameRecordKey recordKey) {
    CassandraRecordsV1 records = CassandraRecordsV1.getInstance();
    return records.contains(name, recordKey.getName());
  }

  @Override
  public Set<NameRecordV1> getAllNameRecords() {
    CassandraRecordsV1 records = CassandraRecordsV1.getInstance();
    Set<NameRecordV1> result = new HashSet();
    for (JSONObject json : records.retrieveAllEntries(collectionName)) {
      try {
        result.add(new NameRecordV1(json));
      } catch (JSONException e) {
        GNS.getLogger().severe("Error getting name record: " + e);
        e.printStackTrace();
      }
    }
    return result;
  }
  
  @Override
  public void reset() {
   CassandraRecordsV1.getInstance().resetKeySpace();
  }
}
