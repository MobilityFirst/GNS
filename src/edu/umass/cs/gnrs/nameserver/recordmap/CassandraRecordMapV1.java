package edu.umass.cs.gnrs.nameserver.recordmap;

import edu.umass.cs.gnrs.database.CassandraRecords;
import edu.umass.cs.gnrs.database.MongoRecords;
import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.nameserver.NameRecordKey;
import edu.umass.cs.gnrs.nameserver.NameRecordKey;
import edu.umass.cs.gnrs.nameserver.NameRecordV1;
import edu.umass.cs.gnrs.nameserver.NameRecordV1;
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
    String COLLECTION_NAME = MongoRecords.DBNAMERECORD;
  @Override
  public NameRecordV1 getNameRecord(String name, NameRecordKey recordKey) {
    
    CassandraRecords records = CassandraRecords.getInstance();

    try {
      String string = records.lookup(COLLECTION_NAME, name, recordKey.getName());
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
    CassandraRecords records = CassandraRecords.getInstance();
    try {
      records.insert(record.getName(), record.getRecordKey().getName(), record.toJSONObject().toString());
    } catch (JSONException e) {
      GNS.getLogger().severe("Error adding name record: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void updateNameRecord(NameRecordV1 record) {
    CassandraRecords records = CassandraRecords.getInstance();
    try {
      records.update(COLLECTION_NAME, record.getName(), record.getRecordKey().getName(), record.toJSONObject().toString());
    } catch (JSONException e) {
      GNS.getLogger().severe("Error updating name record: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void removeNameRecord(String name, NameRecordKey recordKey) {
    CassandraRecords records = CassandraRecords.getInstance();
    records.remove(name, recordKey.getName());
  }

  @Override
  public boolean containsName(String name, NameRecordKey recordKey) {
    CassandraRecords records = CassandraRecords.getInstance();
    return records.contains(name, recordKey.getName());
  }

  @Override
  public Set<NameRecordV1> getAllNameRecords() {
    CassandraRecords records = CassandraRecords.getInstance();
    Set<NameRecordV1> result = new HashSet();
    for (JSONObject json : records.retrieveAllEntries(COLLECTION_NAME)) {
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
   CassandraRecords.getInstance().resetKeySpace();
  }
  
   public static void main(String[] args) throws Exception {
     for (NameRecordV1 record : new CassandraRecordMapV1().getAllNameRecords()) {
       System.out.println(record);
     }
       
   }
}
