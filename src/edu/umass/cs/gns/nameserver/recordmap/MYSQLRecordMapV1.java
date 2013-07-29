package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.database.MYSQLRecordTable;
import edu.umass.cs.gns.database.MYSQLRecordTableEntry;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.NameRecordV1;
import edu.umass.cs.gns.nameserver.NameRecordV1;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * UNUSED. OBSOLETE. HERE FOR HISTORICAL PURPOSES.
 * 
 * Stores GUID, KEY, VALUE triples
 * 
 * @author westy
 */
public class MYSQLRecordMapV1 extends BasicRecordMapV1 {

  @Override
  public NameRecordV1 getNameRecord(String name, NameRecordKey recordKey) {
    MYSQLRecordTable table = MYSQLRecordTable.getInstance();
    try {
      String recordString = table.lookup(name, recordKey.getName());
      if (recordString != null) {
        return new NameRecordV1(new JSONObject(recordString));
      } else {
        return null;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Error getting name record " + name + " / " + recordKey.getName() + " from " + table.getTableName() + " : " + e);
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void addNameRecord(NameRecordV1 recordEntry) {
    MYSQLRecordTable table = MYSQLRecordTable.getInstance();
    try {
      table.update(recordEntry.getName(), recordEntry.getRecordKey().getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Error adding name record: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void updateNameRecord(NameRecordV1 recordEntry) {
    addNameRecord(recordEntry);
  }

  @Override
  public void removeNameRecord(String name, NameRecordKey recordKey) {
    MYSQLRecordTable table = MYSQLRecordTable.getInstance();
    table.remove(name, recordKey.getName());
  }

  @Override
  public boolean containsName(String name, NameRecordKey recordKey) {
    MYSQLRecordTable table = MYSQLRecordTable.getInstance();
    return table.contains(name, recordKey.getName());
  }

  @Override
  public Set<NameRecordV1> getAllNameRecords() {
    MYSQLRecordTable table = MYSQLRecordTable.getInstance();
    Set<NameRecordV1> result = new HashSet();
    for (MYSQLRecordTableEntry entry : table.retrieveAllEntries()) {
      JSONObject json = entry.getJsonObject();
      Iterator<String> nameItr = json.keys();
      while (nameItr.hasNext()) {
        String name = nameItr.next();
        try {
          result.add(new NameRecordV1(json.getJSONObject(name)));
        } catch (JSONException e) {
          GNS.getLogger().severe("Error getting name record: " + e);
          e.printStackTrace();
        }
      }
    }
    return result;
  }
  
  @Override
  public void reset() {
   MYSQLRecordTable.getInstance().resetTable();
  }

}
