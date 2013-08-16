package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Stores GUID, KEY, VALUE triples
 *
 * @author westy
 */
public class InCoreRecordMap extends BasicRecordMap {

  private ConcurrentMap<String, NameRecord> recordMap;

  public InCoreRecordMap() {
    recordMap = new ConcurrentHashMap<String, NameRecord>();
  }

  @Override
  public NameRecord getNameRecord(String name) {
    return recordMap.get(name);
  }

  @Override
  public void addNameRecord(NameRecord recordEntry) {
    recordMap.put(recordEntry.getName(), recordEntry);
  }

  @Override
  public void addNameRecord(JSONObject json) {
    try {
      recordMap.put(json.getString(NameRecord.NAME), new NameRecord(json));
    } catch (JSONException e) {
      GNS.getLogger().severe("Error getting json record: " + e);
    }
  }

  @Override
  public void updateNameRecord(NameRecord recordEntry) {
    addNameRecord(recordEntry);
  }

  @Override
  public void removeNameRecord(String name) {
    recordMap.remove(name);
  }

  @Override
  public boolean containsName(String name) {
    return recordMap.containsKey(name);
  }

  @Override
  public Set<NameRecord> getAllNameRecords() {
    Set<NameRecord> result = new HashSet();
    for (Map.Entry<String, NameRecord> entry : recordMap.entrySet()) {
      result.add(entry.getValue());
    }
    return result;
  }

  @Override
  public Set<String> getAllRowKeys() {
    return recordMap.keySet();
  }

  @Override
  public Set<String> getAllColumnKeys(String name) {
    if (!containsName(name)) {
      return recordMap.get(name).getValuesMap().keySet();
    } else {
      return null;
    }
  }

  @Override
  public void reset() {
    recordMap.clear();
  }

  @Override
  public NameRecord getNameRecordLazy(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void updateNameRecordField(String name, String key, String value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void updateNameRecordListValue(String name, String key, ArrayList<String> value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void updateNameRecordListValueInt(String name, String key, Set<Integer> value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getNameRecordField(String name, String key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public ReplicaControllerRecord getNameRecordPrimary(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  @Override
  public ReplicaControllerRecord getNameRecordPrimaryLazy(String name) {
     throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void addNameRecordPrimary(ReplicaControllerRecord recordEntry) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Set<ReplicaControllerRecord> getAllPrimaryNameRecords() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
