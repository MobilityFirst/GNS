package edu.umass.cs.gnrs.nameserver;

import edu.umass.cs.gnrs.nameserver.NameRecord;
import edu.umass.cs.gnrs.nameserver.NameRecordKey;
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
public class InCoreRecordMapV2 extends BasicRecordMapV2 {

  private ConcurrentMap<String, NameRecord> recordMap;

  public InCoreRecordMapV2() {
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
  public void reset() {
    recordMap.clear();
  }

  @Override
  public void updateNameRecordField(String name, NameRecordKey recordKey, String value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getNameRecordField(String name, NameRecordKey recordKey) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
