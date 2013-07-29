/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnrs.nameserver.recordmap;

import edu.umass.cs.gnrs.nameserver.NameRecord;
import edu.umass.cs.gnrs.nameserver.NameRecord;
import edu.umass.cs.gnrs.nameserver.NameRecordKey;
import edu.umass.cs.gnrs.nameserver.NameRecordKey;
import java.util.Set;

/**
 *
 * @author westy
 */
public interface RecordMapInterfaceV2 {
  
  public NameRecord getNameRecord(String name);
  public void addNameRecord(NameRecord recordEntry);
  public void updateNameRecord(NameRecord recordEntry);
  public void updateNameRecordField(String name, NameRecordKey recordKey, String value);
  public String getNameRecordField(String name, NameRecordKey recordKey);
  public void removeNameRecord(String name);
  public boolean containsName(String name);
  public Set<NameRecord> getAllNameRecords();
  public String tableToString();
  public void reset();
  
}
