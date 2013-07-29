/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnrs.nameserver;

import edu.umass.cs.gnrs.nameserver.NameRecordKey;
import edu.umass.cs.gnrs.nameserver.NameRecordV1;
import java.util.Set;

/**
 * THIS IS ONLY STILL HERE SO THAT OLDER CASSANDRA CODE COMPILES.
 * IT WILL GO AWAY ONCE I UPDATE THAT.
 * @author westy
 */
public interface RecordMapInterfaceV1 {
  
  public NameRecordV1 getNameRecord(String name, NameRecordKey recordKey);
  public void addNameRecord(NameRecordV1 recordEntry);
  public void updateNameRecord(NameRecordV1 recordEntry);
  public void removeNameRecord(String name, NameRecordKey recordKey);
  public boolean containsName(String name, NameRecordKey recordKey);
  public Set<NameRecordV1> getAllNameRecords();
  public String tableToString();
  public void reset();
  
}
