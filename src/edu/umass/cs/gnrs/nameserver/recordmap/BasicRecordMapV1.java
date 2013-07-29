/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnrs.nameserver.recordmap;

import edu.umass.cs.gnrs.nameserver.NameRecordV1;
import edu.umass.cs.gnrs.nameserver.NameRecordV1;

/**
 * THIS IS ONLY STILL HERE SO THAT OLDER CASSANDRA CODE COMPILES.
 * IT WILL GO AWAY ONCE I UPDATE THAT.
 * @author westy
 */
public abstract class BasicRecordMapV1 implements RecordMapInterfaceV1 {
  
  @Override
  public String tableToString() {
    StringBuilder table = new StringBuilder();
    for (NameRecordV1 record : getAllNameRecords()) {
      table.append(record.getName() + ": ");
      table.append(record.toString());
      table.append("\n");
    }
    return table.toString();
  }
  
}
