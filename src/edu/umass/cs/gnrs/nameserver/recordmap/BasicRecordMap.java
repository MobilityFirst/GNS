/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnrs.nameserver.recordmap;

import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author westy
 */
public abstract class BasicRecordMap implements RecordMapInterface {
  
  public void updateNameRecordField(String name, String key, String value) {
    updateNameRecordField(name, key, new ArrayList(Arrays.asList(value)));
  }

  @Override
  public String tableToString() {
    StringBuilder table = new StringBuilder();
    for (String row : getAllRowKeys()) {
      table.append(row + ": ");
      String prefix = "";
      for (String column : getAllColumnKeys(row)) {
        table.append(prefix);
        table.append(column);
        table.append(" -> ");
        table.append(getNameRecordField(row, column));
        prefix = ", ";
      }
      table.append("\n");
    }
    return table.toString();
  }
}
