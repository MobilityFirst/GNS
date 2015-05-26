/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport;

import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.InternalField.makeInternalFieldString;

/**
 * Provides types for various meta data fields that are stored as internal fields.
 * 
 * @author westy
 */
public enum MetaDataTypeName {
  READ_WHITELIST("ACL"), 
  WRITE_WHITELIST("ACL"), 
  READ_WHITELIST_GUID("ACL"), 
  WRITE_WHITELIST_GUID("ACL"), 
  READ_BLACKLIST("ACL"), 
  WRITE_BLACKLIST("ACL"), 
  TIMESTAMP("MD");
  
  private String prefix;

  private MetaDataTypeName(String prefix) {
    this.prefix = makeInternalFieldString(prefix);
  }

  /**
   *
   * @return
   */
  public String getPrefix() {
    return prefix;
  }
  
  /**
   *
   * @return
   */
  public String getFieldPath() {
    return prefix + "." + name();
  }
 
//  /**
//   * Currently unused.
//   * @return
//   */
//  public static String typesToString() {
//    StringBuilder result = new StringBuilder();
//    String prefix = "";
//    for (MetaDataTypeName type : MetaDataTypeName.values()) {
//      result.append(prefix);
//      result.append(type.name());
//      prefix = ", ";
//    }
//    return result.toString();
//  }
  
}
