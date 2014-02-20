/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.client;

/**
 *
 * @author westy
 */
public enum MetaDataTypeName {
  READ_WHITELIST, WRITE_WHITELIST, READ_BLACKLIST, WRITE_BLACKLIST, TIMESTAMP;

  public static String typesToString() {
    StringBuilder result = new StringBuilder();
    String prefix = "";
    for (MetaDataTypeName type : MetaDataTypeName.values()) {
      result.append(prefix);
      result.append(type.name());
      prefix = ", ";
    }
    return result.toString();
  }
  
}
