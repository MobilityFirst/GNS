/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnscommon;

/**
 * This class defines AclAccessType for ACLs
 *
 * @version 1.0
 */
public enum AclAccessType {
  /**
   * Whitelist of GUIDs authorized to read a field
   */
  READ_WHITELIST, /**
   * Whitelist of GUIDs authorized to write/update a field
   */
  WRITE_WHITELIST, /**
   * Black list of GUIDs not authorized to read a field
   */
  READ_BLACKLIST, /**
   * Black list of GUIDs not authorized to write/update a field
   */
  WRITE_BLACKLIST;

  /**
   *
   * @return the types as a string
   */
  public static String typesToString() {
    StringBuilder result = new StringBuilder();
    String prefix = "";
    for (AclAccessType type : AclAccessType.values()) {
      result.append(prefix);
      result.append(type.name());
      prefix = ", ";
    }
    return result.toString();
  }

}
