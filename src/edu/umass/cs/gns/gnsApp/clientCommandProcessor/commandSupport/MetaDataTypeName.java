/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport;

import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.InternalField.makeInternalFieldString;

/**
 * Provides types for various meta data fields that are stored as internal fields.
 * 
 * @author westy
 */
public enum MetaDataTypeName {

  /**
   * The whitelist for reading.
   */
  READ_WHITELIST("ACL"), 

  /**
   * The whitelist for writing.
   */
  WRITE_WHITELIST("ACL"), 

  /**
   * The blacklist for reading.
   */
  READ_BLACKLIST("ACL"), 

  /**
   * The blacklist for writing.
   */
  WRITE_BLACKLIST("ACL"), 

  /**
   * A timestamp. Currently unused.
   */
  TIMESTAMP("MD");
  
  private String prefix;

  private MetaDataTypeName(String prefix) {
    this.prefix = makeInternalFieldString(prefix);
  }

  /**
   * Returns the prefix.
   * 
   * @return the prefix
   */
  public String getPrefix() {
    return prefix;
  }
  
  /**
   * Returns the path to the field.
   * 
   * @return the path
   */
  public String getFieldPath() {
    return prefix + "." + name();
  }
 
}
