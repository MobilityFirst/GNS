/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.InternalField.makeInternalFieldString;

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
