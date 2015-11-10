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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.exceptions;

import edu.umass.cs.gnsserver.database.ColumnField;

/**
 * Exception means that field being accessed does not exist.
 * This is often not an error.
 * 
 * @author westy
 */
public class FieldNotFoundException extends GnsException{
  ColumnField missingField;

  /**
   * Create a FieldNotFoundException instance.
   * 
   * @param f
   */
  public FieldNotFoundException(ColumnField f) {
    missingField = f;
  }

  @Override
  public String getMessage() {
    return "FieldNotFoundException: " + missingField.toString();
  }
}
