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
package edu.umass.cs.gnsserver.database;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Encapsulates the name and type of a column in the database.
 *
 * @author Abhigyan
 */
public class ColumnField {

  private final String name;
  private final ColumnFieldType type;

  /** 
   * Create a column field instance.
   * 
   * @param name
   * @param type 
   */
  public ColumnField(String name, ColumnFieldType type) {
    this.name = name;
    this.type = type;
  }
  
  /**
   * Return the name of a column.
   * 
   * @return 
   */
  public String getName() {
    return name;
  }

  /**
   * Return the type of a column.
   * 
   * @return 
   */
  public ColumnFieldType type() {
    return type;
  }

  @Override
  public String toString() {
    return name + " " + type.toString();
  }

}
