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
package edu.umass.cs.gnsserver.utils;

import java.util.ArrayList;
import java.util.Collection;

// PLEASE DON'T DELETE THIS CLASS WITHOUT TALKING TO WESTY FIRST!

/**
 * This class is used to represent values in key / values for some of the "system" keys.
 * It differs from ResultValue in that the elements are Strings.
 *
 * @author westy
 */
public class ResultValueString extends ArrayList<String> {

  /**
   * Create an empty ResultValueString instance.
   */
  public ResultValueString() {
  }

  /**
   * Create a ResultValueString instance from the contents of the collection.
   * 
   * @param collection
   */
  public ResultValueString(Collection<? extends String> collection) {
    super(collection);
  }
  
}
