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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Maintains information about the collections we maintain in the mongo db.
 * 
 * @author westy
 */
public class MongoCollectionSpecs {
  
  private final Map<String, MongoCollectionSpec> collectionSpecMap = new HashMap<>();

  private final List<MongoCollectionSpec> collectionSpecs = new ArrayList<>();
  
  /**
   * Add a collection spec to the collection specs.
   * 
   * @param name
   * @param indexField
   */
  public void addCollectionSpec(String name, ColumnField indexField) {
    MongoCollectionSpec spec = new MongoCollectionSpec(name, indexField);
    collectionSpecs.add(spec);
    collectionSpecMap.put(name, spec);
  }
          
  /**
   * Retrieve a collection specs.
   * 
   * @param name
   * @return the collection spec
   */
  public MongoCollectionSpec getCollectionSpec(String name) {
    return collectionSpecMap.get(name);
  }
  
  /**
   * Retrieve all the collection specs.
   * 
   * @return a list of collection specifications
   */
  public List<MongoCollectionSpec> allCollectionSpecs() {
    return collectionSpecs;
  }
  
}
