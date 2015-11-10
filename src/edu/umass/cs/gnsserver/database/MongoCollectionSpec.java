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

import com.mongodb.BasicDBObject;
import java.util.ArrayList;
import java.util.List;

/**
 * Stores the name, primary key, and index of each collection we maintain in the mongo db.
 *
 * @author westy
 */
public class MongoCollectionSpec {

  private final String name;
  private final ColumnField primaryKey;
  private final BasicDBObject primaryIndex;
  private final List<BasicDBObject> otherIndexes = new ArrayList<>();

  /**
   * Creates a MongoCollectionSpec instance.
   *
   * @param name
   * @param primaryKey
   */
  public MongoCollectionSpec(String name, ColumnField primaryKey) {
    this.name = name;
    this.primaryKey = primaryKey;
    this.primaryIndex = new BasicDBObject(primaryKey.getName(), 1);
  }

  /**
   * Returns the name of the collection.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the name of the collection.
   *
   * @return the column field object represnting the key
   */
  public ColumnField getPrimaryKey() {
    return primaryKey;
  }

  /**
   * Returns the primary index of the collection.
   *
   * @return the index representation as a BasicDBObject
   */
  public BasicDBObject getPrimaryIndex() {
    return primaryIndex;
  }

  /**
   * Adds a secondary index to the collection.
   * @param index
   */
  public void addOtherIndex(BasicDBObject index) {
    otherIndexes.add(index);
  }

  /**
   * Returns a list of the a secondary indices of the collection.
   *
   * @return the indices as a list of BasicDBObject
   */
  public List<BasicDBObject> getOtherIndexes() {
    return otherIndexes;
  }

}
