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
package edu.umass.cs.gnsserver.installer;

/**
 * Represents the type of data store we're using.
 * Default is MONGO.
 * 
 * @author westy
 */
public enum DataStoreType {
 
  /**
   * Mongo.
   */
  MONGO("edu.umass.cs.gnsserver.gnsApp.recordmap.MongoRecordMap"),

  /**
   * Cassandra.
   */
  CASSANDRA("edu.umass.cs.gnsserver.gnsApp.recordmap.CassandraRecordMap"),

  /**
   * For testing we sometimes use JSON.
   */
  IN_CORE_JSON("edu.umass.cs.gnsserver.gnsApp.recordmap.InCoreRecordMapJSON");
  
  String className;

  private DataStoreType(String className) {
    this.className = className;
  }

  /**
   * Returns the associated classname.
   * 
   * @return the class name
   */
  public String getClassName() {
    return className;
  }
  
}
