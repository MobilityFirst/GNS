/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.installer;

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
  MONGO("edu.umass.cs.gns.gnsApp.recordmap.MongoRecordMap"),

  /**
   * Cassandra.
   */
  CASSANDRA("edu.umass.cs.gns.gnsApp.recordmap.CassandraRecordMap"),

  /**
   * For testing we sometimes use JSON.
   */
  IN_CORE_JSON("edu.umass.cs.gns.gnsApp.recordmap.InCoreRecordMapJSON");
  
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
