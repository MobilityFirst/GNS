/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.installer;

/**
 *
 * @author westy
 */
public enum DataStoreType {
 
  MONGO("edu.umass.cs.gns.nameserver.recordmap.MongoRecordMap"),
  CASSANDRA("edu.umass.cs.gns.nameserver.recordmap.CassandraRecordMap"),
  IN_CORE_JSON("edu.umass.cs.gns.nameserver.recordmap.InCoreRecordMapJSON");
  
  String className;

  private DataStoreType(String className) {
    this.className = className;
  }

  public String getClassName() {
    return className;
  }
  
}
