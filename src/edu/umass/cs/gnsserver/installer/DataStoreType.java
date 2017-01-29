
package edu.umass.cs.gnsserver.installer;


public enum DataStoreType {
 

  MONGO("edu.umass.cs.gnsserver.gnsApp.recordmap.MongoRecordMap"),


  CASSANDRA("edu.umass.cs.gnsserver.gnsApp.recordmap.CassandraRecordMap"),


  IN_CORE_JSON("edu.umass.cs.gnsserver.gnsApp.recordmap.InCoreRecordMapJSON");
  
  String className;

  private DataStoreType(String className) {
    this.className = className;
  }


  public String getClassName() {
    return className;
  }
  
}
