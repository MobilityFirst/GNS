
package edu.umass.cs.gnsserver.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoCollectionSpecs {
  
  private final Map<String, MongoCollectionSpec> collectionSpecMap = new HashMap<>();

  private final List<MongoCollectionSpec> collectionSpecs = new ArrayList<>();
  

  public void addCollectionSpec(String name, ColumnField indexField) {
    MongoCollectionSpec spec = new MongoCollectionSpec(name, indexField);
    collectionSpecs.add(spec);
    collectionSpecMap.put(name, spec);
  }
          

  public MongoCollectionSpec getCollectionSpec(String name) {
    return collectionSpecMap.get(name);
  }
  

  public List<MongoCollectionSpec> allCollectionSpecs() {
    return collectionSpecs;
  }
  
}
