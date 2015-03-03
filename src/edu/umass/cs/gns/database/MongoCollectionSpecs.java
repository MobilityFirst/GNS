/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

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
  
  private Map<String, MongoCollectionSpec> collectionSpecMap = new HashMap<String, MongoCollectionSpec>();

  private List<MongoCollectionSpec> collectionSpecs = new ArrayList<MongoCollectionSpec>();
  
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
