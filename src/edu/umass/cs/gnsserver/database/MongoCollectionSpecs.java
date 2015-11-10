/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
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
