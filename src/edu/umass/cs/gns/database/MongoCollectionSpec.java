/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import com.mongodb.BasicDBObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author westy
 */
public class MongoCollectionSpec {

  /**
   * Stores the name, primary key, and index of each collection we maintain in the mongo db.
   */
  private String name;
  private Field primaryKey;
  private BasicDBObject primaryIndex;
  private List<BasicDBObject> otherIndexes = new ArrayList<BasicDBObject>();

  public MongoCollectionSpec(String name, Field primaryKey) {
    this.name = name;
    this.primaryKey = primaryKey;
    this.primaryIndex = new BasicDBObject(primaryKey.getName(), 1);
  }

  public String getName() {
    return name;
  }

  public Field getPrimaryKey() {
    return primaryKey;
  }

  public BasicDBObject getPrimaryIndex() {
    return primaryIndex;
  }
  
  public void addOtherIndex(BasicDBObject index) {
    otherIndexes.add(index);
  }
  
  public List<BasicDBObject> getOtherIndexes() {
    return otherIndexes;
  }
  
  // maintains information about the collections we maintain in the mongo db
  private static Map<String, MongoCollectionSpec> collectionSpecMap = new HashMap<String, MongoCollectionSpec>();

  private static List<MongoCollectionSpec> collectionSpecs = new ArrayList<MongoCollectionSpec>();
  
  public static void addCollectionSpec(String name, Field indexField) {
    MongoCollectionSpec spec = new MongoCollectionSpec(name, indexField);
    collectionSpecs.add(spec);
    collectionSpecMap.put(name, spec);
  }
          
  public static MongoCollectionSpec getCollectionSpec(String name) {
    return collectionSpecMap.get(name);
  }
  
  public static List<MongoCollectionSpec> allCollectionSpecs() {
    return collectionSpecs;
  }
  
}
