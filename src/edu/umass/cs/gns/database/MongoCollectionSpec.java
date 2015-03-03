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
  private final String name;
  private final ColumnField primaryKey;
  private final BasicDBObject primaryIndex;
  private final List<BasicDBObject> otherIndexes = new ArrayList<>();

  public MongoCollectionSpec(String name, ColumnField primaryKey) {
    this.name = name;
    this.primaryKey = primaryKey;
    this.primaryIndex = new BasicDBObject(primaryKey.getName(), 1);
  }

  public String getName() {
    return name;
  }

  public ColumnField getPrimaryKey() {
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
  
}
