/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

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
