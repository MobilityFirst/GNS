/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsserver.database;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Encapsulates the name and type of a column in the database.
 *
 * @author Abhigyan
 */
public class ColumnField {

  private final String name;
  private final ColumnFieldType type;

  /** 
   * Create a column field instance.
   * 
   * @param name
   * @param type 
   */
  public ColumnField(String name, ColumnFieldType type) {
    this.name = name;
    this.type = type;
  }
  
  /**
   * Return the name of a column.
   * 
   * @return 
   */
  public String getName() {
    return name;
  }

  /**
   * Return the type of a column.
   * 
   * @return 
   */
  public ColumnFieldType type() {
    return type;
  }

  @Override
  public String toString() {
    return name + " " + type.toString();
  }

}
