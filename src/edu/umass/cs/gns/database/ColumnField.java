/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import java.util.ArrayList;
import java.util.Arrays;

public class ColumnField {

  private final String name;
  private final ColumnFieldType type;

  public ColumnField(String name, ColumnFieldType type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }
  public ColumnFieldType type() {
    return type;
  }

  @Override
  public String toString() {
    return name + " " + type.toString();
  }
  
  public static ArrayList<ColumnField> keys(ColumnField ... fields) {
    return new ArrayList<ColumnField>(Arrays.asList(fields));
  }

  public static ArrayList<Object> values(Object ... objects) {
    return new ArrayList<Object>(Arrays.asList(objects));
  }
}
