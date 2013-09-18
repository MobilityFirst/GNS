/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import java.util.ArrayList;
import java.util.Arrays;

public class Field {

  final String name;
  final FieldType type;

  public Field(String name, FieldType type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }
  public FieldType type() {
    return type;
  }

  @Override
  public String toString() {
    return name + " " + type.toString();
  }
  
  public static ArrayList<Field> keys(Field ... fields) {
    return new ArrayList<Field>(Arrays.asList(fields));
  }

  public static ArrayList<Object> values(Object ... objects) {
    return new ArrayList<Object>(Arrays.asList(objects));
  }
}
