/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nameserver.fields;

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
}
