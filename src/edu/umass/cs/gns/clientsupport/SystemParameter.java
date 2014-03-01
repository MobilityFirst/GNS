/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

import java.lang.reflect.Field;

/**
 * Provides an interface to set values of static fields that act as system parameters.
 * Currently only supports boolean, string and integer fields.
 * Relies on the fact that the fields in question are static! 
 * 
 * @author westy
 */
public enum SystemParameter {
  // NOTE: All the fields in here MUST be static fields.
  EMAIL(ParameterType.Boolean, "edu.umass.cs.gns.main.GNS", "enableEmailAccountAuthentication",
  "Determines if email authentication is used."),
  SIGNATURE(ParameterType.Boolean, "edu.umass.cs.gns.main.GNS", "enableSignatureVerification",
  "Determines if query and field authentication using signatures and ACLS is enabled."),
  MAXGUIDS(ParameterType.Integer, "edu.umass.cs.gns.httpserver.Defs", "MAXGUIDS",
  "Puts a limit on the number of guids an account can have."),
  MAXALIASES(ParameterType.Integer, "edu.umass.cs.gns.httpserver.Defs", "MAXALIASES",
  "Puts a limit on the number of alias an account can have.");

  //
  public enum ParameterType {

    Boolean, Integer, String
  }
  //
  ParameterType type;
  String className;
  String fieldName;
  String description;

  private SystemParameter(ParameterType type, String className, String field, String description) {
    this.type = type;
    this.className = className;
    this.fieldName = field;
    this.description = description;
  }

  private Object parseValue(String value) {
    switch (type) {
      case Boolean:
        return Boolean.parseBoolean(value);
      case Integer:
        return Integer.parseInt(value);
      case String:
        return value;
      default:
        return value;
    }
  }

  public void setFieldValue(String value) throws Exception {
    Class closs = Class.forName(className);
    Field field = closs.getField(fieldName);
    // this relys on the fact that these are static fields
    Object o = closs.newInstance();
    field.set(o, parseValue(value));
  }

  public Object getFieldValue() throws Exception {
    Class closs = Class.forName(className);
    Field field = closs.getField(fieldName);
    Object o = closs.newInstance();
    return field.get(o);
  }

  public static String listParameters() {
    StringBuilder result = new StringBuilder();
    String prefix = "";
    for (SystemParameter parameter : SystemParameter.values()) {
      result.append(prefix);
      result.append(parameter.toString());
      result.append(" = ");
      try {
        result.append(parameter.getFieldValue().toString());
      } catch (Exception e) {
        result.append("<problem getting field value>");
      }
      result.append("   ");
      result.append(parameter.getDescription());
      prefix = NEWLINE;
    }
    return result.toString();
  }

  public ParameterType getType() {
    return type;
  }

  public String getClassName() {
    return className;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getDescription() {
    return description;
  }
  public final static String NEWLINE = System.getProperty("line.separator");
}
