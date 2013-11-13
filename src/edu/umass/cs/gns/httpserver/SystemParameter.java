/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.httpserver;

import java.lang.reflect.Field;

/**
 * Currently only supports boolean parameters.
 * 
 * @author westy
 */
public enum SystemParameter {

  EMAIL("edu.umass.cs.gns.main.GNS", "enableEmailAccountAuthentication", "Determines if email authentication is used."),
  SIGNATURE("edu.umass.cs.gns.main.GNS", "enableSignatureVerification", "Determines query and field authentication using signatures and ACLS is enabled.");
  String className;
  String fieldName;
  String description;

  private SystemParameter(String className, String field, String description) {
    this.className = className;
    this.fieldName = field;
    this.description = description;
  }

  public String getClassName() {
    return className;
  }

  public String getField() {
    return fieldName;
  }

  public String getDescription() {
    return description;
  }

  public void setFieldBoolean(boolean b) throws Exception {
    Class closs = Class.forName(className);
    Field field = closs.getField(fieldName);
    Object o = closs.newInstance();
    field.setBoolean(o, b);
  }
  
  public Boolean getFieldBoolean() throws Exception {
    Class closs = Class.forName(className);
    Field field = closs.getField(fieldName);
    Object o = closs.newInstance();
    return field.getBoolean(o);
  }
}
