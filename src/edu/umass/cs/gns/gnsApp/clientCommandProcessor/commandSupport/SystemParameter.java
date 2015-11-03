/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport;

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
  /**
   * Determines if email verification is used.
   */
  EMAIL_VERIFICATION(ParameterType.Boolean, "edu.umass.cs.gns.main.GNS", "enableEmailAccountVerification",
          "Determines if email verification is used."),
  /**
   * Determines if query and field authentication using signatures and ACLS is enabled.
   */
  SIGNATURE_AUTHENTICATION(ParameterType.Boolean, "edu.umass.cs.gns.main.GNS", "enableSignatureAuthentication",
          "Determines if query and field authentication using signatures and ACLS is enabled."),
  /**
   * Puts a limit on the number of guids an account can have.
   */
  MAX_GUIDS(ParameterType.Integer, "edu.umass.cs.gns.main.GNS", "MAXGUIDS",
          "Puts a limit on the number of guids an account can have."),
  /**
   * Puts a limit on the number of alias an account can have.
   */
  MAX_ALIASES(ParameterType.Integer, "edu.umass.cs.gns.main.GNS", "MAXALIASES",
          "Puts a limit on the number of alias an account can have.");

  //
  /**
   * The type of the parameter.
   */
  public enum ParameterType {

    /**
     * A Boolean parameter.
     */
    Boolean,
    /**
     * An Integer parameter.
     */
    Integer,
    /**
     * A String parameter.
     */
    String
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

  /**
   * Sets the field value.
   * 
   * @param value
   * @throws Exception
   */
  public void setFieldValue(String value) throws Exception {
    Class closs = Class.forName(className);
    Field field = closs.getField(fieldName);
    // this relys on the fact that these are static fields
    Object o = closs.newInstance();
    field.set(o, parseValue(value));
  }

  /**
   * Returns the field value.
   * 
   * @return an object
   * @throws Exception
   */
  public Object getFieldValue() throws Exception {
    Class closs = Class.forName(className);
    Field field = closs.getField(fieldName);
    Object o = closs.newInstance();
    return field.get(o);
  }

  /**
   * List all of the parameters.
   * 
   * @return a descriptive string
   */
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

  /**
   * Return the type.
   * 
   * @return a ParameterType
   */
  public ParameterType getType() {
    return type;
  }

  /**
   * Return the class name.
   * 
   * @return a string
   */
  public String getClassName() {
    return className;
  }

  /**
   * Return the field name.
   * 
   * @return a string
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * Return the description.
   * 
   * @return a string
   */
  public String getDescription() {
    return description;
  }

  private final static String NEWLINE = System.getProperty("line.separator");
}
