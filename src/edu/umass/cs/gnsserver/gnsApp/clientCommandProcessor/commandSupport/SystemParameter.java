/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

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
  EMAIL_VERIFICATION(ParameterType.Boolean, "edu.umass.cs.gnsserver.main.GNS", "enableEmailAccountVerification",
          "Determines if email verification is used."),
  /**
   * Determines if query and field authentication using signatures and ACLS is enabled.
   */
  SIGNATURE_AUTHENTICATION(ParameterType.Boolean, "edu.umass.cs.gnsserver.main.GNS", "enableSignatureAuthentication",
          "Determines if query and field authentication using signatures and ACLS is enabled."),
  /**
   * Puts a limit on the number of guids an account can have.
   */
  MAX_GUIDS(ParameterType.Integer, "edu.umass.cs.gnsserver.main.GNS", "MAXGUIDS",
          "Puts a limit on the number of guids an account can have."),
  /**
   * Puts a limit on the number of alias an account can have.
   */
  MAX_ALIASES(ParameterType.Integer, "edu.umass.cs.gnsserver.main.GNS", "MAXALIASES",
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
