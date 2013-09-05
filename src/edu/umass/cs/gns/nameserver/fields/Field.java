package edu.umass.cs.gns.nameserver.fields;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 9/2/13
 * Time: 1:15 AM
 * To change this template use File | Settings | File Templates.
 */
public class Field {

  final String fieldName;
  final FieldType type;

  public Field(String fieldName, FieldType type) {
    this.fieldName = fieldName;
    this.type = type;
  }

  public String getFieldName() {
    return fieldName;
  }
  public FieldType type() {
    return type;
  }

  @Override
  public String toString() {
    return fieldName + " " + type.toString();
  }
}
