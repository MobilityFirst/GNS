
package edu.umass.cs.gnsserver.database;


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
    return name + "/" + type.toString();
  }

}
