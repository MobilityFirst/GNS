
package edu.umass.cs.gnscommon.exceptions.server;

import edu.umass.cs.gnsserver.database.ColumnField;


public class FieldNotFoundException extends ServerException{

  private static final long serialVersionUID = 1L;
  private final ColumnField missingField;


  public FieldNotFoundException(ColumnField f) {
    missingField = f;
  }

  @Override
  public String getMessage() {
    return "FieldNotFoundException: " + missingField.toString();
  }
}
