/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.exceptions;

import edu.umass.cs.gns.exceptions.GnsException;
import edu.umass.cs.gns.database.ColumnField;

public class FieldNotFoundException extends GnsException{
  ColumnField missingField;
  public FieldNotFoundException(ColumnField f) {
    missingField = f;
  }

  @Override
  public String getMessage() {
    return "FieldNotFoundException: " + missingField.toString();
  }
}
