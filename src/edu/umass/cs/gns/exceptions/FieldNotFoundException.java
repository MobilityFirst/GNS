/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.exceptions;

import edu.umass.cs.gns.exceptions.GnsException;
import edu.umass.cs.gns.database.Field;

public class FieldNotFoundException extends GnsException{
  Field missingField;
  public FieldNotFoundException(Field f) {
    missingField = f;
  }

  @Override
  public String getMessage() {
    return "FieldNotFoundException: " + missingField.toString();
  }
}
