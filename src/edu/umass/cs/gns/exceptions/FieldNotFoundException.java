/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.exceptions;

import edu.umass.cs.gns.database.ColumnField;

/**
 * Exception means that field being accessed does not exist.
 * This is often not an error.
 * 
 * @author westy
 */
public class FieldNotFoundException extends GnsException{
  ColumnField missingField;

  /**
   * Create a FieldNotFoundException instance.
   * 
   * @param f
   */
  public FieldNotFoundException(ColumnField f) {
    missingField = f;
  }

  @Override
  public String getMessage() {
    return "FieldNotFoundException: " + missingField.toString();
  }
}
