/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.exceptions;

import edu.umass.cs.gns.exceptions.GnsException;

public class RecordExistsException extends GnsException {
  String collection;
  String guid;

  public RecordExistsException(String collection, String guid) {
    this.collection = collection;
    this.guid = guid;
  }

  public String getMessage() {
    return "RecordExistsException: " + " Collection = " + collection + " Guid = " + guid;
  }
}
