/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.exceptions;

public class RecordExistsException extends GnsException {
  String collection;
  String guid;

  public RecordExistsException(String collection, String guid) {
    this.collection = collection;
    this.guid = guid;
  }

  @Override
  public String getMessage() {
    return "RecordExistsException: " + " Collection = " + collection + " Guid = " + guid;
  }
}
