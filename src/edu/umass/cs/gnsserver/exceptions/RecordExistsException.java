/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsserver.exceptions;

/**
 * Exception means that field being created already exists.
 * This is sometimes not an error.
 * 
 * @author westy
 */
public class RecordExistsException extends GnsException {
  String collection;
  String guid;

  /**
   * Create a RecordExistsException.
   * 
   * @param collection
   * @param guid
   */
  public RecordExistsException(String collection, String guid) {
    this.collection = collection;
    this.guid = guid;
  }

  @Override
  public String getMessage() {
    return "RecordExistsException: " + " Collection = " + collection + " Guid = " + guid;
  }
}
