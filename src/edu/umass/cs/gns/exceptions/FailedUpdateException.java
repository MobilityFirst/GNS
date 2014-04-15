/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.exceptions;

public class FailedUpdateException extends GnsException {
  String collection;
  String insert;

  public FailedUpdateException(String collection, String insert) {
    this.collection = collection;
    this.insert = insert;
  }

  @Override
  public String getMessage() {
    return "FailedInsertionException: " + " Collection = " + collection + " Insert = " + insert;
  }
}
