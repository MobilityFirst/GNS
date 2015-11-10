/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsserver.exceptions;

/**
 * Exception means that the desired database operation could not be completed.
 * Probably because the database server is crashed.
 *
 * This exception does not tell much about the cause of the error. Therefore, if this exception
 * is seen, one can only attempt to retry the operation hoping that the database 
 * unavailability is transient. If indeed the database is permanently crashed, 
 * an external mechanism would be needed to restart the database.
 * 
 */
public class FailedDBOperationException extends GnsException {
  String collection;
  String name;

  /**
   * Create a FailedDBOperationException instance.
   * 
   * @param collection
   * @param name
   */
  public FailedDBOperationException(String collection, String name) {
    this.collection = collection;
    this.name = name;
  }

  @Override
  public String getMessage() {
    return "FailedDBOperationException: " + " Collection = " + collection + " Insert = " + name;
  }
}
