
package edu.umass.cs.gnscommon.exceptions.server;


public class FailedDBOperationException extends ServerException {

  private static final long serialVersionUID = 6627620787610127842L;

  private final String collection;
  private final String name;
  private final String message;


  public FailedDBOperationException(String collection, String name, String message) {
    this.collection = collection;
    this.name = name;
    this.message = message;
  }

  @Override
  public String getMessage() {
    return "FailedDBOperationException: " + " collection = " + collection + " name = " + name
            + (message != null ? "message = " + message : "");
  }
}
