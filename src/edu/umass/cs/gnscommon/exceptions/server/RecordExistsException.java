
package edu.umass.cs.gnscommon.exceptions.server;


public class RecordExistsException extends ServerException {

  private static final long serialVersionUID = 1L;
  private final String collection;
  private final String guid;


  public RecordExistsException(String collection, String guid) {
    this.collection = collection;
    this.guid = guid;
  }

  @Override
  public String getMessage() {
    return "RecordExistsException: " + " Collection = " + collection + " Guid = " + guid;
  }
}
