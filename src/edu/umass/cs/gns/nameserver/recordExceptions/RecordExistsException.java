package edu.umass.cs.gns.nameserver.recordExceptions;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 9/3/13
 * Time: 2:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class RecordExistsException extends Throwable {
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
