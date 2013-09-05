package edu.umass.cs.gns.nameserver.recordExceptions;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 9/2/13
 * Time: 9:57 AM
 * To change this template use File | Settings | File Templates.
 */
public class RecordNotFoundException extends Throwable{
  String name;
  public RecordNotFoundException(String name) {
    this.name = name;
  }



}
