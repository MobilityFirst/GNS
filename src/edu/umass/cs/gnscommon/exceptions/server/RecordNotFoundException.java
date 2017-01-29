
package edu.umass.cs.gnscommon.exceptions.server;




public class RecordNotFoundException extends ServerException{

  private static final long serialVersionUID = 1L;
  private final String name;


  public RecordNotFoundException(String name) {
    this.name = name;
  }



}
