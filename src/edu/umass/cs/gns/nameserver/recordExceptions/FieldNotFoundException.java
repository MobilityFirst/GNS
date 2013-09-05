package edu.umass.cs.gns.nameserver.recordExceptions;

import edu.umass.cs.gns.nameserver.fields.Field;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 9/2/13
 * Time: 9:58 AM
 * To change this template use File | Settings | File Templates.
 */
public class FieldNotFoundException extends Throwable{
  Field missingField;
  public FieldNotFoundException(Field f) {
    missingField = f;
  }

  public String getMessage() {
    return "FieldNotFoundException: " + missingField.toString();
  }
}
