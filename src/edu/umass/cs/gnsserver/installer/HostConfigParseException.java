
package edu.umass.cs.gnsserver.installer;

import edu.umass.cs.gnscommon.exceptions.server.ServerException;


public class HostConfigParseException extends ServerException {

  private static final long serialVersionUID = 1L;


  public HostConfigParseException() {
    super();
  }


  public HostConfigParseException(String message, Throwable cause) {
    super(message, cause);
  }


  public HostConfigParseException(String message) {
    super(message);
  }


  public HostConfigParseException(Throwable throwable) {
    super(throwable);
  }

}
