
package edu.umass.cs.gnscommon.exceptions.server;


public class ServerRuntimeException extends RuntimeException
{
  private static final long serialVersionUID = 6627620787610127842L;


  public ServerRuntimeException()
  {
    super();
  }


  public ServerRuntimeException(String message, Throwable cause)
  {
    super(message, cause);
  }


  public ServerRuntimeException(String message)
  {
    super(message);
  }


  public ServerRuntimeException(Throwable throwable)
  {
    super(throwable);
  }

}
