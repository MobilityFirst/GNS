package edu.umass.cs.gnscommon.exceptions;

import edu.umass.cs.gnscommon.ResponseCode;

/**
 * @author arun
 *
 */
public class GNSException extends Exception {

  final ResponseCode code;
  /**
   *
   */
  private static final long serialVersionUID = 6816831396928147083L;

  /**
   * The GNSException.
   */
  protected GNSException() {
    super();
    this.code = null;
  }

  /**
   * @param code
   * @param GUID
   * @param message
   */
  public GNSException(ResponseCode code, String message, String GUID) {
    super(message);
    this.code = code;
  }

  /**
   * @param code
   * @param message
   */
  public GNSException(ResponseCode code, String message) {
    this(code, message, (String) null);
  }

  /**
   *
   * @param message
   * @param cause
   */
  public GNSException(String message, Throwable cause) {
    super(message, cause);
    this.code = null;
  }

  /**
   *
   * @param message
   */
  public GNSException(String message) {
    this(null, message);
  }

  /**
   *
   * @param throwable
   */
  public GNSException(Throwable throwable) {
    super(throwable);
    this.code = null;
  }

  /**
   * @param code
   * @param message
   * @param cause
   */
  public GNSException(ResponseCode code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  /**
   * @return Code
   */
  public ResponseCode getCode() {
    return this.code;
  }
}
