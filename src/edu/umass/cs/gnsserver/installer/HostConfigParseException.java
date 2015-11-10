/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsserver.installer;

import edu.umass.cs.gnsserver.exceptions.GnsException;

/**
 * This class defines a HostConfigParseException
 *
 * @author <a href="mailto:westy@cs.umass.edu">Westy</a>
 */
public class HostConfigParseException extends GnsException {

  /**
   * Creates a new <code>HostConfigParseException</code> object
   */
  public HostConfigParseException() {
    super();
  }

  /**
   * Creates a new <code>HostConfigParseException</code> object
   *
   * @param message
   * @param cause
   */
  public HostConfigParseException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new <code>HostConfigParseException</code> object
   *
   * @param message
   */
  public HostConfigParseException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>HostConfigParseException</code> object
   *
   * @param throwable
   */
  public HostConfigParseException(Throwable throwable) {
    super(throwable);
  }

}
