/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.exceptions;

/**
 * This class defines a GnsException
 */
public class GnsRuntimeException extends RuntimeException
{
  private static final long serialVersionUID = 6627620787610127842L;

  /**
   * Creates a new <code>GnrsException</code> object
   */
  public GnsRuntimeException()
  {
    super();
  }

  /**
   * Creates a new <code>GnrsException</code> object
   * 
   * @param message
   * @param cause
   */
  public GnsRuntimeException(String message, Throwable cause)
  {
    super(message, cause);
  }

  /**
   * Creates a new <code>GnrsException</code> object
   * 
   * @param message
   */
  public GnsRuntimeException(String message)
  {
    super(message);
  }

  /**
   * Creates a new <code>GnrsException</code> object
   * 
   * @param throwable
   */
  public GnsRuntimeException(Throwable throwable)
  {
    super(throwable);
  }

}
