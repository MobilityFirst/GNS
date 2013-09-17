/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.exceptions;

/**
 * This class defines a GnsException
 */
public class GnsException extends Exception
{
  private static final long serialVersionUID = 6627620787610127842L;

  /**
   * Creates a new <code>GnrsException</code> object
   */
  public GnsException()
  {
    super();
  }

  /**
   * Creates a new <code>GnrsException</code> object
   * 
   * @param message
   * @param cause
   */
  public GnsException(String message, Throwable cause)
  {
    super(message, cause);
  }

  /**
   * Creates a new <code>GnrsException</code> object
   * 
   * @param message
   */
  public GnsException(String message)
  {
    super(message);
  }

  /**
   * Creates a new <code>GnrsException</code> object
   * 
   * @param throwable
   */
  public GnsException(Throwable throwable)
  {
    super(throwable);
  }

}
