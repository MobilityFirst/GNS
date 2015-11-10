/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsclient.exceptions;

/**
 * This class defines a GnsFieldNotFoundException
 * 
 * @version 1.0
 */
public class GnsFieldNotFoundException extends GnsException
{
  private static final long serialVersionUID = 2676899572105162853L;

  /**
   * Creates a new <code>GnsInvalidFieldException</code> object
   */
  public GnsFieldNotFoundException()
  {
    // TODO Auto-generated constructor stub
  }

  /**
   * Creates a new <code>GnsInvalidFieldException</code> object
   * 
   * @param detailMessage
   */
  public GnsFieldNotFoundException(String detailMessage)
  {
    super(detailMessage);
    // TODO Auto-generated constructor stub
  }

  /**
   * Creates a new <code>GnsInvalidFieldException</code> object
   * 
   * @param throwable
   */
  public GnsFieldNotFoundException(Throwable throwable)
  {
    super(throwable);
    // TODO Auto-generated constructor stub
  }

  /**
   * Creates a new <code>GnsInvalidFieldException</code> object
   * 
   * @param detailMessage
   * @param throwable
   */
  public GnsFieldNotFoundException(String detailMessage, Throwable throwable)
  {
    super(detailMessage, throwable);
    // TODO Auto-generated constructor stub
  }

}
