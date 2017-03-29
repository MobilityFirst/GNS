/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.utils;

/**
 *
 * @author westy
 */
public class Utils {

  /**
   * A version of fail that includes a stack trace.
   * 
   * @param message
   * @param e
   */
  public static final void failWithStackTrace(String message, Exception... e) {
    if (e != null && e.length > 0) {
      e[0].printStackTrace();
    }
    org.junit.Assert.fail(message);
  }

}
