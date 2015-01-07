/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.clientsupport;

/**
 * Support for fields that can't be accessed by the client.
 * 
 * @author westy
 */
public class InternalField {
  private static final String INTERNAL_PREFIX = "_GNS_";

  /**
   * Creates a GNS field that is hidden from the user.
   *
   * @param string
   * @return
   */
  public static String makeInternalFieldString(String string) {
    return INTERNAL_PREFIX + string;
  }

  /**
   * Returns true if field is a GNS field that is hidden from the user.
   *
   * @param key
   * @return
   */
  public static boolean isInternalField(String key) {
    return key.startsWith(INTERNAL_PREFIX);
  }

  /**
   * Returns the length of the prefix used with internal fields.
   * 
   * @return
   */
  public static int getPrefixLength() {
    return INTERNAL_PREFIX.length();
  }
  
  
  
}
