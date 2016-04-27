/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnscommon.utils;

import java.util.regex.Pattern;

/**
 *
 * @author westy
 */
public class Util {

  private static final Pattern HEX_PATTERN = Pattern.compile("^[0-9A-Fa-f]+$");

  /**
   * Returns true if the given String is a potentially valid GUID (160 bits
   * expressed as a 40 digit hex number). Note that this does not check if the
   * GUID is valid in the GNS, just that this is a properly formed string.
   *
   * @param guid the string to evaluate
   * @return true if it is a properly formatted GUID string
   */
  public static boolean isValidGuidString(String guid) {
    return (guid != null) && (guid.length() == 40) && HEX_PATTERN.matcher(guid).matches();
  }
  
}
