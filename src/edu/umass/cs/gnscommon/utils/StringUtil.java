
package edu.umass.cs.gnscommon.utils;

import java.util.regex.Pattern;


public class StringUtil {

  private static final Pattern HEX_PATTERN = Pattern.compile("^[0-9A-Fa-f]+$");


  public static boolean isValidGuidString(String guid) {
    return (guid != null) && (guid.length() == 40) && HEX_PATTERN.matcher(guid).matches();
  }
  

  public static String insertUnderScoresBeforeCapitals(String str) {
    StringBuilder result = new StringBuilder();
    // SKIP THE FIRST CAPITAL
    result.append(str.charAt(0));
    // START AT ONE SO WE SKIP THE FIRST CAPITAL
    for (int i = 1; i < str.length(); i++) {
      if (Character.isUpperCase(str.charAt(i))) {
        result.append("_");
      }
      result.append(str.charAt(i));
    }
    return result.toString();
  }
  
}
