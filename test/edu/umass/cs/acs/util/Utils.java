/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.acs.util;

/**
 *
 * @author westy
 */
public class Utils {

  public static double roundTo(double value, int places) {
    if (places < 0 || places > 20) {
      throw new IllegalArgumentException();
    }

    double factor = Math.pow(10.0, places);
    value = value * factor;
    double tmp = Math.round(value);
    return tmp / factor;
  }

  public static String capitalizeSentences(String input) {
    int pos = 0;
    boolean capitalize = true;
    StringBuilder sb = new StringBuilder(input);
    while (pos < sb.length()) {
      if (sb.charAt(pos) == '.' || sb.charAt(pos) == '!' || sb.charAt(pos) == '?') {
        capitalize = true;
      } else if (capitalize && !Character.isWhitespace(sb.charAt(pos))) {
        sb.setCharAt(pos, Character.toUpperCase(sb.charAt(pos)));
        capitalize = false;
      }
      pos++;
    }
    return sb.toString();
  }

  public static void main(String... args) {

    String text = ". frank. now is the time. this line is over?  this one has started.";
    
    System.out.println(capitalizeSentences(text));

  }

}
