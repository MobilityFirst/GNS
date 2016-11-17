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
package edu.umass.cs.gnscommon.utils;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;

/**
 * Provides encoding and decoding of URI strings.
 *
 */
public class URIEncoderDecoder {

  private static final String DIGITS = "0123456789ABCDEF";

  private static final String ENCODING = "UTF8";

  /**
   * Validate a string by checking if it contains any characters other than:
   * <br>
   * 1. letters ('a'..'z', 'A'..'Z') 2. numbers ('0'..'9') 3. characters in
   * the legalset parameter 4. others (Unicode characters that are not in
   * US-ASCII set, and are not ISO Control or are not ISO Space characters)
   * called from URI.Helper.parseURI() to validate each component
   *
   * @param s java.lang.String the string to be validated
   * @param legal java.lang.String the characters allowed in the String s
   */
  static void validate(String s, String legal) throws URISyntaxException {
    for (int i = 0; i < s.length();) {
      char ch = s.charAt(i);
      if (ch == '%') {
        do {
          if (i + 2 >= s.length()) {
            throw new URISyntaxException(s, "Incomplete % sequence");
          }
          int d1 = Character.digit(s.charAt(i + 1), 16);
          int d2 = Character.digit(s.charAt(i + 2), 16);
          if (d1 == -1 || d2 == -1) {
            throw new URISyntaxException(s,
                    "Invalid % sequence " + s.substring(i, i + 3), i);
          }

          i += 3;
        } while (i < s.length() && s.charAt(i) == '%');

        continue;
      }
      if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
              || (ch >= '0' && ch <= '9') || legal.indexOf(ch) > -1 || (ch > 127
              && !Character.isSpaceChar(ch) && !Character
              .isISOControl(ch)))) {
        throw new URISyntaxException(s, "Illegal character", i);
      }
      i++;
    }
  }

  static void validateSimple(String s, String legal)
          throws URISyntaxException {
    for (int i = 0; i < s.length();) {
      char ch = s.charAt(i);
      if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
              || (ch >= '0' && ch <= '9') || legal.indexOf(ch) > -1)) {
        throw new URISyntaxException(s, "Illegal character", i); //$NON-NLS-1$
      }
      i++;
    }
  }

  /**
   * All characters except letters ('a'..'z', 'A'..'Z') and numbers ('0'..'9')
   * and legal characters are converted into their hexidecimal value prepended
   * by '%'.
   * <br>
   * For example: '#' -- %23
   * <br>
   * Other characters, which are Unicode chars that are not US-ASCII, and are
   * not ISO Control or are not ISO Space chars, are preserved.
   *
   * @param s java.lang.String the string to be converted
   * @return java.lang.String the converted string
   * @throws java.io.UnsupportedEncodingException
   */
  public static String quoteIllegal(String s) throws UnsupportedEncodingException {
    return quoteIllegal(s, "");
  }

  private static String quoteIllegal(String s, String legal) throws UnsupportedEncodingException {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if ((ch >= 'a' && ch <= 'z')
              || (ch >= 'A' && ch <= 'Z')
              || (ch >= '0' && ch <= '9')
              || legal.indexOf(ch) > -1) {
        buf.append(ch);
      } else {
        byte[] bytes = new String(new char[]{ch}).getBytes(ENCODING);
        for (int j = 0; j < bytes.length; j++) {
          buf.append('%');
          buf.append(DIGITS.charAt((bytes[j] & 0xf0) >> 4));
          buf.append(DIGITS.charAt(bytes[j] & 0xf));
        }
      }
    }
    return buf.toString();
  }

  /**
   * Other characters, which are Unicode chars that are not US-ASCII, and are
   * not ISO Control or are not ISO Space chars are not preserved. They are
   * converted into their hexidecimal value prepended by '%'.
   * <br>
   * For example: Euro currency symbol - "%E2%82%AC".
   * <br>
   * Called from URI.toASCIIString()
   * <br>
   *
   * @param s java.lang.String the string to be converted
   * @return java.lang.String the converted string
   */
  static String encodeOthers(String s) throws UnsupportedEncodingException {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (ch <= 127) {
        buf.append(ch);
      } else {
        byte[] bytes = new String(new char[]{ch}).getBytes(ENCODING);
        for (int j = 0; j < bytes.length; j++) {
          buf.append('%');
          buf.append(DIGITS.charAt((bytes[j] & 0xf0) >> 4));
          buf.append(DIGITS.charAt(bytes[j] & 0xf));
        }
      }
    }
    return buf.toString();
  }

  /**
   * Decodes the string argument which is assumed to be encoded in the
   * <code>x-www-form-urlencoded</code> MIME content type using the UTF-8
   * ENCODING scheme.
   * <br>
   * '%' and two following hex digit characters are converted to the
   * equivalent byte value. All other characters are passed through
   * unmodified.
   * <br>
   * <br>
   * e.g. "A%20B%20C %24%25" - "A B C $%"
   * <br>
   * Called from URI.getXYZ() methods
   * <br>
   *
   * @param s java.lang.String The encoded string.
   * @return java.lang.String The decoded version.
   * @throws java.io.UnsupportedEncodingException
   */
  public static String decode(String s) throws UnsupportedEncodingException {

    StringBuilder result = new StringBuilder();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < s.length();) {
      char c = s.charAt(i);
      if (c == '%') {
        out.reset();
        do {
          if (i + 2 >= s.length()) {
            throw new IllegalArgumentException("Incomplete % sequence at " + i);
          }
          int d1 = Character.digit(s.charAt(i + 1), 16);
          int d2 = Character.digit(s.charAt(i + 2), 16);
          if (d1 == -1 || d2 == -1) {
            throw new IllegalArgumentException(
                    "Invalid % sequence" + s.substring(i, i + 3) + "at "
                    + String.valueOf(i));
          }
          out.write((byte) ((d1 << 4) + d2));
          i += 3;
        } while (i < s.length() && s.charAt(i) == '%');
        result.append(out.toString(ENCODING));
        continue;
      }
      result.append(c);
      i++;
    }
    return result.toString();
  }

}
