
package edu.umass.cs.gnscommon.utils;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;


public class URIEncoderDecoder {

  private static final String DIGITS = "0123456789ABCDEF";

  private static final String ENCODING = "UTF8";


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
