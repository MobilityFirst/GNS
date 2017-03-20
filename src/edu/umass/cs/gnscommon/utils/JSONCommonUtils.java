package edu.umass.cs.gnscommon.utils;

import org.json.JSONObject;

import java.util.Iterator;

public class JSONCommonUtils {
    /**
     * Try to convert a string into a number, boolean, or null. If the string
     * can't be converted, return the string.
     * COPIED from org.json - to make Android's org.json and j2objc's org.json
     * compatible with it.
     * @param string A String.
     * @return A simple JSON value.
     */
    public static Object stringToValue(String string) {
      if (string.equals("")) {
        return string;
      }
      if (string.equalsIgnoreCase("true")) {
        return Boolean.TRUE;
      }
      if (string.equalsIgnoreCase("false")) {
        return Boolean.FALSE;
      }
      if (string.equalsIgnoreCase("null")) {
        return JSONObject.NULL;
      }

      /*
       * If it might be a number, try converting it.
       * We support the non-standard 0x- convention.
       * If a number cannot be produced, then the value will just
       * be a string. Note that the 0x-, plus, and implied string
       * conventions are non-standard. A JSON parser may accept
       * non-JSON forms as long as it accepts all correct JSON forms.
       */
      char b = string.charAt(0);
      if ((b >= '0' && b <= '9') || b == '.' || b == '-' || b == '+') {
        if (b == '0' && string.length() > 2
                && (string.charAt(1) == 'x' || string.charAt(1) == 'X')) {
          try {
            return new Integer(Integer.parseInt(string.substring(2), 16));
          } catch (Exception ignore) {
          }
        }
        try {
          if (string.indexOf('.') > -1
                  || string.indexOf('e') > -1 || string.indexOf('E') > -1) {
            return Double.valueOf(string);
          } else {
            Long myLong = new Long(string);
            if (myLong.longValue() == myLong.intValue()) {
              return new Integer(myLong.intValue());
            } else {
              return myLong;
            }
          }
        } catch (Exception ignore) {
        }
      }
      return string;
    }

    /**
     * Get an array of field names from a JSONObject.
     * COPIED from org.json - to make Android's org.json and j2objc's org.json
     * compatible with it.
     *
     * @return An array of field names, or null if there are no names.
     */
    public static String[] getNames(JSONObject jo) {
      int length = jo.length();
      if (length == 0) {
        return null;
      }
      Iterator iterator = jo.keys();
      String[] names = new String[length];
      int i = 0;
      while (iterator.hasNext()) {
        names[i] = (String) iterator.next();
        i += 1;
      }
      return names;
    }
}
